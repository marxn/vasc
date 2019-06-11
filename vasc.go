package vasc

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"syscall"
	"os/signal"
	"io/ioutil"
    "encoding/json"
    "github.com/marxn/vasc/global"
    "github.com/marxn/vasc/database"
    vredis "github.com/marxn/vasc/redis"
    "github.com/marxn/vasc/webserver"
    "github.com/marxn/vasc/scheduler"
    "github.com/marxn/vasc/task"
    "github.com/marxn/vasc/localcache"
    "github.com/marxn/vasc/logger"
)

const VASC_NONE      = 0x0
const VASC_WEBSERVER = 0x01 << 1
const VASC_CACHE     = 0x01 << 2
const VASC_DB        = 0x01 << 3
const VASC_REDIS     = 0x01 << 4
const VASC_SCHEDULER = 0x01 << 5
const VASC_TASK      = 0x01 << 6

type VascService struct {
    Cache         *localcache.CacheManager
    WebServer     *webserver.VascWebServer
    Log           *logger.VascLog
    DB            *database.VascDataBase
    Redis         *vredis.VascRedis
    Scheduler     *scheduler.VascScheduler
    Task          *task.VascTask
    BitCode        uint64
}

var vascInstance    *VascService
var vascSignalChan   chan os.Signal
var configFilePath  *string
var environment     *string

func GetEnvironment() string {
    return *environment
}

func GetConfigFilePath() string {
    return *configFilePath
}

func loadModule(projectName string, logLevel string, app *global.VascApplication) error {
    var vascConfiguration global.VascConfig
    err := json.Unmarshal([]byte(app.Configuration), &vascConfiguration)
    if err != nil {
        return errors.New("Cannot parse vasc config file for project:" + projectName)
    }
    
    var appConfiguration global.ControllerConfig
    err = json.Unmarshal([]byte(app.AppConfiguration), &appConfiguration)
    if err != nil {
        return errors.New("Cannot parse application config file for project:" + projectName)
    }
    
    //Initialization of logger. This must be done before all vasc modules
    vascInstance.Log = new(logger.VascLog)
    err = vascInstance.Log.LoadConfig(projectName)
    if err!=nil {
        return err
    }
    
    switch logLevel {
    	case "debug":
    		vascInstance.Log.SetLogLevel(logger.LOG_DEBUG)
    	case "info":
    		vascInstance.Log.SetLogLevel(logger.LOG_INFO)
    	case "warning":
    		vascInstance.Log.SetLogLevel(logger.LOG_WARN)
    	case "error":
    		vascInstance.Log.SetLogLevel(logger.LOG_ERROR)
        default:
            return errors.New("invalid log level")
    }
    
    if vascConfiguration.Redis!=nil && vascConfiguration.Redis.Enable {
        vascInstance.Redis = new(vredis.VascRedis)
        err := vascInstance.Redis.LoadConfig(vascConfiguration.Redis, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_REDIS
    }    
    
    if vascConfiguration.Database!=nil && vascConfiguration.Database.Enable && len(vascConfiguration.Database.InstanceList) > 0 {
        vascInstance.DB = new(database.VascDataBase)
        err := vascInstance.DB.LoadConfig(vascConfiguration.Database, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_DB
    }
    
    if vascConfiguration.LocalCache!=nil && vascConfiguration.LocalCache.Enable{
        vascInstance.Cache = new(localcache.CacheManager)
        err := vascInstance.Cache.LoadConfig(vascConfiguration.LocalCache, vascInstance.Redis, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_CACHE
    }
    
    if vascConfiguration.Webserver!=nil && vascConfiguration.Webserver.Enable {
        vascInstance.WebServer = new(webserver.VascWebServer)
        err := vascInstance.WebServer.LoadConfig(vascConfiguration.Webserver, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.WebServer.LoadModules(appConfiguration.WebserverRoute, appConfiguration.WebServerGroup, app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= VASC_WEBSERVER
    }
    
    if vascConfiguration.Scheduler!=nil && vascConfiguration.Scheduler.Enable {
        vascInstance.Scheduler = new(scheduler.VascScheduler)
        err := vascInstance.Scheduler.LoadConfig(vascConfiguration.Scheduler, vascInstance.Redis, vascInstance.DB, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.Scheduler.LoadSchedule(appConfiguration.ScheduleList, app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= VASC_SCHEDULER
    }

    if vascConfiguration.Task!=nil && vascConfiguration.Task.Enable {
        vascInstance.Task = new(task.VascTask)
        err := vascInstance.Task.LoadConfig(vascConfiguration.Task, vascInstance.Redis, vascInstance.DB, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.Task.LoadTask(appConfiguration.TaskList, app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= VASC_TASK
    }    
    
    return nil
}

func initModule(projectName string, logLevel string, app *global.VascApplication) error {
    return nil
}

func InitInstance(app *global.VascApplication) error {
    project        := flag.String("n", "",      "project name")
    configFilePath  = flag.String("c", "",      "project config file path")
    environment     = flag.String("e", "",      "environment(demo/test/online/...)")
	pidfile        := flag.String("p", "",      "pid file path")
	mode           := flag.String("m", "normal","running mode(normal/bootstrap)")
	logLevel       := flag.String("l", "debug", "log level(debug, info, warning, error)")
    
	flag.Parse()
    
    if *project=="" {
        return errors.New("project name cannot be empty")
    }
    
    if app.Configuration=="" {
        return errors.New("config file path cannot be empty")
    }
   
    if *pidfile!="" {
        GeneratePidFile(pidfile)
	}
    
    //Install signal receiver
    vascSignalChan = make(chan os.Signal)
    signal.Notify(vascSignalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
    
    //Initliaze object
    vascInstance = new(VascService)
    vascInstance.BitCode = 0
    
    if *mode=="bootstrap" {
        return initModule(*project, *logLevel, app)
    }
    
    return loadModule(*project, *logLevel, app)
}

func StartService() error {
    err := vascInstance.WebServer.Start()
    if err==nil {
        return vascInstance.WebServer.CheckService()
    }
    return err
}

func Close() {
    if(vascInstance.BitCode & VASC_TASK != 0) {
        vascInstance.Task.Close()
    }
    if(vascInstance.BitCode & VASC_SCHEDULER != 0) {
        vascInstance.Scheduler.Close()
    }
    if(vascInstance.BitCode & VASC_WEBSERVER != 0) {
        vascInstance.WebServer.Close()
    }
    if(vascInstance.BitCode & VASC_CACHE != 0) {
        vascInstance.Cache.Close()
    }
    if(vascInstance.BitCode & VASC_DB != 0) {
        vascInstance.DB.Close()
    }
    if(vascInstance.BitCode & VASC_REDIS != 0) {
        vascInstance.Redis.Close()
    }
    
    vascInstance.Log.Close()
}

func GetVascInstance() *VascService {
    return vascInstance
}

func GeneratePidFile(pidfile *string) {
	pid := fmt.Sprintf("%d", os.Getpid())
	err := ioutil.WriteFile(*pidfile, []byte(pid), 0666)
	if err != nil {
		fmt.Println("Cannot write pid file:" + err.Error())
	}
}

func Wait() {
    for {
        s := <- vascSignalChan
        switch s {
            case syscall.SIGHUP:
            case syscall.SIGUSR2:
                VascReloader()
            default:
                return
        }
    }
}

func VascReloader() {
    if vascInstance==nil {
        return
    }
    vascInstance.Scheduler.ReloadSchedule()
    vascInstance.Task.ReloadTaskList()
}

//Some simple encapsulations
func ErrorLog(format string, v ...interface{}) {
    GetVascInstance().Log.ErrorLog(format, v...)
}

func InfoLog(format string, v ...interface{}) {
    GetVascInstance().Log.InfoLog(format, v...)
}

func WarnLog(format string, v ...interface{}) {
    GetVascInstance().Log.WarnLog(format, v...)
}

func DebugLog(format string, v ...interface{}) {
    GetVascInstance().Log.DebugLog(format, v...)
}
