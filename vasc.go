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
var logLevel        *string

func loadModule(projectName string, logLevel string, configFilePath string, app *global.VascApplication) error {
    config, err := ioutil.ReadFile(configFilePath)
    if err != nil{
        return errors.New("Cannot find config file for project:" + projectName)
    }
    
    var jsonResult global.VascConfig
    err = json.Unmarshal([]byte(config), &jsonResult)
    if err != nil {
        return errors.New("Cannot parse config file for project:" + projectName)
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
    
    if jsonResult.Redis!=nil && jsonResult.Redis.Enable {
        vascInstance.Redis = new(vredis.VascRedis)
        err := vascInstance.Redis.LoadConfig(jsonResult.Redis, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= global.VASC_REDIS
    }    
    
    if jsonResult.Database!=nil && jsonResult.Database.Enable && len(jsonResult.Database.InstanceList) > 0 {
        vascInstance.DB = new(database.VascDataBase)
        err := vascInstance.DB.LoadConfig(jsonResult.Database, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= global.VASC_DB
    }
    
    if jsonResult.LocalCache!=nil && jsonResult.LocalCache.Enable{
        vascInstance.Cache = new(localcache.CacheManager)
        err := vascInstance.Cache.LoadConfig(jsonResult.LocalCache, vascInstance.Redis, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= global.VASC_CACHE
    }
    
    if jsonResult.Webserver!=nil && jsonResult.Webserver.Enable {
        vascInstance.WebServer = new(webserver.VascWebServer)
        err := vascInstance.WebServer.LoadConfig(jsonResult.Webserver, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.WebServer.LoadModules(app.WebserverRoute)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= global.VASC_WEBSERVER
    }
    
    if jsonResult.Scheduler!=nil && jsonResult.Scheduler.Enable {
        vascInstance.Scheduler = new(scheduler.VascScheduler)
        err := vascInstance.Scheduler.LoadConfig(jsonResult.Scheduler, vascInstance.Redis, vascInstance.DB, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.Scheduler.LoadSchedule(app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= global.VASC_SCHEDULER
    }

    if jsonResult.Task!=nil && jsonResult.Task.Enable {
        vascInstance.Task = new(task.VascTask)
        err := vascInstance.Task.LoadConfig(jsonResult.Task, vascInstance.Redis, vascInstance.DB, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.Task.LoadTask(app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= global.VASC_TASK
    }    
    
    return nil
}

func initModule(projectName string, logLevel string, configFilePath string, app *global.VascApplication) error {
    return nil
}

func InitInstance(app *global.VascApplication) error {
    project    := flag.String("n", "",      "project name")
    configfile := flag.String("c", "",      "vasc config file path")
	pidfile    := flag.String("p", "",      "pid file path")
	mode       := flag.String("m", "normal","running mode(normal/bootstrap)")
	logLevel   := flag.String("l", "debug", "log level(debug, info, warning, error)")
    
	flag.Parse()
    
    if *project=="" {
        return errors.New("project name cannot be empty")
    }
    
    if *configfile=="" {
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
        return initModule(*project, *logLevel, *configfile, app)
    }
    
    return loadModule(*project, *logLevel, *configfile, app)
}

func StartService() error {
    return vascInstance.WebServer.Start()
}

func Close() {
    if(vascInstance.BitCode & global.VASC_TASK != 0) {
        vascInstance.Task.Close()
    }
    if(vascInstance.BitCode & global.VASC_SCHEDULER != 0) {
        vascInstance.Scheduler.Close()
    }
    if(vascInstance.BitCode & global.VASC_WEBSERVER != 0) {
        vascInstance.WebServer.Close()
    }
    if(vascInstance.BitCode & global.VASC_CACHE != 0) {
        vascInstance.Cache.Close()
    }
    if(vascInstance.BitCode & global.VASC_DB != 0) {
        vascInstance.DB.Close()
    }
    if(vascInstance.BitCode & global.VASC_REDIS != 0) {
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
