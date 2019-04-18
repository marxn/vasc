package vasc

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"syscall"
	"os/signal"
    "encoding/json"
	"io/ioutil"
)

const VASC_NONE      = 0x0
const VASC_WEBSERVER = 0x01 << 1
const VASC_CACHE     = 0x01 << 2
const VASC_DB        = 0x01 << 3
const VASC_REDIS     = 0x01 << 4
const VASC_SCHEDULER = 0x01 << 5
const VASC_TASK      = 0x01 << 6

type VascService struct {
    Cache         *CacheManager
    WebServer     *VascWebServer
    Log           *VascLog
    DB            *VascDataBase
    Redis         *VascRedis
    Scheduler     *VascScheduler
    Task          *VascTask
    SignalHandler  func ()
    BitCode        uint64
}

type VascConfig struct {
    Database    *databaseConfig     `json:"database"`
    Redis       *redisConfig        `json:"redis"`
    Webserver   *webServerConfig    `json:"webserver"`
    LocalCache  *cacheConfigFile    `json:"localcache"`
    Scheduler   *scheduleConfig     `json:"scheduler"`
    Task        *taskConfig         `json:"task"`
}

type VascApplication struct {
    WebserverRoute []VascRoute
    TaskList       []TaskInfo
    ScheduleList   []ScheduleInfo
    FuncMap          map[string]VascRoutine
    SignalHandler    func ()
}

type VascRoutine func (interface{}) error

var vascInstance    *VascService
var vascSignalChan   chan os.Signal
var logLevel        *string

func loadModule(projectName string, logLevel string, configFilePath string, app *VascApplication) error {
    config, err := ioutil.ReadFile(configFilePath)
    if err != nil{
        return errors.New("Cannot find config file for project:" + projectName)
    }
    
    var jsonResult VascConfig
    err = json.Unmarshal([]byte(config), &jsonResult)
    if err != nil {
        return errors.New("Cannot parse config file for project:" + projectName)
    }
    
    //Initialization of logger. This must be done before all vasc modules
    vascInstance.Log = new(VascLog)
    err = vascInstance.Log.LoadConfig(projectName)
    if err!=nil {
        return err
    }
    
    vascInstance.SignalHandler = app.SignalHandler
    
    switch logLevel {
    	case "debug":
    		vascInstance.Log.SetLogLevel(LOG_DEBUG)
    	case "info":
    		vascInstance.Log.SetLogLevel(LOG_INFO)
    	case "warning":
    		vascInstance.Log.SetLogLevel(LOG_WARN)
    	case "error":
    		vascInstance.Log.SetLogLevel(LOG_ERROR)
        default:
            return errors.New("invalid log level")
    }
    
    if jsonResult.Redis!=nil && jsonResult.Redis.Enable {
        vascInstance.Redis = new(VascRedis)
        err := vascInstance.Redis.LoadConfig(jsonResult.Redis, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_REDIS
    }    
    
    if jsonResult.Database!=nil && jsonResult.Database.Enable && len(jsonResult.Database.InstanceList) > 0 {
        vascInstance.DB = new(VascDataBase)
        err := vascInstance.DB.LoadConfig(jsonResult.Database, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_DB
    }
    
    if jsonResult.LocalCache!=nil && jsonResult.LocalCache.Enable{
        vascInstance.Cache = new(CacheManager)
        err := vascInstance.Cache.LoadConfig(jsonResult.LocalCache, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_CACHE
    }
    
    if jsonResult.Webserver!=nil && jsonResult.Webserver.Enable {
        vascInstance.WebServer = new(VascWebServer)
        err := vascInstance.WebServer.LoadConfig(jsonResult.Webserver, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.WebServer.LoadModules(app.WebserverRoute)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= VASC_WEBSERVER
    }
    
    if jsonResult.Scheduler!=nil && jsonResult.Scheduler.Enable {
        vascInstance.Scheduler = new(VascScheduler)
        err := vascInstance.Scheduler.LoadConfig(jsonResult.Scheduler, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.Scheduler.LoadSchedule(app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= VASC_SCHEDULER
    }

    if jsonResult.Task!=nil && jsonResult.Task.Enable {
        vascInstance.Task = new(VascTask)
        err := vascInstance.Task.LoadConfig(jsonResult.Task, projectName)
        if err!=nil {
            return err
        }
        
        err = vascInstance.Task.LoadTask(app)
        if err!=nil {
            return err
        }
        
        vascInstance.BitCode |= VASC_TASK
    }    
    
    return nil
}

func initModule(projectName string, logLevel string, configFilePath string, app *VascApplication) error {
    return nil
}

func InitInstance(app *VascApplication) error {
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
                vascInstance.SignalHandler()
            default:
                return
        }
    }
}
