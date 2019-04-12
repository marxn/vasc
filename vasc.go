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
    Cache      *CacheManager
    WebServer  *VascWebServer
    Log        *VascLog
    DB         *VascDataBase
    Redis      *VascRedis
    Scheduler  *VascScheduler
    Task       *VascTask
    Reloader    func ()
    BitCode    uint64
}

type VascConfig struct {
    Database   []databaseConfig     `json:"database"`
    Redis       *redisConfig        `json:"kvstore"`
    Webserver   *webServerConfig    `json:"webserver"`
    LocalCache  *cacheConfigFile    `json:"localcache"`
    Scheduler   *scheduleConfig     `json:"scheduler"`
    Task        *taskConfig         `json:"task"`
}

type VascApplication struct {
    WebserverRoute []VascRoute
    TaskList       []TaskInfo
    ScheduleList   []ScheduleInfo
    Reloader         func () error
}

var vascInstance    *VascService
var vascSignalChan   chan os.Signal
var logLevel        *string

func loadModule(projectName string, logLevel string, configFilePath string, core *VascService) error {
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
    
    }
    
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
    
    if jsonResult.LocalCache!=nil {
        vascInstance.Cache = new(CacheManager)
        err := vascInstance.Cache.LoadConfig(jsonResult.LocalCache, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_CACHE
    }
    
    if jsonResult.Database!=nil && len(jsonResult.Database) > 0 {
        vascInstance.DB = new(VascDataBase)
        err := vascInstance.DB.LoadConfig(jsonResult.Database, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_DB
    }
    
    if jsonResult.Redis!=nil {
        vascInstance.Redis = new(VascRedis)
        err := vascInstance.Redis.LoadConfig(jsonResult.Redis, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_REDIS
    }    
    
    if jsonResult.Webserver!=nil {
        vascInstance.WebServer = new(VascWebServer)
        err := vascInstance.WebServer.LoadConfig(jsonResult.Webserver, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_WEBSERVER
    }
    
    if jsonResult.Scheduler!=nil {
        vascInstance.Scheduler = new(VascScheduler)
        err := vascInstance.Scheduler.LoadConfig(jsonResult.Scheduler, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_SCHEDULER
    }

    if jsonResult.Task!=nil {
        vascInstance.Task = new(VascTask)
        err := vascInstance.Task.LoadConfig(jsonResult.Task, projectName)
        if err!=nil {
            return err
        }
        vascInstance.BitCode |= VASC_TASK
    }    
    
    return nil
}

func InitInstance(app *VascApplication) error {
    project    := flag.String("project",    "",      "project name")
    configfile := flag.String("configfile", "./",    "config file path")
	pidfile    := flag.String("pidfile",    "",      "pid filename")
	logLevel   := flag.String("loglevel",   "debug", "log level(debug, info, warning, error)")
    
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
    signal.Notify(vascSignalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
    
    //Initliaze object
    vascInstance = new(VascService)
    vascInstance.BitCode = 0
    
    err := loadModule(*project, *logLevel, *configfile, vascInstance)
    if err!=nil {
        return err
    }
    
    err = vascInstance.Scheduler.LoadSchedule(app.ScheduleList)
    if err!=nil {
        return err
    }
    
    err = vascInstance.Task.LoadTask(app.TaskList)
    if err!=nil {
        return err
    }
    
    err = vascInstance.WebServer.LoadModules(app.WebserverRoute)
    if err==nil {
        vascInstance.WebServer.Start()
        return nil
    }
    
    return nil
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
    if(vascInstance.BitCode & VASC_REDIS != 0) {
        vascInstance.Redis.Close()
    }
    if(vascInstance.BitCode & VASC_DB != 0) {
        vascInstance.DB.Close()
    }
    if(vascInstance.BitCode & VASC_CACHE != 0) {
        vascInstance.Cache.Close()
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

func WaitSignal() {
    for {
        s := <- vascSignalChan
        switch s {
            case syscall.SIGUSR2:
                vascInstance.Reloader()
            default:
                return
        }
    }
}
