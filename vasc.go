package vasc

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"syscall"
	"os/signal"
	"io/ioutil"
)

const VASC_NONE      = 0x0
const VASC_WEBSERVER = 0x01 << 1
const VASC_CACHE     = 0x01 << 2
const VASC_DB        = 0x01 << 3
const VASC_REDIS     = 0x01 << 4
const VASC_SCHEDULER = 0x01 << 5

type VascService struct {
    Cache      *CacheManager
    WebServer  *VascWebServer
    Log        *VascLog
    DB         *VascDataBase
    Redis      *VascRedis
    Scheduler  *VascScheduler
    Reloader    func ()
    BitCode    uint64
}

var vascInstance *VascService
var vascProfile  string
var vascSignalChan chan os.Signal

func InitInstance(projectName string, bitCode uint64) error {
    configfile := flag.String("config",   "./",    "config file path")
	profile    := flag.String("profile",  "dev",   "profile for running environment(dev, test, online, ...)")
	pidfile    := flag.String("pidfile",  "",      "pid filename")
	logLevel   := flag.String("loglevel", "debug", "log level(debug, info, warning, error)")
    
	flag.Parse()
    
    if *configfile=="" {
        return errors.New("config file path cannot be empty")
    }
    
    if *profile=="" {
        return errors.New("profile cannot be empty")
    }
    
    if *pidfile!="" {
        GeneratePidFile(pidfile)
	}
    
    vascProfile = *profile
    
    //Install signal receiver
    vascSignalChan = make(chan os.Signal)
    signal.Notify(vascSignalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
    
    //Initliaze object
    vascInstance = new(VascService)
    vascInstance.BitCode = bitCode
    
    if(true) {
        vascInstance.Log = new(VascLog)
        err := vascInstance.Log.LoadConfig(*configfile, projectName, vascProfile)
        if err!=nil {
        
        }
        
        switch *logLevel {
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
    }
    
    if(bitCode & VASC_WEBSERVER != 0) {
        vascInstance.WebServer = new(VascWebServer)
        err := vascInstance.WebServer.LoadConfig(*configfile, projectName, vascProfile)
        if err!=nil {
            return err
        }
    }
    
    if(bitCode & VASC_CACHE != 0) {
        vascInstance.Cache = new(CacheManager)
        err := vascInstance.Cache.LoadConfig(*configfile, projectName, vascProfile)
        if err!=nil {
            return err
        }
    }
    
    if(bitCode & VASC_DB != 0) {
        vascInstance.DB = new(VascDataBase)
        err := vascInstance.DB.LoadConfig(*configfile, projectName, vascProfile)
        if err!=nil {
            return err
        }
    }
    
    if(bitCode & VASC_REDIS != 0) {
        vascInstance.Redis = new(VascRedis)
        err := vascInstance.Redis.LoadConfig(*configfile, projectName, vascProfile)
        if err!=nil {
            return err
        }
    }    
    
    if(bitCode & VASC_SCHEDULER != 0) {
        vascInstance.Scheduler = new(VascScheduler)
        err := vascInstance.Scheduler.LoadConfig(*configfile, projectName, vascProfile)
        if err!=nil {
            return err
        }
    }
    
    return nil
}

func Close() {
    if(true) {
        vascInstance.Log.Close()
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
    if(vascInstance.BitCode & VASC_SCHEDULER != 0) {
        vascInstance.Scheduler.Close()
    }
}

func GetVascInstance() *VascService {
    return vascInstance
}

func RegisterReloader(reloader func ()) {
    vascInstance.Reloader = reloader
}

func GetProfile() string {
    return vascProfile
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