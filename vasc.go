package vasc

import (
	"flag"
	"fmt"
	"os"
	"io/ioutil"
)

const VASC_NONE      = 0x0
const VASC_LOG       = 0x01 << 0
const VASC_WEBSERVER = 0x01 << 1
const VASC_CACHE     = 0x01 << 2
const VASC_DB        = 0x01 << 3
const VASC_REDIS     = 0x01 << 4

type VascService struct {
    Cache      *KVManager
    WebServer  *VascWebServer
    Log        *VascLog
    DB         *VascDataBase
    Redis      *VascRedis
}

type VascDataBase struct {
}

func (this *VascDataBase) LoadConfig(projectName string, profile string) error {
    return nil
}

type VascRedis struct {
}

func (this *VascRedis) LoadConfig(projectName string, profile string) error {
    return nil
}

var vascInstance *VascService
var vascProfile  string

func InitInstance(projectName string, bitCode uint64) error {
    listenAddr := flag.String("listen",   "localhost:8080", "listen address")
	profile    := flag.String("profile",  "dev",            "profile for running environment(dev, test, online, ...)")
	pidfile    := flag.String("pidfile",  "",               "pid filename")
	logLevel   := flag.String("loglevel", "debug",          "log level(debug, info, warning, error)")
    
	flag.Parse()
    
    vascProfile = *profile
    
    if *pidfile != "" {
	    GeneratePidFile(pidfile)
	}
	
    vascInstance = new(VascService)
    
    if(bitCode | VASC_LOG != 0) {
        vascInstance.Log = new(VascLog)
        vascInstance.Log.LoadConfig(projectName, vascProfile)
        
        switch *logLevel {
        	case "debug":
        		vascInstance.Log.SetLogLevel(LOG_DEBUG)
        	case "info":
        		vascInstance.Log.SetLogLevel(LOG_INFO)
        	case "warning":
        		vascInstance.Log.SetLogLevel(LOG_WARN)
        	case "error":
        		vascInstance.Log.SetLogLevel(LOG_ERROR)
        	}
    }
    
    if(bitCode & VASC_WEBSERVER != 0) {
        vascInstance.WebServer = new(VascWebServer)
        vascInstance.WebServer.LoadConfig(projectName, vascProfile)
        vascInstance.WebServer.SetAddr(*listenAddr)
    }
    
    if(bitCode & VASC_CACHE != 0) {
        vascInstance.Cache = new(KVManager)
        vascInstance.Cache.LoadConfig(projectName, vascProfile)
    }
    
    if(bitCode & VASC_DB != 0) {
        vascInstance.DB = new(VascDataBase)
        vascInstance.DB.LoadConfig(projectName, vascProfile)
    }
    
    if(bitCode & VASC_CACHE != 0) {
        vascInstance.Redis = new(VascRedis)
        vascInstance.Redis.LoadConfig(projectName, vascProfile)
    }
    
    return nil
}

func CloseVasc() {
    
}

func GetVascInstance() *VascService {
    return vascInstance
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