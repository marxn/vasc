package vasc

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/marxn/vasc/database"
	"github.com/marxn/vasc/global"
	"github.com/marxn/vasc/localcache"
	"github.com/marxn/vasc/logger"
	vredis "github.com/marxn/vasc/redis"
	"github.com/marxn/vasc/scheduler"
	"github.com/marxn/vasc/task"
	"github.com/marxn/vasc/webserver"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const VascWebserver = 0x01 << 1
const VascCache = 0x01 << 2
const VascDb = 0x01 << 3
const VascRedis = 0x01 << 4
const VascScheduler = 0x01 << 5
const VascTask = 0x01 << 6

type VascService struct {
	Cache     *localcache.CacheManager
	WebServer *webserver.VascWebServer
	DB        *database.VascDataBase
	Redis     *vredis.VascRedis
	Scheduler *scheduler.VascScheduler
	Task      *task.VascTask
	BitCode   uint64
}

var vascInstance *VascService
var vascSignalChan chan os.Signal
var project *string
var environment *string
var mode *string
var initializer func() error
var vascLogLevel string

// Global logger referenced by default logging
var loggerMapper map[string]*logger.VascLogger
var loggerMapMutex sync.Mutex

func GetProjectName() string {
	return *project
}

func GetEnvironment() string {
	return *environment
}

func GetMode() string {
	return *mode
}

func loadModule(projectName string, app *global.VascApplication) error {
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

	if vascConfiguration.Redis != nil && vascConfiguration.Redis.Enable {
		vascInstance.Redis = new(vredis.VascRedis)
		err := vascInstance.Redis.LoadConfig(vascConfiguration.Redis)
		if err != nil {
			return err
		}
		vascInstance.BitCode |= VascRedis
	}

	if vascConfiguration.Database != nil && vascConfiguration.Database.Enable && len(vascConfiguration.Database.InstanceList) > 0 {
		vascInstance.DB = new(database.VascDataBase)
		err := vascInstance.DB.LoadConfig(vascConfiguration.Database, projectName)
		if err != nil {
			return err
		}
		vascInstance.BitCode |= VascDb
	}

	if vascConfiguration.LocalCache != nil && vascConfiguration.LocalCache.Enable {
		vascInstance.Cache = new(localcache.CacheManager)
		err := vascInstance.Cache.LoadConfig(vascConfiguration.LocalCache, vascInstance.Redis, projectName)
		if err != nil {
			return err
		}
		vascInstance.BitCode |= VascCache
	}

	if vascConfiguration.Webserver != nil && vascConfiguration.Webserver.Enable {
		vascInstance.WebServer = new(webserver.VascWebServer)
		err := vascInstance.WebServer.LoadConfig(vascConfiguration.Webserver, projectName)
		if err != nil {
			return err
		}

		err = vascInstance.WebServer.LoadModules(appConfiguration.WebserverRoute, appConfiguration.WebServerGroup, app)
		if err != nil {
			return err
		}

		vascInstance.BitCode |= VascWebserver
	}

	if vascConfiguration.Scheduler != nil && vascConfiguration.Scheduler.Enable {
		vascInstance.Scheduler = new(scheduler.VascScheduler)
		err := vascInstance.Scheduler.LoadConfig(vascConfiguration.Scheduler, vascInstance.Redis, vascInstance.DB, projectName)
		if err != nil {
			return err
		}

		err = vascInstance.Scheduler.LoadSchedule(appConfiguration.ScheduleList, app)
		if err != nil {
			return err
		}

		vascInstance.BitCode |= VascScheduler
	}

	if vascConfiguration.Task != nil && vascConfiguration.Task.Enable {
		vascInstance.Task = new(task.VascTask)
		err := vascInstance.Task.LoadConfig(vascConfiguration.Task, vascInstance.Redis, vascInstance.DB, projectName)
		if err != nil {
			return err
		}

		err = vascInstance.Task.LoadTask(appConfiguration.TaskList, app)
		if err != nil {
			return err
		}

		vascInstance.BitCode |= VascTask
	}

	return nil
}

func InitInstance(app *global.VascApplication) error {
	project = flag.String("n", "", "project name")
	environment = flag.String("e", "", "environment(demo/test/online/...)")
	pidfile := flag.String("p", "", "pid file path")
	mode = flag.String("m", "normal", "running mode(normal/bootstrap)")
	logLevel := flag.String("l", "debug", "log level(debug, info, warning, error)")

	flag.Parse()

	if *project == "" {
		return errors.New("project name cannot be empty")
	}

	if app.Configuration == "" {
		return errors.New("config file path cannot be empty")
	}

	if *pidfile != "" {
		GeneratePidFile(pidfile)
	}

	//Install signal receiver
	vascSignalChan = make(chan os.Signal)
	signal.Notify(vascSignalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)

	//Initialize object
	vascInstance = new(VascService)
	vascInstance.BitCode = 0

	vascLogLevel = *logLevel

	return loadModule(*project, app)
}

func SetInitializer(initfunc func() error) {
	initializer = initfunc
}

func StartService() error {
	if initializer != nil {
		if err := initializer(); err != nil {
			return err
		}
	}

	if vascInstance.BitCode&VascScheduler != 0 {
		if err := vascInstance.Scheduler.Start(); err != nil {
			return err
		}
	}

	if vascInstance.BitCode&VascTask != 0 {
		if err := vascInstance.Task.Start(); err != nil {
			return err
		}
	}

	if vascInstance.BitCode&VascWebserver != 0 {
		if err := vascInstance.WebServer.Start(); err != nil {
			return err
		}
	}

	return nil
}

func Close() {
	if vascInstance.BitCode&VascTask != 0 {
		vascInstance.Task.Close()
	}
	if vascInstance.BitCode&VascScheduler != 0 {
		vascInstance.Scheduler.Close()
	}
	if vascInstance.BitCode&VascWebserver != 0 {
		vascInstance.WebServer.Close()
	}
	if vascInstance.BitCode&VascCache != 0 {
		vascInstance.Cache.Close()
	}
	if vascInstance.BitCode&VascDb != 0 {
		vascInstance.DB.Close()
	}
	if vascInstance.BitCode&VascRedis != 0 {
		vascInstance.Redis.Close()
	}
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
		s := <-vascSignalChan
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
	if vascInstance == nil {
		return
	}
	_ = vascInstance.Scheduler.ReloadSchedule()
	_ = vascInstance.Task.ReloadTaskList()
}

func LogSelector(subsystem string) *logger.VascLogger {
	var logLevel int
	switch vascLogLevel {
	case "debug":
		logLevel = logger.LOG_DEBUG
	case "info":
		logLevel = logger.LOG_INFO
	case "warning":
		logLevel = logger.LOG_WARN
	case "error":
		logLevel = logger.LOG_ERROR
	default:
		logLevel = logger.LOG_DEBUG
	}

	loggerMapMutex.Lock()
	defer loggerMapMutex.Unlock()

	if loggerMapper == nil {
		loggerMapper = make(map[string]*logger.VascLogger)
	}

	result := loggerMapper[subsystem]
	if result == nil {
		result = logger.NewVascLogger(GetProjectName(), logLevel, subsystem)
		loggerMapper[subsystem] = result
	}

	return result
}

//Some simple encapsulations
func ErrorLog(format string, v ...interface{}) {
	LogSelector("main").ErrorLog(format, v...)
}

func InfoLog(format string, v ...interface{}) {
	LogSelector("main").InfoLog(format, v...)
}

func WarnLog(format string, v ...interface{}) {
	LogSelector("main").WarnLog(format, v...)
}

func DebugLog(format string, v ...interface{}) {
	LogSelector("main").DebugLog(format, v...)
}
