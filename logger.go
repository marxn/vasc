package vasc

import "fmt"
import "log/syslog"
import "sync"

const LOG_DEBUG = 0
const LOG_INFO  = 1
const LOG_WARN  = 2
const LOG_ERROR = 3

type VascLogger struct {
    LogLevel    int
    Logger     *syslog.Writer
}

func getTxID() uint64 {
    return 0
}

var loggerMapper   map[string]*VascLogger
var loggerMapMutex sync.Mutex

func (this *VascLogger) VLogger(tag string, evt string, tid uint64, level int, s string) {
    if this.Logger == nil {
        return
    }
    
    switch level {
        case LOG_DEBUG:
            this.Logger.Debug(fmt.Sprintf("tag[%s] evt[%s] [debug] tid[%x] %s", tag, evt, tid, s))
        case LOG_INFO:
            this.Logger.Info(fmt.Sprintf("tag[%s] evt[%s] [info] tid[%x] %s", tag, evt, tid, s))
        case LOG_WARN:
            this.Logger.Warning(fmt.Sprintf("tag[%s] evt[%s] [warning] tid[%x] %s", tag, evt, tid, s))
        case LOG_ERROR:
            this.Logger.Err(fmt.Sprintf("tag[%s] evt[%s] [error] tid[%x] %s", tag, evt, tid, s))
        default:
            this.Logger.Err(fmt.Sprintf("tag[%s] evt[%s] [error] tid[%x] %s", tag, evt, tid, s))
    }
}

func (this *VascLogger) Close() {
    this.Logger.Close()
}

func (this *VascLogger) ErrorLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_ERROR {
        this.VLogger("root", "", getTxID(), LOG_ERROR, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) InfoLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_INFO {
        this.VLogger("root", "", getTxID(), LOG_INFO, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) WarnLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_WARN {
        this.VLogger("root", "", getTxID(), LOG_WARN, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) DebugLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_DEBUG {
        this.VLogger("root", "", getTxID(), LOG_DEBUG, fmt.Sprintf(format, v...))
    }
}

func LogSelector(subsystem string) *VascLogger {
    var logLevel int
    switch vascLogLevel {
        case "debug":
            logLevel = LOG_DEBUG
        case "info":
            logLevel = LOG_INFO
        case "warning":
            logLevel = LOG_WARN
        case "error":
            logLevel = LOG_ERROR
        default:
            logLevel = LOG_DEBUG
    }
    
    loggerMapMutex.Lock()
	defer loggerMapMutex.Unlock()
	
	if loggerMapper == nil {
	    loggerMapper = make(map[string]*VascLogger)
    }
	
	result := loggerMapper[subsystem]
	if result == nil {
        tag := GetProjectName() + "/" + subsystem
        loggerInstance, _ := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, tag)
        result = &VascLogger{LogLevel: logLevel, Logger: loggerInstance}
        loggerMapper[subsystem] = result
        return result
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
