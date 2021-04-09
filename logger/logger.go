package logger

import "fmt"
import "log/syslog"
import "sync"

const LOG_DEBUG = 0
const LOG_INFO  = 1
const LOG_WARN  = 2
const LOG_ERROR = 3

// Global logger referenced by default logging
var loggerMapper    map[string]*VascLogger
var loggerMapMutex  sync.Mutex
var projectName     string

func SetProjectName(name string) {
    projectName = name
}

type VascLogger struct {
    LogLevel    int
    TxID        uint64
    Logger     *syslog.Writer
}

func (this *VascLogger) SetTxID(txID uint64) {
    this.TxID = txID
}

func (this *VascLogger) VLogger(tid uint64, level int, s string) {
    if this.Logger == nil {
        return
    }
    
    switch level {
        case LOG_DEBUG:
            this.Logger.Debug(fmt.Sprintf("[debug] tid[%016x] %s", tid, s))
        case LOG_INFO:
            this.Logger.Info(fmt.Sprintf("[info] tid[%016x] %s", tid, s))
        case LOG_WARN:
            this.Logger.Warning(fmt.Sprintf("[warning] tid[%016x] %s", tid, s))
        case LOG_ERROR:
            this.Logger.Err(fmt.Sprintf("[error] tid[%016x] %s", tid, s))
        default:
            this.Logger.Err(fmt.Sprintf("[error] tid[%016x] %s", tid, s))
    }
}

func (this *VascLogger) Close() {
    this.Logger.Close()
}

func (this *VascLogger) ErrorLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_ERROR {
        this.VLogger(this.TxID, LOG_ERROR, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) InfoLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_INFO {
        this.VLogger(this.TxID, LOG_INFO, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) WarnLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_WARN {
        this.VLogger(this.TxID, LOG_WARN, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) DebugLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_DEBUG {
        this.VLogger(this.TxID, LOG_DEBUG, fmt.Sprintf(format, v...))
    }
}

func NewVascLogger(projectName string, logLevel int, subsystem string) *VascLogger {
    tag := projectName + "/" + subsystem
    loggerInstance, _ := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, tag)
    return &VascLogger{LogLevel: logLevel, Logger: loggerInstance}
}

func EmptyLogger() *VascLogger {
    return &VascLogger{LogLevel: LOG_DEBUG, Logger: nil}
}

func LogSelector(subsystem string) *VascLogger {
    loggerMapMutex.Lock()
	defer loggerMapMutex.Unlock()
	
	if loggerMapper == nil {
	    loggerMapper = make(map[string]*VascLogger)
    }
	
	result := loggerMapper[subsystem]
	if result == nil {
	    result = NewVascLogger(projectName, LOG_DEBUG, subsystem)
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
