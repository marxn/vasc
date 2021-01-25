package logger

import "fmt"
import "log/syslog"
import "runtime"

const LOG_DEBUG = 0
const LOG_INFO  = 1
const LOG_WARN  = 2
const LOG_ERROR = 3

type VascLogger struct {
    LogLevel    int
    TxID        uint64
    Logger     *syslog.Writer
}

func (this *VascLogger) SetTxID(txID uint64) {
    this.TxID = txID
}

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
        this.VLogger("root", FuncCaller(3), this.TxID, LOG_ERROR, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) InfoLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_INFO {
        this.VLogger("root", FuncCaller(3), this.TxID, LOG_INFO, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) WarnLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_WARN {
        this.VLogger("root", FuncCaller(3), this.TxID, LOG_WARN, fmt.Sprintf(format, v...))
    }
}

func (this *VascLogger) DebugLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_DEBUG {
        this.VLogger("root", FuncCaller(3), this.TxID, LOG_DEBUG, fmt.Sprintf(format, v...))
    }
}

func NewVascLogger(projectName string, logLevel int, subsystem string) *VascLogger {
    tag := projectName + "/" + subsystem
    loggerInstance, _ := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, tag)
    return &VascLogger{LogLevel: logLevel, Logger: loggerInstance}
}

func FuncCaller(frame int) string {
	pc := make([]uintptr, 1)
	runtime.Callers(frame, pc)
	f := runtime.FuncForPC(pc[0])
	return f.Name()
}
