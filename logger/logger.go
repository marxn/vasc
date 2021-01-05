package logger

import "fmt"
import "errors"
import "log/syslog"
import "bytes"
import "runtime"
import "strconv"

const (
    LOG_DEBUG = 0
    LOG_INFO  = 1
    LOG_WARN  = 2
    LOG_ERROR = 3
)

type VascLog struct {
    ProjectName string
    LogLevel    int
    Logger     *syslog.Writer
}

func getGID() uint64 {
    b := make([]byte, 64)
    b = b[:runtime.Stack(b, false)]
    b = bytes.TrimPrefix(b, []byte("goroutine"))
    b = b[:bytes.IndexByte(b, ' ')]
    n, _ := strconv.ParseUint(string(b), 10, 64)
    return n
}

func (this *VascLog) VLogger(tag string, evt string, tid uint64, level int, s string) {
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

func (this *VascLog) LoadConfig(projectName string) error {
    this.ProjectName = projectName
    this.LogLevel    = LOG_DEBUG
    
    logger, err := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, this.ProjectName)
    if err != nil {
        return errors.New("cannot open syslog")
    }
    
    this.Logger = logger
    
    return nil
}

func (this *VascLog) Close() {
    this.Logger.Close()
}

func (this *VascLog) ErrorLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_ERROR {
        this.VLogger("root", "", getGID(), LOG_ERROR, fmt.Sprintf(format, v...))
    }
}

func (this *VascLog) InfoLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_INFO {
        this.VLogger("root", "", getGID(), LOG_INFO, fmt.Sprintf(format, v...))
    }
}

func (this *VascLog) WarnLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_WARN {
        this.VLogger("root", "", getGID(), LOG_WARN, fmt.Sprintf(format, v...))
    }
}

func (this *VascLog) DebugLog(format string, v ...interface{}) {
    if this.LogLevel <= LOG_DEBUG {
        this.VLogger("root", "", getGID(), LOG_DEBUG, fmt.Sprintf(format, v...))
    }
}

func (this *VascLog) SetLogLevel(level int) {
    this.LogLevel = level
}
