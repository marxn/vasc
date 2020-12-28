package logger

import (
    "fmt"
    "errors"
    "log/syslog"
)

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

func (this *VascLog) vascLogWrapper(level int, s string) {
	switch level {
	case LOG_DEBUG:
		this.Logger.Debug(fmt.Sprintf("[debug] %s", s))
	case LOG_INFO:
		this.Logger.Info(fmt.Sprintf("[info] %s", s))
	case LOG_WARN:
		this.Logger.Warning(fmt.Sprintf("[warning] %s", s))
	case LOG_ERROR:
		this.Logger.Err(fmt.Sprintf("[error] %s", s))
	default:
		this.Logger.Err(fmt.Sprintf("[error] %s", s))
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
		this.vascLogWrapper(LOG_ERROR, fmt.Sprintf(format, v...))
	}
}

func (this *VascLog) InfoLog(format string, v ...interface{}) {
	if this.LogLevel <= LOG_INFO {
		this.vascLogWrapper(LOG_INFO, fmt.Sprintf(format, v...))
	}
}

func (this *VascLog) WarnLog(format string, v ...interface{}) {
	if this.LogLevel <= LOG_WARN {
		this.vascLogWrapper(LOG_WARN, fmt.Sprintf(format, v...))
	}
}

func (this *VascLog) DebugLog(format string, v ...interface{}) {
	if this.LogLevel <= LOG_DEBUG {
		this.vascLogWrapper(LOG_DEBUG, fmt.Sprintf(format, v...))
	}
}

func (this *VascLog) SetLogLevel(level int) {
    this.LogLevel = level
}
