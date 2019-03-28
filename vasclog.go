package vasc

import (
    "fmt"
    "errors"
    "log/syslog"
)

type VascLog struct {
    ProjectName string
    LogLevel    int
    Logger     *syslog.Writer
}

const (
	LOG_DEBUG = 0
	LOG_INFO  = 1
	LOG_WARN  = 2
	LOG_ERROR = 3
)

func (this *VascLog) vascLogWrapper(level int, s string) {
	switch level {
	case LOG_DEBUG:
		this.Logger.Debug(s)
	case LOG_INFO:
		this.Logger.Info(s)
	case LOG_WARN:
		this.Logger.Warning(s)
	case LOG_ERROR:
		this.Logger.Err(s)
	default:
		this.Logger.Err(s)
	}
}

func (this *VascLog) LoadConfig(configfile string, projectName string, profile string) error {
    this.ProjectName = projectName
    this.LogLevel    = LOG_DEBUG
    
    logger, err := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, this.ProjectName)
	if err != nil {
		return errors.New("Could not open syslog")
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
	if this.LogLevel <= LOG_WARN {
		this.vascLogWrapper(LOG_INFO, fmt.Sprintf(format, v...))
	}
}

func (this *VascLog) WarnLog(format string, v ...interface{}) {
	if this.LogLevel <= LOG_INFO {
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