package vasc

import (
    "fmt"
    "errors"
    "log/syslog"
    "github.com/gin-gonic/gin"
)

type VascRoute struct {
    Method        string
    Route         string
    RouteHandler  gin.HandlerFunc
    Middleware    gin.HandlerFunc
    LocalFilePath string
}

const (
	LOG_DEBUG = 0
	LOG_INFO  = 1
	LOG_WARN  = 2
	LOG_ERROR = 3
)

var projectName string = "VASCSERVER"
var logLevel    int    = LOG_DEBUG 

func vascLogWrapper(level int, s string) error {
	logger, err := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, projectName)
	if err != nil {
		return errors.New("Could not open syslog for writing")
	}

	switch level {
	case LOG_DEBUG:
		logger.Debug(s)
	case LOG_INFO:
		logger.Info(s)
	case LOG_WARN:
		logger.Warning(s)
	case LOG_ERROR:
		logger.Err(s)
	default:
		logger.Err(s)
	}

	logger.Close()
	return nil
}

func ErrorLog(format string, v ...interface{}) {
	if LOG_ERROR >= logLevel {
		vascLogWrapper(LOG_ERROR, fmt.Sprintf(format, v...))
	}
}

func InfoLog(format string, v ...interface{}) {
	if LOG_WARN >= logLevel {
		vascLogWrapper(LOG_ERROR, fmt.Sprintf(format, v...))
	}
}

func WarnLog(format string, v ...interface{}) {
	if LOG_INFO >= logLevel {
		vascLogWrapper(LOG_ERROR, fmt.Sprintf(format, v...))
	}
}

func DebugLog(format string, v ...interface{}) {
	if LOG_DEBUG >= logLevel {
		vascLogWrapper(LOG_ERROR, fmt.Sprintf(format, v...))
	}
}

func SetLogLevel(level int) {
	logLevel = level
}

func SetProjectName(name string) {
    projectName = name
}

func DefaultMiddleware(c *gin.Context) {
}
