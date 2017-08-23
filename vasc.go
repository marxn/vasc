package vasc

import "github.com/gin-gonic/gin"

type VascHandler func(c *gin.Context)

type VascRoute struct {
    ProjectName  string
    AccessMethod string
    AccessRoute  string
    FunctionName VascHandler
}

const (
	LOG_DEBUG = 0
	LOG_INFO  = 1
	LOG_WARN  = 2
	LOG_ERROR = 3
)

func vascLogWrapper(level int, s string) error {
	logger, err := syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL6, "mara-track-server/_all")
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

var logLevel int = LOG_DEBUG

func VascLog(level int, format string, v ...interface{}) {
	if level >= logLevel {
		vascLogWrapper(level, fmt.Sprintf(format, v...))
	}
}

func SetLogLevel(level int) {
	logLevel = level
}
