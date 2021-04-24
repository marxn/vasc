package portal

import (
    "context"
    "errors"
    "fmt"
    "github.com/gin-gonic/gin"
    "github.com/marxn/vasc/logger"
    "math/rand"
    "net/http"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Portal struct {
    ProjectName     string
    TxID            uint64
    Context         context.Context
    containerCtx    interface{}
    HandlerName     string
    LogLevel        int
    LogSelector     string
    LoggerMap       map[string]*logger.VascLogger
    LoggerMapMutex  sync.Mutex
}

type TaskContent struct {
    ProjectName       string      `json:"project_name"`
    CreateTime        int64       `json:"create_time"`
    Content         []byte        `json:"content"`
}

func MakeGinRouteWithContext(projectName string, handlerName string, payload func(*Portal), timeout int) func(c *gin.Context) {
    // return a wrapper for handling http request
    return func(c *gin.Context) {
        // Return directly if the upper group handler has broke.
        needBreak := c.Request.Header.Get("X-Vasc-Request-Needbreak")
        if strings.ToLower(needBreak) == "true" {
            return
        }

        var ctx context.Context
        var cancelFunc context.CancelFunc

        if timeout > 0 {
            ctx, cancelFunc = context.WithTimeout(context.Background(), time.Second * time.Duration(timeout))
        } else {
            ctx, cancelFunc = context.WithCancel(context.Background())
        }

        vContext := NewVascContext(projectName)
        vContext.HandlerName  = handlerName
        vContext.Context      = ctx
        vContext.LogSelector  = "request"
        vContext.containerCtx = c

        defer func () {
            if r := recover(); r != nil {
                c.AbortWithStatus(http.StatusInternalServerError)
                vContext.Logger("_gin").ErrorLog("Panic:[%v]", r)
            }
            cancelFunc()
            vContext.Close()
        }()

        tracer := c.Request.Header.Get("X-Vasc-Request-Tracer")
        if tracer != "" {
            txID, _ := strconv.ParseUint(tracer, 16, 64)
            vContext.SetTID(txID)
        } else {
            // Save TxID in order to use customized logger
            c.Request.Header.Set("X-Vasc-Request-Tracer", fmt.Sprintf("%016x", vContext.TxID))
        }

        // Do handling
        payload(vContext)
    }
}

func MakeSchedulePortalWithContext(projectName string, enableLogger bool, scheduleKey string, payload func(*Portal) error, parent context.Context) func() error {
    // return a wrapper for handling schedule
    return func() error {
        ctx, cancelFunc := context.WithCancel(parent)

        vContext := NewVascContext(projectName)
        vContext.HandlerName  = scheduleKey
        vContext.Context      = ctx
        vContext.LogSelector  = "schedule"
        vContext.containerCtx = nil

        defer func () {
            if r := recover(); r != nil {
                vContext.Logger("_schedule").ErrorLog("%s: Panic:[%v]", scheduleKey, r)
            }
            cancelFunc()
            vContext.Close()
        }()

        startTime := time.Now().UnixNano()

        // Call scheduled func
        err := payload(vContext)

        endTime := time.Now().UnixNano()
        if enableLogger {
            if err != nil {
                vContext.Logger("_schedule").ErrorLog("%s: cost[%d ms], result[%v]", scheduleKey, (endTime - startTime) / 1e6, err)
            } else {
                vContext.Logger("_schedule").InfoLog("%s: cost[%d ms], result[%v]", scheduleKey, (endTime - startTime) / 1e6, err)
            }
        }

        return err
    }
}

func MakeTaskHandlerWithContext(projectName string, enableLogger bool, taskKey string, payload func(*Portal) error, content *TaskContent, parent context.Context) func() error {
    // return a wrapper for handling underlying task
    return func() error {
        ctx, cancelFunc := context.WithCancel(parent)
        
        vContext := NewVascContext(projectName)
        vContext.HandlerName  = taskKey
        vContext.Context      = ctx
        vContext.LogSelector  = "task"
        vContext.containerCtx = content
        
        defer func () {
            if r := recover(); r != nil {
                vContext.Logger("_task").ErrorLog("%s: Panic:[%v]", taskKey, r)
            }
            cancelFunc()
            vContext.Close()
        }()
        
        startTime := time.Now().UnixNano()
        
        // Entrance of task
        err := payload(vContext)
        
        endTime := time.Now().UnixNano()
        if enableLogger {
            if err != nil {
                vContext.Logger("_task").ErrorLog("%s: cost[%d ms], result[%v]", taskKey, (endTime - startTime) / 1e6, err)
            } else {
                vContext.Logger("_task").InfoLog("%s: cost[%d ms], result[%v]", taskKey, (endTime - startTime) / 1e6, err)
            }
        }
        
        return err
    }
}

func NewVascContext(projectName string) *Portal {
    rand.Seed(time.Now().UnixNano())
    result := &Portal{
        ProjectName: projectName,
        LogLevel   : logger.LOG_DEBUG,
        TxID       : rand.Uint64(), 
        LogSelector: "default",
        LoggerMap  : make(map[string]*logger.VascLogger)}
        
    return result
}

func (ctx *Portal) SetTID(txID uint64) {
    ctx.TxID = txID
}

func (ctx *Portal) NeedBreak(need bool) {
    value := "false"
    if need {
        value = "true"
    }
    ctx.HttpContext().Request.Header.Set("X-Vasc-Request-Needbreak", value)
}

func (ctx *Portal) SetDefaultLogger(LogSelector string) {
    ctx.LogSelector = LogSelector
}

func (ctx *Portal) HttpContext() *gin.Context {
    return ctx.containerCtx.(*gin.Context)
}

func (ctx *Portal) TaskContent() (*TaskContent, error) {
    taskContent := ctx.containerCtx.(*TaskContent)
    if taskContent == nil {
        return nil, errors.New("invalid task")
    }
    return taskContent, nil
}

func (ctx *Portal) Close() {
    ctx.LoggerMapMutex.Lock()
	defer ctx.LoggerMapMutex.Unlock()
	
	for _, value := range ctx.LoggerMap {
	    value.Close()
	}
}

func (ctx *Portal) Logger(subsystem string) *logger.VascLogger {
    ctx.LoggerMapMutex.Lock()
	defer ctx.LoggerMapMutex.Unlock()
	
	result := ctx.LoggerMap[subsystem]
	if result == nil {
	    result = logger.NewVascLogger(ctx.ProjectName, ctx.LogLevel, subsystem)
        ctx.LoggerMap[subsystem] = result
    }
    
    result.TxID = ctx.TxID
    return result
}

func (ctx *Portal) DefaultLogger() *logger.VascLogger {
    subsystem := ctx.LogSelector
    
    ctx.LoggerMapMutex.Lock()
	defer ctx.LoggerMapMutex.Unlock()
	
	result := ctx.LoggerMap[subsystem]
	if result == nil {
	    result = logger.NewVascLogger(ctx.ProjectName, ctx.LogLevel, subsystem)
        ctx.LoggerMap[subsystem] = result
    }
    
    result.TxID = ctx.TxID
    return result
}

func (ctx *Portal) DefaultInfoLog(format string, v ...interface{}) {
    ctx.DefaultLogger().InfoLog(ctx.HandlerName + ": " + format, v)
}

func (ctx *Portal) DefaultErrorLog(format string, v ...interface{}) {
    ctx.DefaultLogger().ErrorLog(ctx.HandlerName + ": " + format, v)
}

func (ctx *Portal) DefaultDebugLog(format string, v ...interface{}) {
    ctx.DefaultLogger().DebugLog(ctx.HandlerName + ": " + format, v)
}

func (ctx *Portal) DefaultWarnLog(format string, v ...interface{}) {
    ctx.DefaultLogger().WarnLog(ctx.HandlerName + ": " + format, v)
}

func (ctx *Portal) Exit() {
    panic("VASC_HANDLER_EXIT")
}

func (ctx *Portal) ErrorLogAndReturnJSON(code int, format string, v ...interface{}) {
    ctx.DefaultErrorLog(format, v)
    ctx.HttpContext().JSON(code, gin.H{"code": code, "message": fmt.Sprintf(format, v)})
}

func (ctx *Portal) InfoLogAndReturnJSON(code int, format string, v ...interface{}) {
    ctx.DefaultInfoLog(format, v)
    ctx.HttpContext().JSON(code, gin.H{"code": code, "message": fmt.Sprintf(format, v)})
}

func (ctx *Portal) WarnLogAndReturnJSON(code int, format string, v ...interface{}) {
    ctx.DefaultWarnLog(format, v)
    ctx.HttpContext().JSON(code, gin.H{"code": code, "message": fmt.Sprintf(format, v)})
}

func (ctx *Portal) DebugLogAndReturnJSON(code int, format string, v ...interface{}) {
    ctx.DefaultDebugLog(format, v)
    ctx.HttpContext().JSON(code, gin.H{"code": code, "message": fmt.Sprintf(format, v)})
}