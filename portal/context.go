package portal

import "time"
import "sync"
import "math/rand"
import "context"
import "errors"
import "github.com/gin-gonic/gin"
import "github.com/marxn/vasc/logger"

type Portal struct {
    ProjectName     string
    TxID            uint64
    Context         context.Context
    containerCtx    interface{}
    LogLevel        int
    LoggerMap       map[string]*logger.VascLogger
    LoggerMapMutex  sync.Mutex
}

type TaskContent struct {
    ProjectName       string      `json:"project_name"`
    CreateTime        int64       `json:"create_time"`
    Content         []byte        `json:"content"`
}

func MakeGinRouteWithContext(projectName string, payload func(*Portal), parent context.Context) func(c *gin.Context) {
    // return a wrapper for handling http request
    return func(c *gin.Context) {
        ctx, cancelFunc := context.WithCancel(parent)
        defer cancelFunc()
        
        vContext := NewVascContext(projectName)
        vContext.Context      = ctx
        vContext.containerCtx = c
        
        // Save TxID in order to use customed logger
        c.Keys["VascTraceID"] = vContext.TxID
        
        // Do handling
        payload(vContext)
        vContext.Close()
    }
}

func MakeSchedulePortalWithContext(projectName string, scheduleKey string, payload func(*Portal) error, parent context.Context) func() error {
    // return a wrapper for handling schedule
    return func() error {
        ctx, cancelFunc := context.WithCancel(parent)
        defer cancelFunc()
        
        vContext := NewVascContext(projectName)
        vContext.Context      = ctx
        vContext.containerCtx = nil
        
        startTime := time.Now().UnixNano()
        // Call scheduled func
        err := payload(vContext)
        
        endTime := time.Now().UnixNano()
        if err != nil {
            vContext.Logger("_schedule").ErrorLog("%s: cost[%d ms], result[%v]", scheduleKey, (endTime - startTime) / 1e6, err)
        } else {
            vContext.Logger("_schedule").InfoLog("%s: cost[%d ms], result[%v]", scheduleKey, (endTime - startTime) / 1e6, err)
        }
        
        vContext.Close()
        return err
    }
}

func MakeTaskHandlerWithContext(projectName string, taskKey string, payload func(*Portal) error, content *TaskContent, parent context.Context) func() error {
    // return a wrapper for handling underlying task
    return func() error {
        ctx, cancelFunc := context.WithCancel(parent)
        defer cancelFunc()
        
        vContext := NewVascContext(projectName)
        vContext.Context      = ctx
        vContext.containerCtx = content
        
        startTime := time.Now().UnixNano()
        
        // Call scheduled func
        err := payload(vContext)
        
        endTime := time.Now().UnixNano()
        if err != nil {
            vContext.Logger("_task").ErrorLog("%s: cost[%d ms], result[%v]", taskKey, (endTime - startTime) / 1e6, err)
        } else {
            vContext.Logger("_task").InfoLog("%s: cost[%d ms], result[%v]", taskKey, (endTime - startTime) / 1e6, err)
        }
        
        vContext.Close()
        return err
    }
}

func NewVascContext(projectName string) *Portal {
    rand.Seed(time.Now().Unix())
    result := &Portal{
        ProjectName: projectName,
        LogLevel   : logger.LOG_DEBUG,
        TxID       : rand.Uint64(), 
        LoggerMap  : make(map[string]*logger.VascLogger)}
        
    return result
}

func (ctx *Portal) SetTID(txID uint64) {
    ctx.TxID = txID
}

func (ctx *Portal) HttpContext() *gin.Context {
    return ctx.containerCtx.(*gin.Context)
}

func (ctx *Portal) TaskContent() (*TaskContent, error) {
    taskContent := ctx.containerCtx.(*TaskContent)
    if taskContent == nil {
        return nil, errors.New("Invalid task")
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
