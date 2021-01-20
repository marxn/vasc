package portal

import "time"
import "sync"
import "math/rand"
import "context"
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

func MakeGinRouteWithContext(projectName string, payload func(*Portal), parent context.Context) func(c *gin.Context) {
    // return a wrapper for handling http request
    return func(c *gin.Context) {
        ctx, cancelFunc := context.WithCancel(parent)
        defer cancelFunc()
        
        vContext := NewVascContext(projectName)
        vContext.Context      = ctx
        vContext.containerCtx = c
        
        // Do handling
        payload(vContext)
        vContext.Close()
    }
}

func MakeSchedulePortalWithContext(projectName string, scheduleKey string, payload func(*Portal) error, parent context.Context) func() error {
    // return a wrapper for handling common request
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
        vContext.Logger("_schedule").ErrorLog("%s: cost[%d], result[%v]", scheduleKey, (endTime - startTime) / 1e6, err)
        
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

func (ctx *Portal) HttpContext() *gin.Context {
    return ctx.containerCtx.(*gin.Context)
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
