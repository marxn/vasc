package portal

import "time"
import "sync"
import "math/rand"
import "github.com/gin-gonic/gin"
import "github.com/marxn/vasc/logger"

type VascContext struct {
    ProjectName     string
    TxID            uint64
    HttpContext    *gin.Context
    LogLevel        int
    LoggerMap       map[string]*logger.VascLogger
    LoggerMapMutex  sync.Mutex
}

func NewVascContext(projectName string) *VascContext {
    rand.Seed(time.Now().Unix())
    result := &VascContext{
        ProjectName: projectName,
        LogLevel   : logger.LOG_DEBUG,
        TxID       : rand.Uint64(), 
        HttpContext: nil,
        LoggerMap  : make(map[string]*logger.VascLogger)}
        
    return result
}

func (ctx *VascContext) Close() {
    ctx.LoggerMapMutex.Lock()
	defer ctx.LoggerMapMutex.Unlock()
	
	for _, value := range ctx.LoggerMap {
	    value.Close()
	}
}

func (ctx *VascContext) Logger(subsystem string) *logger.VascLogger {
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
