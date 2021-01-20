package webserver

import "os"
import "net"
import "fmt"
import "time"
import "net/http"
import "context"
import "errors"
import "log/syslog"
import "github.com/gin-gonic/gin"
import "github.com/marxn/vasc/global"
import "github.com/marxn/vasc/portal"

type VascWebServer struct {
    ProjectName     string
    ServiceCore    *gin.Engine
    ListenRetry     int
    ListenAddr      string
    ReadTimeout     time.Duration
    WriteTimeout    time.Duration
    HttpServer     *http.Server
    Done            chan struct{}
}

func (this *VascWebServer) LoadConfig(config *global.WebServerConfig, projectName string) error {
    this.ProjectName = projectName
    
    gin.SetMode(gin.ReleaseMode)
    
    var engine *gin.Engine
    if config.EnableLogger {
        logWriter, err := syslog.New(syslog.LOG_INFO|syslog.LOG_LOCAL6, projectName + "/_gin")
        if err != nil {
            return err
        }
        gin.DefaultWriter = logWriter
        gin.DisableConsoleColor()
        
        engine = gin.New()  
        engine.Use(gin.Recovery())
        engine.Use(gin.Logger())
    } else {
        engine = gin.New()  
        engine.Use(gin.Recovery())  
    }
    
    this.ServiceCore     = engine
    this.ProjectName     = projectName
    this.ListenRetry     = config.ListenRetry
    this.ListenAddr      = config.ListenAddr
    this.ReadTimeout     = time.Duration(config.ReadTimeout)
    this.WriteTimeout    = time.Duration(config.WriteTimeout)
    this.Done            = make(chan struct{})
    
    return this.InitWebserver()
}

func (this *VascWebServer) Close() {
    ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
	defer cancel()
	
    this.HttpServer.Shutdown(ctx)
    close(this.Done)
}

func (this *VascWebServer) InitWebserver() error {
    this.HttpServer = &http.Server{
        Addr:         this.ListenAddr,
        Handler:      this.ServiceCore,
        ReadTimeout:  this.ReadTimeout  * time.Second,
        WriteTimeout: this.WriteTimeout * time.Second,
    }
    
    return nil
}

func (this *VascWebServer) Start() error {
    address := []byte(this.ListenAddr)
    if len(address) <= 4 {
        return errors.New("Invalid protocol")
    } else if string(address[0:5]) == "unix:" {
        
        // Wait a moment in case of listen error - why?
        time.Sleep(time.Second * 3)
        
        location := string(address[5:])
        os.Remove(location)
        
        unixAddr, err := net.ResolveUnixAddr("unix", location)
        if err != nil{
            return err
        }
        
        listener, err := net.ListenUnix("unix", unixAddr)
        if err != nil{
            return err
        }
        
        go func() {
            if err := this.HttpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
                fmt.Printf("listen unix sock file [%s] failed: %v\n", location, err)
            } 
            <-this.Done
        }()
    } else if string(address[0:4]) == "tcp:" {
        this.HttpServer.Addr = string(address[4:])
        go func() {
            for counter:=0; counter < this.ListenRetry; counter++ {
                if err := this.HttpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                    fmt.Printf("listen[%d] %s failed: %v\n", counter, this.ListenAddr, err)
                    time.Sleep(time.Second)
                } else {
                    <-this.Done
                    break
                }
                
            }
        }()
    } else {
        return errors.New("Invalid listen address")
    }
    
    return nil
}

func findGroupInfo(groups []global.VascRouteGroup, name string) *global.VascRouteGroup {
    for _, value := range groups {
        if value.Group==name {
            return &value
        }
    }
    return nil
}

func (this *VascWebServer) LoadModules(modules []global.VascRoute, groups []global.VascRouteGroup, app *global.VascApplication) error {
    if modules==nil {
        return nil
    }
    
    groupMap := make(map[string][]global.VascRoute)
    other  := make([]global.VascRoute, 0)
    
    for i := 0; i < len(modules); i++ {
        handlerName := modules[i].HandlerName
        handlerFunc := app.FuncMap[handlerName]
        if handlerFunc!=nil {
            switch handlerFunc.(type) {
                case func(*portal.Portal):
                    modules[i].RouteHandler = portal.MakeGinRouteWithContext(this.ProjectName, handlerFunc.(func(*portal.Portal)), context.Background())
                case func(*gin.Context):
                    modules[i].RouteHandler = handlerFunc.(func(*gin.Context))
                default:
                    modules[i].RouteHandler = InvalidHandler
            }
        } else {
            modules[i].RouteHandler = ErrorHandler
        }
        
        if modules[i].Group!="" {
            head := groupMap[modules[i].Group]
            if head==nil {
                head = make([]global.VascRoute, 0)
            }
            head = append(head, modules[i])
            groupMap[modules[i].Group] = head
        } else {
            other = append(other, modules[i])
        }
    }
    
    for groupName, group := range groupMap {
        groupCore := this.ServiceCore.Group(groupName)
        groupInfo := findGroupInfo(groups, groupName)
        if groupInfo!=nil {
            groupMiddleware := app.FuncMap[groupInfo.MiddlewareName]
            if groupMiddleware!=nil {
                switch groupMiddleware.(type) {
                    case func(*portal.Portal):
                        groupCore.Use(portal.MakeGinRouteWithContext(this.ProjectName, groupMiddleware.(func(*portal.Portal)), context.Background()))
                    case func(*gin.Context):
                        groupCore.Use(groupMiddleware.(func(*gin.Context)))
                    default:
                        groupCore.Use(InvalidHandler)
                }
            } else {
                groupCore.Use(DefaultMiddleware)
            }
        }
        
        for _, route := range group {
            switch route.Method {
                case "GET":
                    groupCore.GET(route.Route, route.RouteHandler)
                case "POST":
                    groupCore.POST(route.Route, route.RouteHandler)
                case "OPTIONS":
                    groupCore.OPTIONS(route.Route, route.RouteHandler)
                case "PUT":
                    groupCore.PUT(route.Route, route.RouteHandler)
                case "DELETE":
                    groupCore.DELETE(route.Route, route.RouteHandler)
                case "PATCH":
                    groupCore.PATCH(route.Route, route.RouteHandler)
                case "HEAD":
                    groupCore.HEAD(route.Route, route.RouteHandler)
                case "ANY":
                    groupCore.Any(route.Route, route.RouteHandler)
                case "FILE":
                    groupCore.StaticFS(route.Route, http.Dir(route.LocalFilePath))
                default:
                    continue
            }
        }
    }
    
    for _, routeItem := range other {
        switch routeItem.Method {
            case "GET":
                this.ServiceCore.GET(routeItem.Route, routeItem.RouteHandler)
            case "POST":
                this.ServiceCore.POST(routeItem.Route, routeItem.RouteHandler)
            case "OPTIONS":
                this.ServiceCore.OPTIONS(routeItem.Route, routeItem.RouteHandler)
            case "PUT":
                this.ServiceCore.PUT(routeItem.Route, routeItem.RouteHandler)
            case "DELETE":
                this.ServiceCore.DELETE(routeItem.Route, routeItem.RouteHandler)
            case "PATCH":
                this.ServiceCore.PATCH(routeItem.Route, routeItem.RouteHandler)
            case "HEAD":
                this.ServiceCore.HEAD(routeItem.Route, routeItem.RouteHandler)
            case "ANY":
                this.ServiceCore.Any(routeItem.Route, routeItem.RouteHandler)
            case "FILE":
                this.ServiceCore.StaticFS(routeItem.Route, http.Dir(routeItem.LocalFilePath))
            default:
                continue
        }
    }

    return nil
}

func DefaultMiddleware(c *gin.Context) {
    //Nothing to do
}

func ErrorHandler(c *gin.Context) {
    c.JSON(403, gin.H{"error": gin.H{"code": 501, "message": "Empty handler"}})
}

func InvalidHandler(c *gin.Context) {
    c.JSON(403, gin.H{"error": gin.H{"code": 501, "message": "Invalid handler prototype"}})
}
