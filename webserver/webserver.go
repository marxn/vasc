package webserver

import "time"
import "net/http"
import "errors"
import "github.com/gin-gonic/gin"
import "github.com/marxn/vasc/global" 

type VascWebServer struct {
    ProjectName     string
    ServiceCore    *gin.Engine
    ListenRetry     int
    ListenAddr      string
    ReadTimeout     time.Duration
    WriteTimeout    time.Duration
    HttpServer     *http.Server
    Runnable        bool
}

func (this *VascWebServer) LoadConfig(config *global.WebServerConfig, projectName string) error {
    this.ProjectName = projectName
    
    gin.SetMode(gin.ReleaseMode)
    engine := gin.New()  
    engine.Use(gin.Recovery())  
    
    this.ServiceCore     = engine
    this.ProjectName     = projectName
    this.ListenAddr      = config.ListenAddr
    this.ListenRetry     = config.ListenRetry
    this.ReadTimeout     = time.Duration(config.ReadTimeout)
    this.WriteTimeout    = time.Duration(config.WriteTimeout)
    
    return this.InitWebserver()
}

func (this *VascWebServer) Close() {
    this.Runnable = false
    this.HttpServer.Close()
}

func (this *VascWebServer) InitWebserver() error {
	this.HttpServer = &http.Server{
		Addr:         this.ListenAddr,
		Handler:      this.ServiceCore,
		ReadTimeout:  this.ReadTimeout  * time.Second,
		WriteTimeout: this.WriteTimeout * time.Second,
	}
	this.Runnable = true
	
    return nil
}

func (this *VascWebServer) Start() error {
    timer := time.NewTimer(time.Second * time.Duration(this.ListenRetry + 1))
    succ  := make(chan bool)
    defer close(succ)
    
    go func() {
    	for counter := 0; counter < this.ListenRetry && this.Runnable; counter++ {
    	    this.HttpServer.ListenAndServe()
    		time.Sleep(time.Second)
    	}
    	succ <- false
    }()
    
    select {
    		case <-timer.C:
    		    return nil
    		case <-succ:
    		    return errors.New("ListenAndServe failed")
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
	    handler := app.FuncMap[modules[i].HandlerName]
        if handler!=nil {
            modules[i].RouteHandler = handler.(func(*gin.Context))
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
	            groupCore.Use(groupMiddleware.(func(*gin.Context)))
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
    c.JSON(500, gin.H{"error": gin.H{"code": 500, "message": "Invalid handler"}})
}