package vasc

import "time"
import "net/http"
import "errors"
import "github.com/gin-gonic/gin"

type webServerConfig struct {
    ListenAddr        string         `json:"listen_address"`
    ListenRetry       int            `json:"listen_retry"`
    ReadTimeout       int            `json:"read_timeout"`
    WriteTimeout      int            `json:"write_timeout"`
}

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

type VascRoute struct {
    Method         string             `json:"method"`
    Route          string             `json:"route"`
    HandlerName    string             `json:"handler_name"`
    RouteHandler   gin.HandlerFunc    `json:"-"`
    MiddlewareName string             `json:"middleware_name"`
    Middleware     gin.HandlerFunc    `json:"-"`
    LocalFilePath  string             `json:"local_file_path"`
}

func (this *VascWebServer) LoadConfig(config *webServerConfig, projectName string) error {
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

func (this *VascWebServer) LoadModules(modules []VascRoute) error {
    if modules==nil {
        return nil
    }
	for i := 0; i < len(modules); i++ {
		switch modules[i].Method {
		case "GET":
			this.ServiceCore.GET(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "POST":
			this.ServiceCore.POST(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "OPTIONS":
			this.ServiceCore.OPTIONS(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "PUT":
			this.ServiceCore.PUT(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "DELETE":
			this.ServiceCore.DELETE(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "PATCH":
			this.ServiceCore.PATCH(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "HEAD":
			this.ServiceCore.HEAD(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "FILE":
			this.ServiceCore.StaticFS(modules[i].Route, http.Dir(modules[i].LocalFilePath))
		default:
			continue
		}
	}
	return nil
}

func DefaultMiddleware(c *gin.Context) {
}