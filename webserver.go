package vasc

import "time"
import "net/http"
import "errors"
import "io/ioutil"
import "encoding/json"
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
}

type VascRoute struct {
    Method        string
    Route         string
    RouteHandler  gin.HandlerFunc
    Middleware    gin.HandlerFunc
    LocalFilePath string
}

func (this *VascWebServer) LoadConfig(configFile string, projectName string, profile string) error {
    this.ProjectName = projectName
    config, err := ioutil.ReadFile(configFile + "/" + projectName + "/webserver.json")
    
    if err != nil{
        return errors.New("Cannot find webserver config file for project:" + projectName)
    }
    
    var jsonResult webServerConfig
    err = json.Unmarshal([]byte(config), &jsonResult)
    if err != nil {
        return errors.New("Cannot parse webserver config file for project:" + projectName)
    }
    
    gin.SetMode(gin.ReleaseMode)
    engine := gin.New()  
    engine.Use(gin.Recovery())  
    
    this.ServiceCore     = engine
    this.ProjectName     = projectName
    this.ListenAddr      = jsonResult.ListenAddr
    this.ListenRetry     = jsonResult.ListenRetry
    this.ReadTimeout     = time.Duration(jsonResult.ReadTimeout)
    this.WriteTimeout    = time.Duration(jsonResult.WriteTimeout)
    
    return this.InitWebserver()
}

func (this *VascWebServer) Close() {
    this.HttpServer.Close()
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
    timer := time.NewTimer(time.Second * time.Duration(this.ListenRetry + 1))
    succ  := make(chan bool)
    defer close(succ)
    
    go func() {
    	for counter := 0; counter < this.ListenRetry; counter++ {
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

func (this *VascWebServer) LoadModules(modules []VascRoute) {
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
}

func DefaultMiddleware(c *gin.Context) {
}