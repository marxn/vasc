package vasc

import (
	"time"
	"net/http"
	"errors"
	"github.com/gin-gonic/gin"
)

type webServerConfig struct {
    WebServerAddress  string         `json:"webserver_address"`
}

type VascWebServer struct {
    ProjectName     string
    ServiceCore    *gin.Engine
    ListenRetryTime int
    ListenAddr      string
    ReadTimeout     time.Duration
    WriteTimeout    time.Duration
}

type VascRoute struct {
    Method        string
    Route         string
    RouteHandler  gin.HandlerFunc
    Middleware    gin.HandlerFunc
    LocalFilePath string
}

func (this *VascWebServer) LoadConfig(configFile string, projectName string, profile string) error {
    gin.SetMode(gin.ReleaseMode)  //gin.SetMode(gin.DebugMode)
    this.ServiceCore     = gin.Default()
    this.ProjectName     = projectName
    this.ListenRetryTime = 3
    this.ReadTimeout     = 60
    this.WriteTimeout    = 60
    
    
    return nil
}

func (this *VascWebServer) Close() {
}

func (this *VascWebServer) SetAddr(addr string) {
    this.ListenAddr = addr
}

func (this *VascWebServer) Serve() error {
	httpServer := &http.Server{
		Addr:         this.ListenAddr,
		Handler:      this.ServiceCore,
		ReadTimeout:  this.ReadTimeout  * time.Second,
		WriteTimeout: this.WriteTimeout * time.Second,
	}

	err := httpServer.ListenAndServe()

	//Try to listen for some times in case of address in use
	counter := 0
	for {
		if err == nil {
			break
		}

		counter++
		time.Sleep(time.Second)
		err = httpServer.ListenAndServe()
		if err != nil && counter >= this.ListenRetryTime {
			return errors.New("ListenAndServe failed: " + err.Error())
		}
	}
	
	return nil
}

func (this *VascWebServer) AddModules(modules []VascRoute) {
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
func (this *VascWebServer) DefaultMiddleware(c *gin.Context) {
}