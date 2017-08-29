package vasc

import (
    "os"
    "fmt"
    "flag"
    "time"
    "syscall"
    "crypto/tls"
    "io/ioutil"
    "net/http"
    "os/signal"
    "github.com/gin-gonic/gin"
)

const launchServiceWaitTimeNS = 5000000000
const serviceLoopIntervalNS   = 1000000000

type signalHandler func(s os.Signal, arg interface{})

type signalSet struct {
    m map[os.Signal]signalHandler
}

var runnable        bool
var finished        bool
var conn_type      *string
var serv_cert_path *string
var serv_key_path  *string
var listen_addr    *string
var log_level      *string
var module_list    []VascRoute

func signalSetNew()(*signalSet){
    ss := new(signalSet)
    ss.m = make(map[os.Signal]signalHandler)
    return ss
}

func (set *signalSet) register(s os.Signal, handler signalHandler) {
    if _, found := set.m[s]; !found {
        set.m[s] =  handler
    }
}

func vascServerSigHandler(s os.Signal, arg interface{}) {
    fmt.Printf("SIGUSER signal received. Stopping server...\n")
    runnable = false
}
    
func (set *signalSet) handle(sig os.Signal, arg interface{}) (err error) {
    if _, found := set.m[sig]; found {
        set.m[sig](sig, arg)
        return nil
    } else {
        return fmt.Errorf("No handler available for signal %v", sig)
    }

    panic("won't reach here")
}

func vascSignalBlockingHandle() {
    ss := signalSetNew()

    ss.register(syscall.SIGUSR1, vascServerSigHandler)
    ss.register(syscall.SIGUSR2, vascServerSigHandler)

    for {
        c := make(chan os.Signal)
        var sigs []os.Signal
        for sig := range ss.m {
            sigs = append(sigs, sig)
        }
        
        signal.Notify(c)
        sig := <-c

        err := ss.handle(sig, nil)
        if (err != nil) {
            runnable = false
            fmt.Printf("Unknown signal received: %v\n", sig)
            os.Exit(1)
        }
    }
}

func listModules(c *gin.Context) {
    
    result := fmt.Sprintf("Project             Version             Host                Method    Route\n")
    result += fmt.Sprintf("--------------------------------------------------------------------------------------------------------\n")
    
    for i:=0; i < len(module_list); i++ {
       result += fmt.Sprintf("%-20s%-20s%-20s%-10s%-20s\n", module_list[i].ProjectName, module_list[i].Version, module_list[i].Host, module_list[i].AccessMethod, module_list[i].AccessRoute)
    }
    
    c.String(200, result)
}

func NewServer(middleware func(c *gin.Context)) *VascServer {
    
    result := gin.Default()
    result.Use(middleware)

    manager := gin.Default()        
    manager.GET("checkmodules", listModules)
    
    return &VascServer{serviceCore:result, moduleManager:manager}
}

func (server *VascServer) AddModules(modules []VascRoute) {
    
    for i:=0; i < len(modules); i++ {
        switch modules[i].AccessMethod {
            case "GET"     : server.serviceCore.GET(modules[i].AccessRoute, modules[i].RouteHandler)
            case "POST"    : server.serviceCore.POST(modules[i].AccessRoute, modules[i].RouteHandler)
            case "OPTIONS" : server.serviceCore.OPTIONS(modules[i].AccessRoute, modules[i].RouteHandler)
            case "PUT"     : server.serviceCore.PUT(modules[i].AccessRoute, modules[i].RouteHandler)
            case "DELETE"  : server.serviceCore.DELETE(modules[i].AccessRoute, modules[i].RouteHandler)
            default:
                VascLog(LOG_ERROR, "Unknown method: %s", modules[i].AccessMethod)
                fmt.Println("Unknown method: " + modules[i].AccessMethod)
                continue
        }
        
        module_list = append(module_list, modules[i])
    }
}

func (server *VascServer) internalServer() {
    
    if *conn_type == "https" {
        s := &http.Server{
            Addr:    *listen_addr,
            Handler: server.serviceCore,
            TLSConfig: &tls.Config{
                ClientAuth: tls.NoClientCert,
            },
        }

        VascLog(LOG_INFO, "Service starting for [https]... ")
        err := s.ListenAndServeTLS(*serv_cert_path, *serv_key_path)

        if err != nil {
            VascLog(LOG_ERROR, "vascserver service starting failed: %s", err.Error())
            fmt.Println("ListenAndServeTLS failed: " + err.Error())
        }
    } else {
        s := &http.Server{
            Addr:    *listen_addr,
            Handler: server.serviceCore,
        }

        VascLog(LOG_INFO, "Service starting for [http]... ")
        err := s.ListenAndServe()
        if err != nil {
            VascLog(LOG_ERROR, "vascserver service starting failed: %s", err.Error())
            fmt.Println("ListenAndServe failed: " + err.Error())
        }
    }
    
    runnable = false
    finished = true
}

func (server *VascServer) vascModuleManager() {
    
    s := &http.Server{
        Addr:    "127.0.0.1:30145",
        Handler: server.moduleManager,
    }

    VascLog(LOG_INFO, "Starting module manager... ")
    err := s.ListenAndServe()
    
    if err != nil {
        VascLog(LOG_ERROR, "Module manager starting failed: %s", err.Error())
        fmt.Println("Module manager failed: " + err.Error())
    }
}

func (server *VascServer) Serve () {
    
    //Enable the running flag
    runnable = true
    
    //Start signal dispatching
    go vascSignalBlockingHandle()
    
    //Launch module manager thread
    go server.vascModuleManager()
    
    //Start services in background
    go server.internalServer()
    
    //Ensure the service started correctly
    time.Sleep(launchServiceWaitTimeNS)
    
    //To write process id in order to stop the server gracefully
    if runnable {
        UpdateMaintenanceTool()
    }
    
    for ;runnable; {
        time.Sleep(serviceLoopIntervalNS)
    }
    
}

func UpdateMaintenanceTool() {
    args   := os.Args
    script := fmt.Sprintf("sudo kill %d\n", os.Getpid())
    script  = fmt.Sprintf("%ssleep 1\n", script)
    script  = fmt.Sprintf("%smv %s.update %s\n", script, args[0], args[0])
    script  = fmt.Sprintf("%schmod u+x %s\n", script, args[0])
    script  = fmt.Sprintf("%ssudo nohup %s -server_type http -listen %s&\n\n", script, args[0], *listen_addr)    
    ioutil.WriteFile("./vasc_update.sh", []byte(script), 0766)
}


func InitServer(serverName string) {
    
    conn_type      = flag.String("server_type",   "http",           "server type(http/https)")
    serv_cert_path = flag.String("certfile_path", "",               "server cert path(if https enabled)")
    serv_key_path  = flag.String("keyfile_path",  "",               "server cert path(if https enabled)")
    listen_addr    = flag.String("listen",        "localhost:8080", "listening address")
    log_level      = flag.String("log_level",     "debug",          "log level(debug, info, warning, error)")

    flag.Parse()
    
    SetProjectName(serverName)
    
    gin.SetMode(gin.ReleaseMode)
    
    switch *log_level {
    case "debug":
        SetLogLevel(LOG_DEBUG)
        gin.SetMode(gin.DebugMode)
    case "info":
        SetLogLevel(LOG_INFO)
    case "warning":
        SetLogLevel(LOG_WARN)
    case "error":
        SetLogLevel(LOG_ERROR)
    }
}

