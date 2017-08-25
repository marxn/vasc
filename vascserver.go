package vasc

import (
    "crypto/tls"
    "io/ioutil"
    "net/http"
    "flag"
    "time"
    "os"
    "fmt"
    "syscall"
    "os/signal"
    "github.com/gin-gonic/gin"
)

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
            fmt.Printf("unknown signal received: %v\n", sig)
            os.Exit(1)
        }
    }
}

func internalServer(serviceHandler *gin.Engine) {
    
    if *conn_type == "https" {
        s := &http.Server{
            Addr:    *listen_addr,
            Handler: serviceHandler,
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
            Handler: serviceHandler,
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

func UpdateMaintenanceTool() {
    args   := os.Args
    script := fmt.Sprintf("sudo kill %d\n", os.Getpid())
    script  = fmt.Sprintf("%ssleep 1\n", script)
    script  = fmt.Sprintf("%smv %s.update %s\n", script, args[0])
    script  = fmt.Sprintf("%schmod u+x %s\n", script, args[0])
    script  = fmt.Sprintf("%ssudo nohup %s -server_type http -listen %s&\n", script, args[0], *listen_addr)    
    ioutil.WriteFile("./update.sh", []byte(script), 0766)
}

func VASCServer(modules []VascRoute) {
    
    conn_type      = flag.String("server_type",    "http",           "server type(http/https)")
    serv_cert_path = flag.String("serv_cert_path", "",               "server cert path(if https enabled)")
    serv_key_path  = flag.String("key_path",       "",               "server cert path(if https enabled)")
    listen_addr    = flag.String("listen",         "localhost:8080", "http(s) listen address")
    log_level      = flag.String("log_level",      "debug",          "log level(debug, info, warning, error)")

    flag.Parse()
    
    SetProjectName("vascserver/_all")
    
    switch *log_level {
    case "debug":
        SetLogLevel(LOG_DEBUG)
    case "info":
        SetLogLevel(LOG_INFO)
    case "warning":
        SetLogLevel(LOG_WARN)
    case "error":
        SetLogLevel(LOG_ERROR)
    }

    gin.SetMode(gin.DebugMode)
    serviceHandler := gin.Default()

    //serviceHandler.Use(Middleware)
    
    for i:=0; i < len(modules); i++ {
        
        switch modules[i].AccessMethod {
            case "GET"     : serviceHandler.GET(modules[i].AccessRoute, modules[i].RouteHandler)
            case "POST"    : serviceHandler.POST(modules[i].AccessRoute, modules[i].RouteHandler)
            case "OPTIONS" : serviceHandler.OPTIONS(modules[i].AccessRoute, modules[i].RouteHandler)
            case "PUT"     : serviceHandler.PUT(modules[i].AccessRoute, modules[i].RouteHandler)
            case "DELETE"  : serviceHandler.DELETE(modules[i].AccessRoute, modules[i].RouteHandler)
            default:
                VascLog(LOG_ERROR, "Unknown method: %s", modules[i].AccessMethod)
                fmt.Println("Unknown method: " + modules[i].AccessMethod)
        }
    }
    
    //Enable the running flag
    runnable = true
    
    //Start signal dispatching
    go vascSignalBlockingHandle()
    
    //Start services in background
    go internalServer(serviceHandler)
    
    //Ensure the service started correctly
    time.Sleep(5000000000)
    
    //To write process id in order to stop the server gracefully
    if runnable {
        UpdateMaintenanceTool()
    }
    
    for ;runnable; {
        time.Sleep(1000000000)
    }
    
}

