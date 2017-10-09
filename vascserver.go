package vasc

import (
    "os"
    //"io"
    "fmt"
    "flag"
    "time"
    "syscall"
    "io/ioutil"
    "net/http"
    "os/signal"
    "os/exec"
    "github.com/gin-gonic/gin"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

const serviceLoopIntervalNS   = 1000000000

type signalHandler func(s os.Signal, arg interface{})

type signalSet struct {
    m map[os.Signal]signalHandler
}

var runnable        bool
var listen_addr    *string
var log_level      *string
var mode           *string
var log_path       *string
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
    fmt.Printf("SIGUSR signal received. Stopping server...\n")
    runnable = false
}

func vascServerSigIgnoreHandler(s os.Signal, arg interface{}) {
    fmt.Printf("SIGHUP signal received. Ignore.\n")
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
    ss.register(syscall.SIGHUP,  vascServerSigIgnoreHandler)

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
    
    result := fmt.Sprintf("Project             Version             Method    Route\n")
    result += fmt.Sprintf("------------------- ------------------- --------- ----------------------------------\n")
    
    for i:=0; i < len(module_list); i++ {
       result += fmt.Sprintf("%-20s%-20s%-10s%-20s\n", module_list[i].ProjectName, module_list[i].Version, module_list[i].Method, module_list[i].Route)
    }
    
    c.String(200, result)
}

func NewServer() *VascServer {
    
    result  := gin.Default()
    manager := gin.Default()        
    
    manager.GET("checkmodules", listModules)
    
    return &VascServer{serviceCore:result, moduleManager:manager}
}

func (server *VascServer) AddModules(modules []VascRoute) {
    
    for i:=0; i < len(modules); i++ {
        switch modules[i].Method {
            case "GET"     : server.serviceCore.GET(modules[i].Route,     modules[i].Middleware, modules[i].RouteHandler)
            case "POST"    : server.serviceCore.POST(modules[i].Route,    modules[i].Middleware, modules[i].RouteHandler)
            case "OPTIONS" : server.serviceCore.OPTIONS(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
            case "PUT"     : server.serviceCore.PUT(modules[i].Route,     modules[i].Middleware, modules[i].RouteHandler)
            case "DELETE"  : server.serviceCore.DELETE(modules[i].Route,  modules[i].Middleware, modules[i].RouteHandler)
            case "PATCH"   : server.serviceCore.PATCH(modules[i].Route,   modules[i].Middleware, modules[i].RouteHandler)
	        case "HEAD"    : server.serviceCore.HEAD(modules[i].Route,    modules[i].Middleware, modules[i].RouteHandler)
            case "FILE"    : server.serviceCore.StaticFS(modules[i].Route, http.Dir(modules[i].LocalFilePath))
            default:
                VascLog(LOG_ERROR, "Unknown method: %s", modules[i].Method)
                fmt.Println("Unknown method: " + modules[i].Method)
                continue
        }
        
        module_list = append(module_list, modules[i])
    }
}

var logFileHandle  *os.File
var lastSecondDate  string
var vascLogWriter  *vascServerLogWriter

type vascServerLogItem struct {
    timestamp string
    logitem   string
}

type vascServerLogWriter struct {
    logBuffer chan vascServerLogItem
}

func vascGetNewLogWriter() *vascServerLogWriter {
    result := &vascServerLogWriter {
        logBuffer : make(chan vascServerLogItem)}
    
    return result
}

func (w *vascServerLogWriter) Write(p []byte) (n int, err error) {
    w.logBuffer <- vascServerLogItem {timestamp:time.Now().Format("2006-01-02"), logitem:string(p)}
    return len(p), nil
}

func qualifyPath(path string) string{
    ret := ""
    
    if len(path)==0 {
        ret = "./";
    } else if(path[len(path) - 1]!='/') {
        ret = path + "/";
    } else {
        ret = path;
    }
    
    return ret;

}

func exec_shell(s string) {
    cmd := exec.Command("/bin/bash", "-c", s)
    cmd.Run()
}

func vascServerLogFileUpdate(serverLogFile string) {
    logFileHandle, _  = os.OpenFile(qualifyPath(*log_path) + serverLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
    lastSecondDate    = time.Now().Format("2006-01-02")
}

func vascServerLogRotating(w * vascServerLogWriter) {
    vascServerLogFileUpdate("vascserver-" + time.Now().Format("2006-01-02") + ".log")
    for ;runnable; {
        logItem := <- w.logBuffer
        if logItem.timestamp != lastSecondDate {
            logFileHandle.Close()
            
            //Compress rotated log file
            cmd := "gzip " + qualifyPath(*log_path) + "vascserver-" + lastSecondDate + ".log"
            go exec_shell(cmd)
            
            //Generate a new log file
            vascServerLogFileUpdate("vascserver-" + time.Now().Format("2006-01-02") + ".log")
        }
        
        logFileHandle.WriteString(logItem.logitem)
    }
}

func (server *VascServer) Serve () {
    
    //Launch module manager
    go func () {
        s := &http.Server{
            Addr:    "127.0.0.1:30145",
            Handler: server.moduleManager,
        }

        VascLog(LOG_INFO, "Starting module manager... ")
        err := s.ListenAndServe()
        
        if err != nil {
            VascLog(LOG_ERROR, "Module manager starting failed: %s", err.Error())
            fmt.Println("Module manager failed: " + err.Error())
            os.Exit(-1)
        }
    }()

    //Start signal dispatching
    go vascSignalBlockingHandle()
    
    //Start services in background
    go func() {
        httpServer := &http.Server{
            Addr:    *listen_addr,
            Handler: server.serviceCore,
        }

        VascLog(LOG_INFO, "Service starting... ")
        err := httpServer.ListenAndServe()
        if err != nil {
            VascLog(LOG_ERROR, "vascserver service starting failed: %s", err.Error())
            fmt.Println("ListenAndServe failed: " + err.Error())
            os.Exit(-1)
        }
    }()
    
    //Ensure the service started correctly
    time.Sleep(serviceLoopIntervalNS)
    
    //To write process id in order to stop the server gracefully
    UpdateMaintenanceTool()
    
    runnable = true
    
    //Log file writing & rotating
    go vascServerLogRotating(vascLogWriter)
    
    for ;runnable; {
        time.Sleep(serviceLoopIntervalNS)
    }
    
    fmt.Println("Service terminated.")
}

func UpdateMaintenanceTool() {
    args   := os.Args
    script := fmt.Sprintf("kill %d\n", os.Getpid())
    script  = fmt.Sprintf("%smv %s.update %s\n", script, args[0], args[0])
    script  = fmt.Sprintf("%schmod u+x %s\n", script, args[0])
    ioutil.WriteFile("./vasc_update.sh", []byte(script), 0766)
}

func InitServer() error {

    listen_addr    = flag.String("listen",        "localhost:8080", "listening address")
    mode           = flag.String("mode",          "release",        "running mode(debug, release)")
    log_path       = flag.String("log_path",      "./",             "vascserver log file path")
    log_level      = flag.String("log_level",     "debug",          "log level(debug, info, warning, error)")
    flag.Parse()
        
    gin.DisableConsoleColor()
    gin.SetMode(gin.ReleaseMode)
    
    vascLogWriter = vascGetNewLogWriter()
    gin.DefaultWriter = vascLogWriter
    
    if *mode=="debug" {
        gin.SetMode(gin.DebugMode)
    }
    
    log_level_num := LOG_DEBUG
    
    switch *log_level {
    case "debug":
        log_level_num = LOG_DEBUG
    case "info":
        log_level_num = LOG_INFO
    case "warning":
        log_level_num = LOG_WARN
    case "error":
        log_level_num = LOG_ERROR
    }
    
    SetLogLevel(log_level_num)
    
    return nil
}

func SetupDBConnection(dbEngine, dbUser, dbPassword, dbHost, dbPort, dbName, dbCharset string) (*sql.DB, error) {
    return sql.Open(dbEngine, fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", dbUser, dbPassword, dbHost, dbPort, dbName, dbCharset))
}