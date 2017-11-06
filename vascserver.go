package vasc

import (
    "os"
    "fmt"
    "flag"
    "time"
    "syscall"
    "net/http"
    "os/signal"
    "io/ioutil"
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
var profile        *string
var pidfile        *string
var listen_addr    *string
var watch_addr     *string
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

func vascServerStopHandler(s os.Signal, arg interface{}) {
    fmt.Printf("Stop signal received. Stopping server...\n")
    runnable = false
}

func vascServerReloadHandler(s os.Signal, arg interface{}) {
    fmt.Printf("Reload signal received. Ignore.\n")
}

func vascServerSigIgnoreHandler(s os.Signal, arg interface{}) {
    fmt.Printf("Exceptional signal received. Ignore.\n")
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

    ss.register(syscall.SIGUSR1, vascServerStopHandler)
    ss.register(syscall.SIGUSR2, vascServerReloadHandler)
    ss.register(syscall.SIGHUP,  vascServerSigIgnoreHandler)
    ss.register(syscall.SIGCHLD, vascServerSigIgnoreHandler)

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
    
    result := fmt.Sprintf("Version             Method    Route\n")
    result += fmt.Sprintf("------------------- --------- ----------------------------------\n")
    
    for i:=0; i < len(module_list); i++ {
       result += fmt.Sprintf("%-10s%-20s\n", module_list[i].Method, module_list[i].Route)
    }
    
    c.String(200, result)
}

var serviceCore   *gin.Engine
var moduleManager *gin.Engine

func AddModules(modules []VascRoute) {
    
    for i:=0; i < len(modules); i++ {
        switch modules[i].Method {
            case "GET"     : serviceCore.GET(modules[i].Route,     modules[i].Middleware, modules[i].RouteHandler)
            case "POST"    : serviceCore.POST(modules[i].Route,    modules[i].Middleware, modules[i].RouteHandler)
            case "OPTIONS" : serviceCore.OPTIONS(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
            case "PUT"     : serviceCore.PUT(modules[i].Route,     modules[i].Middleware, modules[i].RouteHandler)
            case "DELETE"  : serviceCore.DELETE(modules[i].Route,  modules[i].Middleware, modules[i].RouteHandler)
            case "PATCH"   : serviceCore.PATCH(modules[i].Route,   modules[i].Middleware, modules[i].RouteHandler)
	        case "HEAD"    : serviceCore.HEAD(modules[i].Route,    modules[i].Middleware, modules[i].RouteHandler)
            case "FILE"    : serviceCore.StaticFS(modules[i].Route, http.Dir(modules[i].LocalFilePath))
            default:
                ErrorLog("Unknown method: %s", modules[i].Method)
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
    exec_shell("mkdir " + qualifyPath(*log_path))
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

func Serve () {
    
    //Install module manager for listing
    moduleManager.GET("checkmodules", listModules)
    
    //Launch module manager
    if(*watch_addr!="") {
        go func () {
            s := &http.Server{
                Addr:    *watch_addr,
                Handler: moduleManager,
            }

            InfoLog("Starting module manager... ")
            err := s.ListenAndServe()
            
            if err != nil {
                ErrorLog("Module manager starting failed: %s", err.Error())
                fmt.Println("Module manager failed: " + err.Error())
                os.Exit(-1)
            }
        }()
    }
    //Start signal dispatching
    go vascSignalBlockingHandle()
    
    //Start web services in background
    go func() {
        httpServer := &http.Server{
            Addr:    *listen_addr,
            Handler: serviceCore,
        }

        InfoLog("Service starting... ")
        err := httpServer.ListenAndServe()
        if err != nil {
            ErrorLog("vascserver service starting failed: %s", err.Error())
            fmt.Println("ListenAndServe failed: " + err.Error())
            os.Exit(-1)
        }
    }()
    
    //Ensure the service started correctly
    time.Sleep(serviceLoopIntervalNS)
    
    runnable = true
    
    //Log file redirecting & rotating
    go vascServerLogRotating(vascLogWriter)
    
    for ;runnable; {
        time.Sleep(serviceLoopIntervalNS)
    }
    
    fmt.Println("Service terminated.")
}

func InitServer(project_name string) error {
    
    SetProjectName(project_name)
    
    listen_addr  = flag.String("listen",      "localhost:8080",          "listening address")
    watch_addr   = flag.String("watch_addr",  "",                        "watch address")
    profile      = flag.String("profile",     "dev",                     "profile for running environment(dev, test, online, ...)")
    pidfile      = flag.String("pidfile",     "/var/run/vascserver.pid", "pid filename")
    mode         = flag.String("mode",        "release",                 "running mode(debug, release)")
    log_path     = flag.String("log_path",    "./",                      "vascserver log file path")
    log_level    = flag.String("log_level",   "debug",                   "log level(debug, info, warning, error)")
    
    flag.Parse()
        
    GeneratePidFile()
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
    
    serviceCore   = gin.Default()
    moduleManager = gin.Default()
    
    return nil
}

func GetProfile() string {
    return *profile
}

func GetMode() string {
    return *mode
}

func GeneratePidFile() {
    fmt.Println("Writing pid file...")
    pid     := fmt.Sprintf("%d", os.Getpid())
    err := ioutil.WriteFile(*pidfile, []byte(pid), 0666)
    if err!=nil {
        fmt.Println("Cannot write pid file:" + err.Error())
    }
}

func SetupDBConnection(dbEngine, dbUser, dbPassword, dbHost, dbPort, dbName, dbCharset string) (*sql.DB, error) {
    return sql.Open(dbEngine, fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", dbUser, dbPassword, dbHost, dbPort, dbName, dbCharset))
}