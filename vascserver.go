package vasc

import (
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"
	"os/exec"
	"os/signal"
	"io/ioutil"
	"net/http"
	"github.com/gin-gonic/gin"
)

const serviceLoopIntervalNS = 1000000000
const ListenRetryTime = 3

type signalHandler func(s os.Signal, arg interface{})

type signalSet struct {
	m map[os.Signal]signalHandler
}

var runnable bool
var profile *string
var pidfile *string
var listen_addr *string
var log_level *string
var mode *string
var module_list []VascRoute

func signalSetNew() *signalSet {
	ss := new(signalSet)
	ss.m = make(map[os.Signal]signalHandler)
	return ss
}

func (set *signalSet) register(s os.Signal, handler signalHandler) {
	if _, found := set.m[s]; !found {
		set.m[s] = handler
	}
}

func vascServerStopHandler(s os.Signal, arg interface{}) {
	//fmt.Printf("Stop signal received. Stopping server...\n")
	runnable = false
}

func vascServerReloadHandler(s os.Signal, arg interface{}) {
	//fmt.Printf("Reload signal received. Ignore.\n")
}

func vascServerSigIgnoreHandler(s os.Signal, arg interface{}) {
	//fmt.Printf("Exceptional signal received. Ignore.\n")
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
	ss.register(syscall.SIGHUP, vascServerSigIgnoreHandler)
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
		if err != nil {
			runnable = false
			fmt.Printf("Unknown signal received: %v\n", sig)
			os.Exit(1)
		}
	}
}

var serviceCore *gin.Engine

func AddModules(modules []VascRoute) {

	for i := 0; i < len(modules); i++ {
		switch modules[i].Method {
		case "GET":
			serviceCore.GET(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "POST":
			serviceCore.POST(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "OPTIONS":
			serviceCore.OPTIONS(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "PUT":
			serviceCore.PUT(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "DELETE":
			serviceCore.DELETE(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "PATCH":
			serviceCore.PATCH(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "HEAD":
			serviceCore.HEAD(modules[i].Route, modules[i].Middleware, modules[i].RouteHandler)
		case "FILE":
			serviceCore.StaticFS(modules[i].Route, http.Dir(modules[i].LocalFilePath))
		default:
			ErrorLog("Unknown method: %s", modules[i].Method)
			fmt.Println("Unknown method: " + modules[i].Method)
			continue
		}
	}
}

type vascServerLogItem struct {
	timestamp string
	logitem   string
}

type vascServerLogWriter struct {
	logBuffer chan vascServerLogItem
}

func vascGetNewLogWriter() *vascServerLogWriter {
	result := &vascServerLogWriter{
		logBuffer: make(chan vascServerLogItem)}

	return result
}

func (w *vascServerLogWriter) Write(p []byte) (n int, err error) {
	w.logBuffer <- vascServerLogItem{timestamp: time.Now().Format("2006-01-02"), logitem: string(p)}
	return len(p), nil
}

func qualifyPath(path string) string {
	ret := ""

	if len(path) == 0 {
		ret = "./"
	} else if path[len(path)-1] != '/' {
		ret = path + "/"
	} else {
		ret = path
	}

	return ret

}

func exec_shell(s string) {
	cmd := exec.Command("/bin/bash", "-c", s)
	cmd.Run()
}

func Serve() {
	//Start signal dispatching
	go vascSignalBlockingHandle()

	//Start web services in background
	go func() {
		httpServer := &http.Server{
			Addr:         *listen_addr,
			Handler:      serviceCore,
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
		}

		InfoLog("Service starting... ")

		err := httpServer.ListenAndServe()

		//Try to listen for some time in case of address in use
		counter := 0
		for {
			if err == nil {
				break
			}

			counter++
			time.Sleep(time.Second)
			fmt.Printf("ListenAndServe failed:[%s] retry %d times...\n", counter)

			err = httpServer.ListenAndServe()
			if err != nil && counter >= ListenRetryTime {
				ErrorLog("vascserver service starting failed: %s", err.Error())
				fmt.Println("ListenAndServe failed: " + err.Error())
				os.Exit(-1)
			}
		}
	}()

	//Ensure the service started correctly
	time.Sleep(serviceLoopIntervalNS)

	runnable = true

	for runnable {
		time.Sleep(serviceLoopIntervalNS)
	}

	fmt.Println("Service terminated.")
}

func InitServer(project_name string) error {

	SetProjectName(project_name)

	listen_addr = flag.String("listen",     "localhost:8080", "listening address")
	profile     = flag.String("profile",    "dev",            "profile for running environment(dev, test, online, ...)")
	pidfile     = flag.String("pidfile",    "",               "pid filename")
	mode        = flag.String("mode",       "release",        "running mode(debug, release, bootstrap)")
	log_level   = flag.String("log_level",  "debug",          "log level(debug, info, warning, error)")

	flag.Parse()
    
    if *pidfile != "" {
	    GeneratePidFile()
	}
	
	gin.SetMode(gin.ReleaseMode)

	if *mode == "debug" {
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

	serviceCore = gin.Default()

	return nil
}

func GetProfile() string {
	return *profile
}

func GetMode() string {
	return *mode
}

func GeneratePidFile() {
	pid := fmt.Sprintf("%d", os.Getpid())
	err := ioutil.WriteFile(*pidfile, []byte(pid), 0666)
	if err != nil {
		fmt.Println("Cannot write pid file:" + err.Error())
	}
}
