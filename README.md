# vasc是什么
vasc是一个基于Go语言（golang）的web服务框架。vasc包括一套用于搭建web服务的数据结构和一个用于对外提供web服务的服务器。
# 如何获取vasc：
vasc的代码位于github.com/marxn/vasc
开发者在搭建完毕golang开发环境后，使用
```
go get github.com/marxn/vasc
```
获取到vasc的代码。在开发者的程序中导入"github.com/marxn/vasc" 就可以使用vasc库了。

# 如何使用vascserver搭建web服务

下面是一个使用vasc框架搭建的web服务器
```
package main

import (
    "github.com/gin-gonic/gin"
    "github.com/marxn/vasc"
)

//编写导出模块
func ExportModules() []vasc.VascRoute{
    return []vasc.VascRoute{
        vasc.VascRoute{ProjectName:"vasctest", Version:"1.0.100", Method:"GET",  Route:"/mary", Middleware: vasc.DefaultMiddleware, RouteHandler:  MaryHandler},
        vasc.VascRoute{ProjectName:"vasctest", Version:"1.0.100", Method:"POST", Route:"/bob",  Middleware: vasc.DefaultMiddleware, RouteHandler:  BobHandler}}
}

//vascserver的route机制是基于gin的，因此handler函数的形式与gin保持一致。
func MaryHandler(c *gin.Context) {
    height, exist := c.GetQuery("height")
    if !exist {
        c.JSON(400, gin.H {"error" : gin.H {"code": 400, "message": "Empty height"}})
        return
    }

    //返回结果
    c.JSON(200, gin.H {"code": 200, "message": "Hello Mary, your height is " + height})
}

func BobHandler(c *gin.Context) {
    weight, exist := c.GetQuery("weight")
    if !exist {
        c.JSON(400, gin.H {"error" : gin.H {"code": 400, "message": "Empty weight"}})
        return
    }

    //返回结果
    c.JSON(200, gin.H {"code": 200, "message": "Hello Bob. your weight is " + weight})
}

func main() {
    //设置工程名
    vasc.SetProjectName("mtr-gw")
    
    //初始化服务器框架，此调用解析命令行参数、生成运行日志、建立信号处理机制。
    vasc.InitServer()
    
    //获取配置文件
    profile := vasc.GetProfile()

    //建立pid文件
    vasc.GeneratePidFile() 
    
    //这里可以添加web服务模块。
    vasc.AddModules(ExportModules(profile))
    
    //开始服务.
    vasc.Serve()
    
}
```

以上代码创建了一个web服务器，它提供两个接口用于查询Mary和Bob的信息。vasc扩充了gin框架提供的HTTP方法，vasc支持FILE方法。用于搭建静态资源服务器。例如，在导出模块表时，可以用以下方式建立一个基于本地文件系统的静态资源下载服务：
```
func ExportModules() []vasc.VascRoute{
    return []vasc.VascRoute{
        vasc.VascRoute{Method:"FILE", Route:"/b/record", Middleware: vasc.DefaultMiddleware, LocalFilePath: "/var/data/download/"}}
}

```

# 如何启动vascserver
上述例子编译通过以后会产生一个名为vascserver的可执行文件。执行vasttest -h，出现以下提示：
```
Usage of ./maragoserver:
  -listen string
        listening address (default "localhost:8080")
  -log_level string
        log level(debug, info, warning, error) (default "debug")
  -log_path string
        vascserver log file path (default "./")
  -mode string
        running mode(debug, release) (default "release")
  -profile string
        profile for running environment(dev, test, online, ...) (default "dev")
```
以上提示是vascserver封装的命令行参数信息。例如，可以用以下格式启动vasctest：
```
vasctest -profile dev -listen locathost:80 -log_level warning -mode release
```
以上命令启动一个vascserver，监听本地80端口。日志级别是warning。用release模式运行。运行参数（数据库、redis等连接信息）从配置存储系统的dev项中获取

# 如何使用vasc访问数据库
对于数据库的访问，vasc直接使用github.com/go-sql-driver/mysql 作为MySQL数据库访问驱动。golang的database标准库具有原生支持连接池的特性，并且能够防范SQL注入攻击。vasc的封装仅提供建立数据库连接的接口并约定数据操作的流程。具体的业务数据库访问实现由开发者进行。当开发者利用vasc搭建服务时，建议将数据访问接口独立开发，例如在工程中编写如下风格的源代码：
```
package vasctest

import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "github.com/marxn/vasc"
)

type VascDBConn struct {
    DBHandle *sql.DB
}

func NewVascDBConn() (*VascDBConn, error) {
    dbConn, err := vasc.SetupDBConnection(dbEngine, dbUser, dbPasswd, dbHost, dbPort, dbName, dbCharset)
    if err!=nil {
        return nil, err
    }

    dbConn.SetMaxOpenConns(maxOpenConns)
    dbConn.SetMaxIdleConns(maxIdleConns)

    return &VascDBConn{DBHandle:dbConn}, nil
}

func (dbconn *MaraTrackSDKDBConn) testDB() {
    dbconn.DBHandle.Ping()
}

```
# 如何使用vasc记录日志
vasc提供以下接口用于记录日志：
```
func ErrorLog(format string, v ...interface{})
func InfoLog(format string, v ...interface{})
func WarnLog(format string, v ...interface{})
func DebugLog(format string, v ...interface{})
```
以上四个分别是debug、info、warning、error级别的日志对应的接口调用。vasc的日志是对syslog的封装，通过syslog服务写入文件系统。
例如：
```
ErrorLog("Module manager starting failed: %s", err.Error())
```

