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

下面是一个使用vasc框架尖利的web服务器
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

    //初始化服务器框架，此调用解析命令行参数、生成运行日志、建立信号处理机制。
    vasc.InitServer()
    
    //生成一个新的web服务实例。
    server := vasc.NewServer()
    
    //这里可以添加web服务模块。
    server.AddModules(ExportModules())
    
    //开始进行web服务。
    server.Serve()
}
```

以上代码创建了一个web服务器，它提供两个接口用于查询Mary和Bob的信息。vasc扩充了gin框架提供的HTTP方法，vasc支持FILE方法。用于搭建静态资源服务器。例如，在导出模块表时，可以用以下方式建立一个基于本地文件系统的静态资源下载服务：
```
func ExportModules() []vasc.VascRoute{
    return []vasc.VascRoute{
        vasc.VascRoute{ProjectName:"vasctest", Version:"1.0.100", Method:"FILE", Route:"/b/record", Middleware: vasc.DefaultMiddleware, LocalFilePath: "/var/data/download/"}}
}

```

# 如何使用vasc访问redis和mysql
vasc引入redigo开源库实现了redis的访问机制。由于它的功能较为完善，vasc并未对其进行进一步封装。
对于mysql的访问，vasc直接使用github.com/go-sql-driver/mysql 作为数据库访问层。golang的database标准库具有原生支持连接池的特性，并且能够SQL注入攻击，因此vasc并未对其进行进一步封装。

