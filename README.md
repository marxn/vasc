# vasc
vasc是一个基于Go语言（golang）的web服务框架。它使用方法简便。例如，你可以用如下代码片段建立一个简单的web服务器：

```
package main

import (
    "github.com/marxn/vasc"
    "git.mararun.cn/b-end/vasctest"
)

func main() {

    //初始化服务器框架，此调用解析命令行参数、生成运行日志、建立信号处理机制。
    vasc.InitServer()
    
    //生成一个新的web服务实例。
    server := vasc.NewServer()
    
    //这里可以添加web服务模块。
    server.AddModules(maratracksdk.ExportModules())
    
    //开始进行web服务。
    server.Serve()
}
```

vasc包括一套用于搭建web服务的数据结构和一个用于对外提供web服务的服务器。承接上例，下面介绍如何使用vascserver搭建一个服务：
```
package vasctest

import (
    "github.com/gin-gonic/gin"
)

//编写导出模块
func ExportModules() []vasc.VascRoute{
    return []vasc.VascRoute{
        vasc.VascRoute{ProjectName:"vasctest", Version:"1.0.100", Method:"GET",  Route:"/mary", Middleware: vasc.DefaultMiddleware, RouteHandler:  MaryHandler},
        vasc.VascRoute{ProjectName:"vasctest", Version:"1.0.100", Method:"POST", Route:"/bob",  Middleware: vasc.DefaultMiddleware, RouteHandler:  Bobhandler},
}

//vascserver的route机制是基于gin的，因此handler函数的形式与gin保持一致。
func MaryHandler(c *gin.Context) {
    height, exist := c.GetQuery("height")
    if !exist {
        c.JSON(400, gin.H {"error" : gin.H {"code": 400, "message": "Empty height"}})
        return
    }
    
    //返回结果
    c.JSON(200, gin.H {"code": 200, "message": "Hello Mary"})
}

func MaryHandler(c *gin.Context) {
    weight, exist := c.GetQuery("weight")
    if !exist {
        c.JSON(400, gin.H {"error" : gin.H {"code": 400, "message": "Empty weight"}})
        return
    }
    
    //返回结果
    c.JSON(200, gin.H {"code": 200, "message": "Hello Bob"})
}
```
