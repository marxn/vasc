# vasc
vasc是一个基于Go语言（golang）的web服务框架。vasc包括一套用于搭建web服务的数据结构和一个用于对外提供web服务的服务器。
## 如何获取vasc：
vasc的代码位于github.com/marxn/vasc
开发者在搭建完毕golang开发环境后，使用go get github.com/marxn/vasc 命令获取vasc的代码。在开发者的程序中导入"github.com/marxn/vasc" 就可以使用vasc库了。下面介绍如何使用vascserver搭建一个web服务：

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
以上代码创建了一个web服务器，它提供两个接口用于查询Mary和Bob的信息。
