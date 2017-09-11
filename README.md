# vasc
vasc是一个基于Go语言（golang）的web服务框架。它使用方法简便。例如，你可以用如下代码片段建立一个简单的web服务：  

···
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
···
