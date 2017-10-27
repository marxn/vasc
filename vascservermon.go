package vasc

import (
    "github.com/gin-gonic/gin"
)

func MonitorHandler(c *gin.Context) {
    c.JSON(200, gin.H {"code": 200, "message": "OK"})
}

func ExportModules(profile string) []VascRoute{
    return []VascRoute{
        VascRoute{Method:"HEAD",  Route:"/monitor", Middleware: DefaultMiddleware, RouteHandler: MonitorHandler}}
}
