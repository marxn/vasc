package global

import "github.com/gin-gonic/gin"

type WebServerConfig struct {
    Enable            bool           `json:"enable"`
    EnableLogger      bool           `json:"enable_logger"`
    ListenAddr        string         `json:"listen_address"`
    ListenRetry       int            `json:"listen_retry"`
    ReadTimeout       int            `json:"read_timeout"`
    WriteTimeout      int            `json:"write_timeout"`
    Monitor           bool           `json:"monitor"`
}

type VascRoute struct {
    Method         string             `json:"method"`
    Group          string             `json:"group"`
    Route          string             `json:"route"`
    HandlerName    string             `json:"route_handler"`
    RouteHandler   gin.HandlerFunc    `json:"-"`
    LocalFilePath  string             `json:"local_file_path"`
}

type VascRouteGroup struct {
    Group          string             `json:"group"`
    MiddlewareName string             `json:"middleware"`
}

type ControllerConfig struct {
    WebserverRoute   []VascRoute      `json:"webserver_route"`
    WebServerGroup   []VascRouteGroup `json:"webserver_route_group"`
    TaskList         []TaskInfo       `json:"task_list"`
    ScheduleList     []ScheduleInfo   `json:"schedule_list"`
}

type VascApplication struct {
    FuncMap            map[string]interface{}
    Configuration      string
    AppConfiguration   string
}

type TaskConfig struct {
    Enable           bool              `json:"enable"`
    LoadTaskDB       string            `json:"load_from_database"`
    GlobalQueueRedis string            `json:"global_queue_redis"`
}

type TaskInfo struct {
    Key         string                  `json:"task_key"`
    Type        uint64                  `json:"type"`
    Handler     func(interface{}) error `json:"-"`
    HandlerName string                  `json:"handler"`
    TaskQueue   chan(interface{})       `json:"-"`
    QueueSize   int64                   `json:"queue_size"`
    HandlerNum  int64                   `json:"handler_num"`
    Scope       int64                   `json:"scope"`
}

type ScheduleConfig struct {
    Enable                bool           `json:"enable"`
    LoadScheduleDB        string         `json:"load_from_database"`
    GlobalLockRedis       string         `json:"global_lock_redis"`
}

type ScheduleInfo struct {
    Key         string                   `json:"schedule_key"`  
    Routine     func () error            `json:"-"`
    HandlerName string                   `json:"handler"`
    Type        uint64                   `json:"type"`
    Timestamp   int64                    `json:"timestamp"`  
    Interval    int64                    `json:"interval"`
    Scope       int64                    `json:"scope"`
    LastRunTime int64                    `json:"-"`
}

type redisInstanceConfig struct {
    Key            string               `json:"key"`
    RedisHost      string               `json:"redis_host"`
    RedisPasswd    string               `json:"redis_passwd"`
    MaxIdle        int                  `json:"max_idle"`
    IdleTimeout    int                  `json:"idle_timeout"`
    Wait           bool                 `json:"wait"`
}

type RedisConfig struct {
    Enable         bool                 `json:"enable"`
    InstanceList []redisInstanceConfig  `json:"instance_list"`
}

type CacheConfigFile struct {
    Enable            bool           `json:"enable"`
    CacheRootPath     string         `json:"cache_rootpath"`
    CacheSourceRedis  string         `json:"cache_source_redis"`
}

type VascConfig struct {
    Database    *DatabaseConfig     `json:"database"`
    Redis       *RedisConfig        `json:"redis"`
    Webserver   *WebServerConfig    `json:"webserver"`
    LocalCache  *CacheConfigFile    `json:"localcache"`
    Scheduler   *ScheduleConfig     `json:"scheduler"`
    Task        *TaskConfig         `json:"task"`
}

type dbConfigItem struct {
    Key               string         `json:"key"`
    DatabaseConnstr   string         `json:"db_connstr"`
    Location          string         `json:"location"`
    MaxIdelConns      int            `json:"max_idle_conns"`
    MaxOpenConns      int            `json:"max_open_conns"`
}

type DatabaseConfig struct {
    Enable             bool          `json:"enable"`
    EnableLogger       bool          `json:"enable_logger"`
    InstanceList     []dbConfigItem  `json:"instance_list"`
}
