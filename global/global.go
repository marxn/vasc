package global

import "time"
//import "sync"
//import "net/http"
import "github.com/gin-gonic/gin"
//import "github.com/go-xorm/xorm"
//import "github.com/garyburd/redigo/redis"

type WebServerConfig struct {
    Enable            bool           `json:"enable"`
    ListenAddr        string         `json:"listen_address"`
    ListenRetry       int            `json:"listen_retry"`
    ReadTimeout       int            `json:"read_timeout"`
    WriteTimeout      int            `json:"write_timeout"`
}

type VascRoute struct {
    Method         string             `json:"method"`
    Route          string             `json:"route"`
    HandlerName    string             `json:"route_handler"`
    RouteHandler   gin.HandlerFunc    `json:"-"`
    MiddlewareName string             `json:"middleware"`
    Middleware     gin.HandlerFunc    `json:"-"`
    LocalFilePath  string             `json:"local_file_path"`
}

type VascApplication struct {
    WebserverRoute   []VascRoute                   `json:"webserver_route"`
    TaskList         []TaskInfo                    `json:"task_list"`
    ScheduleList     []ScheduleInfo                `json:"schedule_list"`
    FuncMap            map[string]interface{}      `json:"-"`
}

type TaskConfig struct {
    Enable           bool           `json:"enable"`
    LoadTaskDB       string         `json:"load_from_database"`
    GlobalQueueRedis string         `json:"global_queue_redis"`
}

type VascTaskDB struct {
    TaskID            int64     `xorm:"BIGINT PK AUTOINCR 'TASK_ID'"`  
    TaskKey           string    `xorm:"VARCHAR(128) NOT NULL UNIQUE 'TASK_KEY'"`
    TaskFuncName      string    `xorm:"VARCHAR(128) NOT NULL 'TASK_FUNC_NAME'"`
    TaskHandlerNum    int64     `xorm:"BIGINT 'TASK_HANDLER_NUM'"`
    TaskQueueSize     int64     `xorm:"BIGINT 'TASK_QUEUE_SIZE'"`
    TaskScope         int64     `xorm:"BIGINT 'TASK_SCOPE'"`
    CreatedTime       time.Time `xorm:"CREATED 'TASK_CREATED_TIME'"`
    UpdatedTime       time.Time `xorm:"UPDATED 'TASK_UPDATED_TIME'"`
}

func (this *VascTaskDB) TableName() string {
    return "VASC_TASK"
}

type TaskInfo struct {
    Key         string                          `json:"task_key"`
    Handler     func(content interface{}) error `json:"-"`
    HandlerName string                          `json:"handler"`
    TaskQueue   chan(interface{})               `json:"-"`
    QueueSize   int64                           `json:"queue_size"`
    HandlerNum  int64                           `json:"handler_num"`
    Scope       int64                           `json:"scope"`
}

type ScheduleConfig struct {
    Enable                bool           `json:"enable"`
    LoadScheduleDB        string         `json:"load_from_database"`
    GlobalLockRedis       string         `json:"global_lock_redis"`
}

type ScheduleInfo struct {
    Key         string                   `json:"schedule_key"`  
    Routine     func (interface{}) error `json:"-"`
    HandlerName string                   `json:"handler"`
    Type        uint64                   `json:"type"`
    Timestamp   int64                    `json:"timestamp"`  
    Interval    int64                    `json:"interval"`
    Scope       int64                    `json:"scope"`
    LastRunTime int64                    `json:"-"`
}

type VascSchedulerDB struct {
    ScheduleID        int64     `xorm:"BIGINT PK AUTOINCR 'SCHEDULE_ID'"`  
    ScheduleKey       string    `xorm:"VARCHAR(128) NOT NULL UNIQUE 'SCHEDULE_KEY'"`
    ScheduleFuncName  string    `xorm:"VARCHAR(128) NOT NULL 'SCHEDULE_FUNC_NAME'"`
    ScheduleType      uint64    `xorm:"BIGINT 'SCHEDULE_TYPE'"`
    ScheduleTimestamp int64     `xorm:"BIGINT 'SCHEDULE_TIMESTAMP'"`
    ScheduleInterval  int64     `xorm:"BIGINT 'SCHEDULE_INTERVAL'"`
    ScheduleScope     int64     `xorm:"BIGINT 'SCHEDULE_SCOPE'"`
    CreatedTime       time.Time `xorm:"CREATED 'SCHEDULE_CREATED_TIME'"`
    UpdatedTime       time.Time `xorm:"UPDATED 'SCHEDULE_UPDATED_TIME'"`
}

func (this *VascSchedulerDB) TableName() string {
    return "VASC_SCHEDULER"
}

const VASC_SCHEDULE_FIXED      = 1
const VASC_SCHEDULE_OVERLAPPED = 2
const VASC_SCHEDULE_SERIAL     = 3

const VASC_SCHEDULE_SCOPE_NATIVE = 1
const VASC_SCHEDULE_SCOPE_HOST   = 2
const VASC_SCHEDULE_SCOPE_GLOBAL = 3

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

const VASC_NONE      = 0x0
const VASC_WEBSERVER = 0x01 << 1
const VASC_CACHE     = 0x01 << 2
const VASC_DB        = 0x01 << 3
const VASC_REDIS     = 0x01 << 4
const VASC_SCHEDULER = 0x01 << 5
const VASC_TASK      = 0x01 << 6

type VascConfig struct {
    Database    *DatabaseConfig     `json:"database"`
    Redis       *RedisConfig        `json:"redis"`
    Webserver   *WebServerConfig    `json:"webserver"`
    LocalCache  *CacheConfigFile    `json:"localcache"`
    Scheduler   *ScheduleConfig     `json:"scheduler"`
    Task        *TaskConfig         `json:"task"`
    Application *VascApplication    `json:"application"`
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
    InstanceList     []dbConfigItem  `json:"instance_list"`
}

type VascRoutine func (interface{}) error
