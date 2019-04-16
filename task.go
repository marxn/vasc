package vasc

import "fmt"
import "errors"
import "sync"
import "time"
import "github.com/go-xorm/xorm"
import "github.com/garyburd/redigo/redis"

type taskConfig struct {
    Enable           bool           `json:"enable"`
    LoadTaskDB       string         `json:"load_from_database"`
    GlobalQueueRedis string         `json:"global_queue_redis"`
}

type VascTaskDB struct {
    TaskID            int64     `xorm:"BIGINT PK AUTOINCR 'TASK_ID'"`  
    TaskKey           string    `xorm:"VARCHAR(128) NOT NULL INDEX(INDEX1) 'TASK_KEY'"`
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
    Key        string
    Handler    func(content interface{}) error
    TaskQueue  chan(interface{})
    QueueSize  int64
    HandlerNum int64
    Scope      int64
}

type VascTask struct {
    ProjectName        string
    RedisHost          string
    RedisPasswd        string
    RedisConn         *redis.Pool
    RedisPrefix        string
    Runnable           bool
    DBConn            *xorm.Engine
    FuncMap            map[string]VascRoutine
    TaskList           map[string]*TaskInfo
    taskWaitGroup  sync.WaitGroup
}

const VASC_TASK_SCOPE_NATIVE = 1
const VASC_TASK_SCOPE_HOST   = 2
const VASC_TASK_SCOPE_GLOBAL = 3

func (this *VascTask) LoadConfig(config *taskConfig, projectName string) error {
    this.ProjectName = projectName
    
    if GetVascInstance().BitCode & VASC_REDIS!=0 && config.GlobalQueueRedis!=""{
        redis := GetVascInstance().Redis.Get(config.GlobalQueueRedis)
        if redis==nil {
            return errors.New("cannot get redis instance for global task")
        }
        this.RedisConn = redis
    }
    if GetVascInstance().BitCode & VASC_DB!=0 && config.LoadTaskDB!="" {
        dbEngine, err := GetVascInstance().DB.GetEngine(config.LoadTaskDB)
        if dbEngine!=nil && err!=nil {
            return err
        }
        this.DBConn  = dbEngine
    }
    this.RedisPrefix = fmt.Sprintf("VASC:%s:TASK:", projectName)
    this.TaskList = make(map[string]*TaskInfo)
    
    return nil
}

func (this * VascTask) Close() {
    this.Runnable = false
    this.taskWaitGroup.Wait()
}

func (this * VascTask) taskHandler(taskInfo *TaskInfo) {
    if taskInfo.Scope==VASC_TASK_SCOPE_NATIVE {
        for ;this.Runnable; {
            select {
                case task := <- taskInfo.TaskQueue:
                    if task!=nil {
                        taskInfo.Handler(task)
                    }
                default:
                    time.Sleep(time.Millisecond * 100)
            }
        }
        close(taskInfo.TaskQueue)
    } else if taskInfo.Scope==VASC_TASK_SCOPE_GLOBAL {
        for ;this.Runnable; {
            queueName := taskInfo.Key
            content, err := this.getTaskFromRedis(queueName, 1)
            if content!=nil && err==nil {
                taskInfo.Handler(content)
            } else if err!=nil {
                ErrorLog("cannot get task from redis: %v", err)
            }
        }
    }
    this.taskWaitGroup.Done()
}

func (this * VascTask) StartTaskHandling(taskInfo *TaskInfo) {
    var i int64 = 0
    for i = 0; i < taskInfo.HandlerNum; i++ {
        this.taskWaitGroup.Add(1)
        go this.taskHandler(taskInfo)
    }
}

func (this * VascTask) launchTask(taskList []TaskInfo) error {
    for _, info := range taskList {
        if this.TaskList[info.Key]!=nil {
            continue
        } else {
            value := new(TaskInfo)
            *value = info
            if value.Scope==VASC_TASK_SCOPE_NATIVE {
                value.TaskQueue = make(chan interface{}, value.QueueSize)
            } else if value.Scope==VASC_TASK_SCOPE_GLOBAL {
                if this.RedisConn==nil {
                    continue
                }
            } else {
                return errors.New("task type does not supported")
            }
            this.TaskList[info.Key] = value
            this.StartTaskHandling(value)
        }
    }
    
    return nil
}

func (this * VascTask) LoadTask(app *VascApplication) error {
    if app==nil {
        return nil
    }
    
    this.FuncMap  = app.FuncMap
    this.Runnable = false
    this.taskWaitGroup.Wait()    
    this.Runnable = true
    
    if app.TaskList!=nil {
        err := this.launchTask(app.TaskList)
        if err!=nil {
            return err
        }
    }
    
    if this.DBConn!=nil {
        dbTaskList, err := this.LoadTaskFromDB()
        if err!=nil {
            return err
        }
        err = this.launchTask(dbTaskList)
        if err!=nil {
            return err
        }
    }
    
    return nil
}

func (this * VascTask) LoadTaskFromDB() ([]TaskInfo, error) {
    if this.DBConn==nil {
        return nil, errors.New("cannot load task from database")
    }
    this.DBConn.Sync2(new(VascTaskDB))
    
    result := make([]VascTaskDB, 0)
    err := this.DBConn.Find(&result)
    if err!=nil {
        return nil, err
    }
    
    taskInfo := make([]TaskInfo, len(result), len(result))
    for index, value := range result {
        taskInfo[index].Key         = value.TaskKey       
        taskInfo[index].Handler     = this.FuncMap[value.TaskFuncName]
        taskInfo[index].Scope       = value.TaskScope     
        taskInfo[index].QueueSize   = value.TaskQueueSize
        taskInfo[index].HandlerNum  = value.TaskHandlerNum
    }
    
    return taskInfo, nil
}

func (this *VascTask) PushNativeTask(key string, content interface{}) error {
    info := this.TaskList[key]
    if info==nil {
        return errors.New("invalid task")
    } 
    
    for ;this.Runnable; {
        select {
            case info.TaskQueue <- content:
                return nil
            default:
                time.Sleep(time.Millisecond * 100)
        }
    }
    
    return nil
}

func (this *VascTask) getTaskFromRedis(key string, timeout int64) (interface{}, error) {
    if this.RedisConn==nil {
        return nil, errors.New("cannot find redis configuration for getting task")
    }
    aKey := this.RedisPrefix + key
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    ret, err := redisConn.Do("BLPOP", aKey, timeout)
    if err!=nil {
        fmt.Println(err)
        return nil, err
    }
    
    if ret!=nil {
        kv := ret.([]interface{})
        if(len(kv)!=2) || string(kv[0].([]byte))!=aKey {
            return nil, errors.New("invalid task queue")
        }
        result := string(kv[1].([]byte))
        return &result, nil
    }
    
    return nil, err
}

func (this *VascTask) PushGlobalTask(key string, content *string) error {
    if this.RedisConn==nil {
        return errors.New("cannot find redis configuration for pushing task")
    }
    aKey := this.RedisPrefix + key
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    _, err := redisConn.Do("RPUSH", aKey, *content)
    if err!=nil {
        fmt.Println(err)
        return err
    }
    
    return nil
}

func (this *VascTask) GetGlobalTaskNum(key string) (int, error) {
    if this.RedisConn==nil {
        return 0, errors.New("cannot find redis configuration for getting task num")
    }
    aKey := this.RedisPrefix + key
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return 0, errors.New("cannot get redis connection from pool")
    }
    
    defer redisConn.Close()
    
    len, err := redis.Int(redisConn.Do("LLEN", aKey))
    if err!=nil {
        fmt.Println(err)
        return len, err
    }
    
    return len, err
}