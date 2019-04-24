package task

import "fmt"
import "errors"
import "sync"
import "time"
import "github.com/go-xorm/xorm"
import "github.com/garyburd/redigo/redis"
import "github.com/marxn/vasc/global" 
import vredis "github.com/marxn/vasc/redis" 
import "github.com/marxn/vasc/database" 

const VASC_TASK_SCOPE_NATIVE = 1
const VASC_TASK_SCOPE_HOST   = 2
const VASC_TASK_SCOPE_GLOBAL = 3

type VascTask struct {
    ProjectName        string
    Application       *global.VascApplication
    RedisConn         *redis.Pool
    RedisPrefix        string
    runnable           bool
    needReload         bool
    DBConn            *xorm.Engine
    TaskList           map[string]*global.TaskInfo
    taskWaitGroup      sync.WaitGroup
}

func (this *VascTask) LoadConfig(config *global.TaskConfig, redisPoolList *vredis.VascRedis, dbList *database.VascDataBase, projectName string) error {
    this.ProjectName = projectName
    
    if redisPoolList!=nil && config.GlobalQueueRedis!=""{
        redis := redisPoolList.Get(config.GlobalQueueRedis)
        if redis==nil {
            return errors.New("cannot get redis instance for global task")
        }
        this.RedisConn = redis
    }
    if dbList!=nil && config.LoadTaskDB!="" {
        dbEngine, err := dbList.GetEngine(config.LoadTaskDB)
        if dbEngine!=nil && err!=nil {
            return err
        }
        this.DBConn  = dbEngine
    }
    this.RedisPrefix = fmt.Sprintf("VASC:%s:TASK:", projectName)
    this.TaskList = make(map[string]*global.TaskInfo)
    this.runnable    = true
    this.needReload  = false
    
    return nil
}

func (this * VascTask) Close() {
    this.runnable = false
    this.taskWaitGroup.Wait()
}

func (this * VascTask) taskHandler(taskInfo *global.TaskInfo) {
    if taskInfo.Scope==VASC_TASK_SCOPE_NATIVE {
        for ;this.runnable && !this.needReload; {
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
        for ;this.runnable && !this.needReload; {
            queueName := taskInfo.Key
            content, err := this.getTaskFromRedis(queueName, 1)
            if content!=nil && err==nil {
                taskInfo.Handler(content)
            } else if err!=nil {
                //ErrorLog("cannot get task from redis: %v", err)
            }
        }
    }
    this.taskWaitGroup.Done()
}

func (this * VascTask) StartTaskHandling(taskInfo *global.TaskInfo) {
    var i int64 = 0
    for i = 0; i < taskInfo.HandlerNum; i++ {
        this.taskWaitGroup.Add(1)
        go this.taskHandler(taskInfo)
    }
}

func (this * VascTask) launchTask(taskList []global.TaskInfo) error {
    for _, info := range taskList {
        if info.Handler==nil {
            info.Handler = global.VascRoutine(this.Application.FuncMap[info.HandlerName].(global.VascRoutine))
        }
        if this.TaskList[info.Key]!=nil || info.Handler==nil {
            continue
        } else {
            value := new(global.TaskInfo)
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

func (this * VascTask) LoadTask(app *global.VascApplication) error {
    if app==nil {
        return nil
    }
    this.Application = app
    this.loadTask()
    
    go func() {
        for ;this.runnable; {
            this.taskWaitGroup.Wait()
            if !this.runnable {
                break
            }
            if this.needReload {
                this.needReload = false
                this.loadTask()
            }
            time.Sleep(time.Millisecond * 100)
        }
    }()
    
    return nil
}

func (this * VascTask) loadTask() error {
    this.taskWaitGroup.Add(1)
    if this.Application.TaskList!=nil {
        err := this.launchTask(this.Application.TaskList)
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
    this.taskWaitGroup.Done()
    return nil
}

func (this * VascTask) LoadTaskFromDB() ([]global.TaskInfo, error) {
    if this.DBConn==nil {
        return nil, errors.New("cannot load task from database")
    }
    this.DBConn.Sync2(new(global.VascTaskDB))
    
    result := make([]global.VascTaskDB, 0)
    err := this.DBConn.Find(&result)
    if err!=nil {
        return nil, err
    }
    
    taskInfo := make([]global.TaskInfo, len(result), len(result))
    for index, value := range result {
        taskInfo[index].Key         = value.TaskKey       
        taskInfo[index].Handler     = global.VascRoutine(this.Application.FuncMap[value.TaskFuncName].(global.VascRoutine))
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
    
    for ;this.runnable && !this.needReload; {
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

func (this *VascTask) ReloadTaskList() error {
    this.needReload = true
    return nil
}

func (this *VascTask) CreateNewPersistentTask(task *global.VascTaskDB) error {
    _, err := this.DBConn.Insert(task)
    return err
}