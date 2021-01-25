package task

import "fmt"
import "errors"
import "sync"
import "time"
import "context"
import "encoding/json"
import "github.com/go-xorm/xorm"
import "github.com/garyburd/redigo/redis"
import "github.com/marxn/vasc/global" 
import vredis "github.com/marxn/vasc/redis" 
import "github.com/marxn/vasc/database" 
import "github.com/marxn/vasc/portal" 

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
    
    GivenTaskList    []global.TaskInfo
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
    this.RedisPrefix = fmt.Sprintf("VASCTASK:")
    this.TaskList = make(map[string]*global.TaskInfo)
    this.runnable    = true
    this.needReload  = false
    
    return nil
}

func (this * VascTask) Close() {
    this.runnable = false
    this.taskWaitGroup.Wait()
}

func (this * VascTask) WrapHandler(taskInfo *global.TaskInfo, taskContent *portal.TaskContent) func()error {
    switch taskInfo.Handler.(type) {
        case func(*portal.Portal)error:
            return portal.MakeTaskHandlerWithContext(this.ProjectName, taskInfo.Key, taskInfo.Handler.(func(*portal.Portal)error), taskContent, context.Background())
        default:
            return portal.MakeTaskHandlerWithContext(this.ProjectName, taskInfo.Key, InvalidTaskHandler, taskContent, context.Background())
    }
}

func (this * VascTask) taskHandler(taskInfo *global.TaskInfo) {
    if taskInfo.Scope==VASC_TASK_SCOPE_NATIVE {
        for ;this.runnable && !this.needReload; {
            select {
                case task := <- taskInfo.TaskQueue:
                    if task!=nil {
                        handler := this.WrapHandler(taskInfo, task.(*portal.TaskContent))
                        if handler != nil {
                            handler()
                        }
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
                handler := this.WrapHandler(taskInfo, content)
                if handler != nil {
                    handler()
                }
            } else if err!=nil {
                time.Sleep(time.Millisecond * 100)
                //fmt.Printf("cannot get task [%s] from redis: %v\n", taskInfo.Key, err)
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
    // Install task handler
    for _, info := range taskList {
        if info.Handler==nil {
            handler := this.Application.FuncMap[info.HandlerName]
            if handler!=nil {
                info.Handler = handler.(func (*portal.Portal) error)
            }
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

func (this *VascTask) Start() error {
    this.loadTask(this.GivenTaskList)
    
    go func() {
        for ;this.runnable; {
            this.taskWaitGroup.Wait()
            if !this.runnable {
                break
            }
            if this.needReload {
                this.needReload = false
                this.loadTask(this.GivenTaskList)
            }
            time.Sleep(time.Millisecond * 100)
        }
    }()
    
    return nil
}

func (this * VascTask) LoadTask(taskList []global.TaskInfo, app *global.VascApplication) error {
    if app==nil {
        return nil
    }
    this.Application   = app
    this.GivenTaskList = taskList
    
    return nil
}

func (this * VascTask) loadTask(taskList []global.TaskInfo) error {
    this.taskWaitGroup.Add(1)
    if taskList!=nil {
        err := this.launchTask(taskList)
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
    
    result := make([]VascTaskDB, 0)
    err := this.DBConn.Find(&result)
    if err!=nil {
        return nil, err
    }
    
    taskInfo := make([]global.TaskInfo, len(result), len(result))
    for index, value := range result {
        taskInfo[index].Key         = value.TaskKey      
        handler := this.Application.FuncMap[value.TaskFuncName]
        if handler!=nil {
            taskInfo[index].Handler = handler.(func(interface{}) error)
        } 
        taskInfo[index].Scope       = value.TaskScope     
        taskInfo[index].QueueSize   = value.TaskQueueSize
        taskInfo[index].HandlerNum  = value.TaskHandlerNum
    }
    
    return taskInfo, nil
}

func (this * VascTask) Bootstrap() {
    this.DBConn.Sync2(new(VascTaskDB))
}

func (this *VascTask) PushNativeTask(key string, content []byte) error {
    info := this.TaskList[key]
    if info==nil {
        return errors.New("invalid task")
    } 
    
    taskContent := &portal.TaskContent{
        ProjectName: this.ProjectName,
        CreateTime:  time.Now().UnixNano(),
        Content:     content,
    }
    
    for ;this.runnable && !this.needReload; {
        select {
            case info.TaskQueue <- taskContent:
                return nil
            default:
                time.Sleep(time.Millisecond * 100)
        }
    }
    
    return nil
}

func (this *VascTask) getTaskFromRedis(key string, timeout int64) (*portal.TaskContent, error) {
    if this.RedisConn==nil {
        return nil, errors.New("cannot find redis configuration for getting task")
    }
    
    aKey      := this.RedisPrefix + key
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    ret, err := redisConn.Do("BLPOP", aKey, timeout)
    if err!=nil {
        fmt.Println(err)
        return nil, err
    }
    
    if ret != nil {
        kv := ret.([]interface{})
        if len(kv) != 2 || string(kv[0].([]byte)) != aKey {
            return nil, errors.New("invalid task queue")
        }
        
        var taskContent portal.TaskContent
        if err := json.Unmarshal(kv[1].([]byte), &taskContent); err != nil {
            return nil, err
        }
        
        return &taskContent, nil
    }
    
    return nil, err
}

func (this *VascTask) PushGlobalTask(key string, content []byte) error {
    if this.RedisConn==nil {
        return errors.New("cannot find redis configuration for pushing task")
    }
    aKey := this.RedisPrefix + key
    
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    taskContent := &portal.TaskContent{
        ProjectName: this.ProjectName,
        CreateTime:  time.Now().UnixNano(),
        Content:     content,
    }
    
    taskContentBytes, err := json.Marshal(taskContent)
    if err != nil {
        fmt.Println(err)
        return err
    }
        
    _, err = redisConn.Do("RPUSH", aKey, taskContentBytes)
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

func (this *VascTask) CreateNewPersistentTask(task *VascTaskDB) error {
    _, err := this.DBConn.Insert(task)
    return err
}

func InvalidTaskHandler(p * portal.Portal) error {
    return errors.New("Invalid task prototype")
}
