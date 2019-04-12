package vasc

import "fmt"
import "errors"
import "sync"
import "time"
import "github.com/garyburd/redigo/redis"

type taskConfig struct {
    TaskRedisHost    string         `json:"task_redis_host"`
    TaskRedisPasswd  string         `json:"task_redis_passwd"`
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
    RedisConn         *VascRedis
    RedisPrefix        string
    Runnable           bool
    TaskList           map[string]*TaskInfo
    taskWaitGroup  sync.WaitGroup
}

const VASC_TASK_SCOPE_NATIVE = 1
const VASC_TASK_SCOPE_HOST   = 2
const VASC_TASK_SCOPE_GLOBAL = 3

func (this *VascTask) LoadConfig(config *taskConfig, projectName string) error {
    this.ProjectName = projectName
    this.RedisHost   = config.TaskRedisHost
    this.RedisPasswd = config.TaskRedisPasswd
    this.RedisPrefix = fmt.Sprintf("%s:TASK:", projectName)
    
    return this.InitTask()
}

func (this * VascTask) InitTask() error {   
    this.RedisConn = new(VascRedis)
    this.RedisConn.LoadConfig(&redisConfig{RedisHost: this.RedisHost, RedisPasswd: this.RedisPasswd}, this.ProjectName)
    this.TaskList = make(map[string]*TaskInfo)
    this.Runnable = true
    return nil
}

func (this * VascTask) Close() {
    this.Runnable = false
    this.RedisConn.Close()
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

func (this * VascTask) LoadTask(taskList []TaskInfo) error {
    if taskList==nil {
        return nil
    }
    for _, info := range taskList {
        if this.TaskList[info.Key]!=nil {
            continue
        } else {
            value := new(TaskInfo)
            *value = info
            if value.Scope==VASC_TASK_SCOPE_NATIVE {
                value.TaskQueue = make(chan interface{}, value.QueueSize)
            }
            this.TaskList[info.Key] = value
            this.StartTaskHandling(value)
        }
        
    }   
    return nil
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
    aKey := this.RedisPrefix + key
    
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    len, err := redis.Int(redisConn.Do("LLEN", aKey))
    if err!=nil {
        fmt.Println(err)
        return len, err
    }
    
    return len, err
}