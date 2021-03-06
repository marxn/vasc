/*
 * A schedule manager which used to sync services running in cluster
 * It is a simple alternative scheme for CRON, which runs only on single machine
 * Author: Kevin Wang
 */
 
package scheduler

import "fmt"
import "time"
import "errors"
import "sync"
import "context"
import "math/rand"
import "encoding/json"
import "xorm.io/xorm"
import "github.com/garyburd/redigo/redis"
import "github.com/marxn/vasc/global" 
import vredis "github.com/marxn/vasc/redis" 
import "github.com/marxn/vasc/database" 
import "github.com/marxn/vasc/portal"
import "github.com/marxn/vasc/logger"

const VascScheduleFixed      = 1
const VascScheduleOverlapped = 2
const VascScheduleSerial     = 3

const VascScheduleScopeNative = 1
const VascScheduleScopeGlobal = 3

type VascScheduler struct {
    ProjectName        string
    Application       *global.VascApplication
    RedisConn         *redis.Pool
    RedisPrefix        string
    DBConnStr          string
    DBConn            *xorm.Engine
    runnable           bool
    needReload         bool
    CycleScheduleList  map[string]*global.ScheduleInfo
    ScheduleWaitGroup  sync.WaitGroup
    
    ScheduleList     []global.ScheduleInfo
    App               *global.VascApplication
    EnableLogger       bool
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

func (this *VascScheduler) LoadConfig(config *global.ScheduleConfig, redisPoolList *vredis.VascRedis, dbList *database.VascDataBase, projectName string) error {
    this.ProjectName  = projectName
    this.EnableLogger = config.EnableLogger
    
    if redisPoolList!=nil && config.GlobalLockRedis!=""{
        redisInstance := redisPoolList.Get(config.GlobalLockRedis)
        if redisInstance == nil {
            return errors.New("cannot get redis instance for global lock")
        }
        this.RedisConn = redisInstance
    }
    if dbList!=nil && config.LoadScheduleDB!="" {
        dbEngine, err := dbList.GetEngine(config.LoadScheduleDB)
        if dbEngine!=nil && err!=nil {
            return err
        }
        this.DBConn  = dbEngine
    }
    this.RedisPrefix = fmt.Sprintf("VASC:%s:SCHEDULE:", projectName)
    this.runnable    = true
    return nil
}

func (this *VascScheduler) Close() {
    this.runnable = false
    this.ScheduleWaitGroup.Wait()
}

func (this *VascScheduler) smartSleep(sleepTime int64) bool {
    if sleepTime < 0 {
        return true
    } else if sleepTime==1 {
        time.Sleep(time.Second)
        return true
    }
    
    targetTime := time.Now().Unix() + sleepTime
    for time.Now().Unix() < targetTime {
        
        if this.runnable==false || this.needReload{
            return false
        }
        time.Sleep(time.Second)
    }
    return true
}

func (this * VascScheduler) scheduleCycle() error {
	driver := time.NewTicker(time.Second)
	for ;this.runnable && !this.needReload; {
		select {
    		case <-driver.C:
                _ = this.traverseCycleScheduleList()
		}
	}
	
	this.ScheduleWaitGroup.Done()
	return nil
}

func (this *VascScheduler) traverseCycleScheduleList () error {
    now := time.Now().Unix()
    for _, scheduleItem := range this.CycleScheduleList {
        if this.runnable==false || this.needReload {
            break
        }
        if scheduleItem.Scope== VascScheduleScopeNative {
            this.ScheduleWaitGroup.Add(1)
        	go func (key string, scheduleFunc func() error) {
        	    info := this.CycleScheduleList[key]
        	    if info.LastRunTime + info.Interval <= now {
                    _ = scheduleFunc()
                    info.LastRunTime = now
                }
                this.ScheduleWaitGroup.Done()
        	}(scheduleItem.Key, scheduleItem.Routine)
        } else if scheduleItem.Scope== VascScheduleScopeGlobal {
            this.ScheduleWaitGroup.Add(1)
            go func (key string, scheduleFunc func()error, interval int64) {
                lockValue, _ := this.GetGlobalToken(key, interval)
                if lockValue!="" {
                    info, _ := this.GetGlobalScheduleStatus(key)
                    if info==nil || info.LastRunTime + info.Interval <= now {
                        _ = scheduleFunc()
                        if info==nil {
                            info = this.CycleScheduleList[key]
                        }
                        info.LastRunTime = now
                        _ = this.SetGlobalScheduleStatus(key, info, interval)
                    }
                    _ = this.ReleaseToken(key, lockValue)
                } else {
                    logger.LogSelector("_schedule").InfoLog("%s has been locked\n", key)
                }
                this.ScheduleWaitGroup.Done()
            }(scheduleItem.Key, scheduleItem.Routine, scheduleItem.Interval)
        }
    }
    
    return nil
}

func (this * VascScheduler) Start() error {
    _ = this.loadSchedule(this.ScheduleList)
    go func() {
        for ;this.runnable; {
            this.ScheduleWaitGroup.Wait()
            if !this.runnable {
                break
            }
            if this.needReload {
                this.needReload = false
                _ = this.loadSchedule(this.ScheduleList)
            }
            time.Sleep(time.Millisecond * 100)
        }
    }()
    
    return nil
}

func (this * VascScheduler) LoadSchedule(scheduleList []global.ScheduleInfo, app *global.VascApplication) error {
    if app==nil {
        return nil
    }
    this.ScheduleList = scheduleList
    this.Application  = app
    this.needReload   = false
    
    return nil
}

func (this * VascScheduler) WrapHandler(handler interface{}, scheduleInfo *global.ScheduleInfo) {
    switch handler.(type) {
        case func(*portal.Portal)error:
            scheduleInfo.Routine = portal.MakeSchedulePortalWithContext(this.ProjectName, this.EnableLogger, scheduleInfo.HandlerName, handler.(func(*portal.Portal)error), context.Background())
        default:
            scheduleInfo.Routine = portal.MakeSchedulePortalWithContext(this.ProjectName, this.EnableLogger, scheduleInfo.HandlerName, InvalidScheduleHandler, context.Background())
    }
}

func (this *VascScheduler) loadSchedule(scheduleList []global.ScheduleInfo) error {
    this.ScheduleWaitGroup.Add(1)
    this.CycleScheduleList  = make(map[string]*global.ScheduleInfo)
    for _, info := range scheduleList {
        if info.Scope== VascScheduleScopeGlobal && this.RedisConn==nil {
            continue
        }
        if info.Routine==nil {
            handler := this.Application.FuncMap[info.HandlerName]
            if handler!=nil {
                // Use a wrapper for handling logger and context.
                this.WrapHandler(handler, &info)
            }
        }
        _ = this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
    }
    
    if this.DBConn!=nil {
        dbScheduleList, err := this.LoadScheduleFromDB()
        if err!=nil {
            return err
        }
        for _, info := range dbScheduleList {
            if info.Scope== VascScheduleScopeGlobal && this.RedisConn==nil {
                continue
            }
            if info.Routine==nil {
                handler := this.Application.FuncMap[info.HandlerName]
                if handler!=nil {
                    // Use a wrapper for handling logger and context.
                    this.WrapHandler(handler, &info)
                }
            }
            _ = this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
        }
    }
    
    this.ScheduleWaitGroup.Add(1)
    go this.scheduleCycle()
    this.ScheduleWaitGroup.Done()
    return nil
}

func (this *VascScheduler) LoadScheduleFromDB() ([]global.ScheduleInfo, error) {
    if this.DBConn==nil {
        return nil, errors.New("cannot load task from database")
    }
    result := make([]VascSchedulerDB, 0)
    err := this.DBConn.Find(&result)
    if err!=nil {
        return nil, err
    }
    
    scheduleInfo := make([]global.ScheduleInfo, len(result), len(result))
    for index, value := range result {
        scheduleInfo[index].Key         = value.ScheduleKey
        handler := this.Application.FuncMap[value.ScheduleFuncName]
        if handler!=nil {
            scheduleInfo[index].Routine = func()error(handler.(func()error))
        }
        scheduleInfo[index].Type        = value.ScheduleType      
        scheduleInfo[index].Timestamp   = value.ScheduleTimestamp 
        scheduleInfo[index].Interval    = value.ScheduleInterval  
        scheduleInfo[index].Scope       = value.ScheduleScope     
    }
    
    return scheduleInfo, nil
}

func (this *VascScheduler) Bootstrap() {
    _ = this.DBConn.Sync2(new(VascSchedulerDB))
}

func (this *VascScheduler) StartSerialSchedule(scheduleKey string, schedule func()error, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    this.ScheduleWaitGroup.Add(1)
    go func(key string, interval int64) {
        for ;this.runnable && !this.needReload; {
            if scope== VascScheduleScopeNative {
                _ = schedule()
                this.smartSleep(interval)
            } else if scope== VascScheduleScopeGlobal {
                lockValue, _ := this.GetGlobalToken(key, interval)
                if lockValue!="" {
                    _ = schedule()
                    this.smartSleep(interval)
                    _ = this.ReleaseToken(key, lockValue)
                } else {
                    logger.LogSelector("_schedule").InfoLog("%s has been locked\n", key)
                    this.smartSleep(interval)
                }
            }
        }
        this.ScheduleWaitGroup.Done()
    }(scheduleKey, interval)
    
    return nil
}

func (this *VascScheduler) StartOverlappedSchedule(scheduleKey string, schedule func()error, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if this.CycleScheduleList[scheduleKey]!=nil {
        return errors.New("Duplicated key")
    }
    this.CycleScheduleList[scheduleKey] = &global.ScheduleInfo {
        Key     : scheduleKey,
        Routine : schedule,
        Type    : scheduleType,
        Interval: interval,
        Timestamp: timestamp,
        Scope   : scope,
        LastRunTime : 0,
    }
    return nil
}

func (this *VascScheduler) StartFixedSchedule(scheduleKey string, schedule func()error, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if interval==0 && time.Now().Unix() > timestamp {
        return errors.New("invalid schedule: timestamp expired with zero-interval")
    }
    this.ScheduleWaitGroup.Add(1)
    go func() {
        timeline := timestamp
        for ; this.runnable && !this.needReload; {
            now := time.Now().Unix()
            over := int64(0)
            if interval!=0 {
                over = (now - timeline) % interval
            }
            logger.LogSelector("_schedule").InfoLog("now=%d, timeline=%d, over=%d, next=%d\n", now, timeline, over, interval - over)
            if scope== VascScheduleScopeNative {
                if now >= timeline {
                    if over==0 {
                        _ = schedule()
                        if interval==0 {
                            break
                        }
                        if this.smartSleep(interval)==false {
                            break
                        }
                        timeline = now + interval
                    } else {
                        if this.smartSleep(interval - over)==false {
                            break
                        }
                        timeline = now + interval - over
                    }
                } else {
                    if this.smartSleep(timeline - now)==false {
                        break
                    }
                }
            } else if scope== VascScheduleScopeGlobal {
                if  now >= timeline {
                    if over==0 {
                        lockValue, _ := this.GetGlobalToken(scheduleKey, interval)
                        if lockValue!="" {
                            _ = schedule()
                            if this.smartSleep(interval)==false {
                                break
                            }
                            if interval==0 {
                                break
                            }
                            _ = this.ReleaseToken(scheduleKey, lockValue)
                        } else {
                            logger.LogSelector("_schedule").InfoLog("%s has been locked:%d\n", scheduleKey, now)
                            if interval==0 || this.smartSleep(interval)==false {
                                break
                            }
                        }
                        timeline = now + interval
                    } else {
                        if this.smartSleep(interval - over)==false {
                            break
                        }
                        timeline = now + interval - over
                    }
                } else {
                    if this.smartSleep(timeline - now)==false {
                        break
                    }
                }
            } else {
                break
            }
        } 
        this.ScheduleWaitGroup.Done()
    }()
    return nil
}

func (this *VascScheduler) setSchedule(scheduleKey string, schedule func()error, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if schedule==nil {
        return errors.New("invalid schedule handler")
    }
    switch scheduleType {
        case VascScheduleOverlapped:
            return this.StartOverlappedSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        case VascScheduleSerial:
            return this.StartSerialSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        case VascScheduleFixed:
            return this.StartFixedSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        default:
            return errors.New("Invalid schedule type")
    }
    return nil
}

func (this *VascScheduler) GetGlobalToken(key string, life int64) (string, error) {
    if this.RedisConn==nil {
        return "", errors.New("cannot find redis configuration for getting global token")
    }
    
    lockValue := fmt.Sprintf("%d:%d", time.Now().UnixNano(), rand.Int())
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return "", errors.New("cannot get redis connection from pool")
    }
    
    defer redisConn.Close()
    
    //In case of run-once schedule
    if life==0 {
        life = 10
    }
    _, err := redis.String(redisConn.Do("SET", this.RedisPrefix + "token:" + key, lockValue, "EX", life, "NX")) 
    
	if err==redis.ErrNil {
	    logger.LogSelector("_schedule").InfoLog("lockfailed:[%s][%v]\n", key, err)
		return "", err
	}
	
	if err!=nil {
	    logger.LogSelector("_schedule").InfoLog("lockerror:[%s][%v]\n", key, err)
		return "", err
	}
	
	logger.LogSelector("_schedule").InfoLog("locksuccess:[%s][%s]\n", key, lockValue)
	return lockValue, nil
}

func (this *VascScheduler) ReleaseToken(key string, lockValue string) error {
    if this.RedisConn==nil {
        return errors.New("cannot find redis configuration for releasing global token")
    }
    
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return nil
    }
    
    defer redisConn.Close()
    
    releaseLockScript := redis.NewScript(1, `
        if redis.call("get", KEYS[1]) == ARGV[1]
        then 
            return redis.call("del", KEYS[1])
        else
            return 0
        end
    `)

    _, _ = releaseLockScript.Do(redisConn, this.RedisPrefix+"token:"+key, lockValue)
    return nil
}

func (this *VascScheduler) SetGlobalScheduleStatus(key string, info *global.ScheduleInfo, life int64) error {
    if this.RedisConn==nil {
        return errors.New("cannot find redis configuration for setting global schedule status")
    }
    
    scheduleInfo, err := json.Marshal(*info)
    if err!=nil {
        return err
    }
    
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return errors.New("cannot get redis connection from pool")
    }
    
    defer redisConn.Close()
    
    aKey := this.RedisPrefix + "info:" + key
    _, err = redisConn.Do("SET",  aKey, string(scheduleInfo), "EX", life)
    return err
}

func (this *VascScheduler) GetGlobalScheduleStatus(key string) (*global.ScheduleInfo, error) {
    if this.RedisConn==nil {
        return nil, errors.New("cannot find redis configuration for setting global schedule status")
    }
    
    aKey := this.RedisPrefix + "info:" + key   
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return nil, errors.New("cannot get redis connection for setting global schedule status")
    }
    
    defer redisConn.Close()
    
    scheduleInfo, err := redis.String(redisConn.Do("GET", aKey))
    if err!=nil {
        return nil, err
    }
    
    var jsonResult global.ScheduleInfo
    err = json.Unmarshal([]byte(scheduleInfo), &jsonResult)
    if err != nil {
        return nil, err
    }
    
    return &jsonResult, nil
}

func (this *VascScheduler) CreateNewPersistentSchedule(schedule *VascSchedulerDB) error {
    _, err := this.DBConn.Insert(schedule)
    return err
}

func (this *VascScheduler) ReloadSchedule() error {
    this.needReload = true
    return nil
}

func InvalidScheduleHandler(p * portal.Portal) error {
    return errors.New("invalid schedule prototype")
}