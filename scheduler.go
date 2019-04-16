/*
 * A schedule manager which used to sync services running in cluster
 * It is a simple alternative scheme for CRON, which runs only on single machine
 * Author: Kevin Wang
 */
 
package vasc

import "fmt"
import "time"
import "errors"
import "sync"
import "math/rand"
import "encoding/json"
import "github.com/go-xorm/xorm"
import "github.com/garyburd/redigo/redis"

type scheduleConfig struct {
    Enable                bool           `json:"enable"`
    LoadScheduleDB        string         `json:"load_from_database"`
    GlobalLockRedis       string         `json:"global_lock_redis"`
}

type ScheduleInfo struct {
    Key         string                   `json:"key"`  
    Routine     VascRoutine              `json:"-"`
    Type        uint64                   `json:"type"`
    Timestamp   int64                    `json:"timestamp"`  
    Interval    int64                    `json:"interval"`
    Scope       int64                    `json:"scope"`
    LastRunTime int64                    `json:"last_run_time"`
}

type VascScheduler struct {
    ProjectName        string
    RedisConn         *redis.Pool
    RedisPrefix        string
    DBConnStr          string
    DBConn            *xorm.Engine
    Runnable           bool
    FuncMap            map[string]VascRoutine
    CycleScheduleList  map[string]*ScheduleInfo
    ScheduleWaitGroup  sync.WaitGroup
}

type VascSchedulerDB struct {
    ScheduleID        int64     `xorm:"BIGINT PK AUTOINCR 'SCHEDULE_ID'"`  
    ScheduleKey       string    `xorm:"VARCHAR(128) NOT NULL INDEX(INDEX1) 'SCHEDULE_KEY'"`
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
const VASC_SCHEDULE_MESSAGEDRV = 4

const VASC_SCHEDULE_SCOPE_NATIVE = 1
const VASC_SCHEDULE_SCOPE_HOST   = 2
const VASC_SCHEDULE_SCOPE_GLOBAL = 3

func (this *VascScheduler) LoadConfig(config *scheduleConfig, projectName string) error {
    this.ProjectName = projectName

    if GetVascInstance().BitCode & VASC_REDIS!=0 && config.GlobalLockRedis!=""{
        redis := GetVascInstance().Redis.Get(config.GlobalLockRedis)
        if redis==nil {
            return errors.New("cannot get redis instance for global lock")
        }
        this.RedisConn = redis
    }
    if GetVascInstance().BitCode & VASC_DB!=0 && config.LoadScheduleDB!="" {
        dbEngine, err := GetVascInstance().DB.GetEngine(config.LoadScheduleDB)
        if dbEngine!=nil && err!=nil {
            return err
        }
        this.DBConn  = dbEngine
    }
    this.RedisPrefix = fmt.Sprintf("VASC:%s:SCHEDULE:", projectName)
    
    return nil
}

func (this * VascScheduler) Close() {
    this.Runnable = false
    this.ScheduleWaitGroup.Wait()
}

func (this * VascScheduler) smartSleep(sleepTime int64) bool {
    if sleepTime < 0 {
        return true
    } else if sleepTime==1 {
        time.Sleep(time.Second)
        return true
    }
    
    targetTime := time.Now().Unix() + sleepTime
    for time.Now().Unix() < targetTime {
        
        if this.Runnable==false {
            return false
        }
        time.Sleep(time.Second)
    }
    return true
}

func (this * VascScheduler) scheduleCycle() error {
	driver := time.NewTicker(time.Second)

	for ;this.Runnable; {
		select {
    		case <-driver.C:
    		    this.traverseCycleScheduleList()
		}
	}
	
	this.ScheduleWaitGroup.Done()
	return nil
}

func (this *VascScheduler) traverseCycleScheduleList () error {
    now := time.Now().Unix()
    for _, scheduleItem := range this.CycleScheduleList {
        if this.Runnable==false {
            break
        }
        if scheduleItem.Scope==VASC_SCHEDULE_SCOPE_NATIVE {
            this.ScheduleWaitGroup.Add(1)
        	go func (key string, scheduleFunc VascRoutine) {
        	    info := this.CycleScheduleList[key]
        	    if info.LastRunTime + info.Interval <= now {
                    scheduleFunc(key)
                    info.LastRunTime = now
                }
                this.ScheduleWaitGroup.Done()
        	}(scheduleItem.Key, scheduleItem.Routine)
        } else if scheduleItem.Scope==VASC_SCHEDULE_SCOPE_GLOBAL {
            this.ScheduleWaitGroup.Add(1)
            go func (key string, scheduleFunc VascRoutine, interval int64) {
                lockValue, _ := this.GetGlobalToken(key, interval)
                if lockValue!="" {
                    info, _ := this.GetGlobalScheduleStatus(key)
                    if info==nil || info.LastRunTime + info.Interval <= now {
                        scheduleFunc(key)
                        if info==nil {
                            info = this.CycleScheduleList[key]
                        }
                        info.LastRunTime = now
                        this.SetGlobalScheduleStatus(key, info, interval)
                    }
                    this.ReleaseToken(key, lockValue)
                } else {
                    fmt.Sprintf("%s has been locked\n", key)
                }
                this.ScheduleWaitGroup.Done()
            }(scheduleItem.Key, scheduleItem.Routine, scheduleItem.Interval)
        }
    }
    
    return nil
}

func (this *VascScheduler) LoadSchedule(app *VascApplication) error {
    if app==nil {
        return nil
    }
    this.FuncMap  = app.FuncMap
    this.Runnable = false
    this.ScheduleWaitGroup.Wait()    
    this.Runnable = true
    
    this.CycleScheduleList  = make(map[string]*ScheduleInfo)
    for _, info := range app.ScheduleList {
        if info.Scope==VASC_SCHEDULE_SCOPE_GLOBAL && this.RedisConn==nil {
            continue
        }
        this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
    }
    
    if this.DBConn!=nil {
        dbScheduleList, err := this.LoadScheduleFromDB()
        if err!=nil {
            return err
        }
        for _, info := range dbScheduleList {
            if info.Scope==VASC_SCHEDULE_SCOPE_GLOBAL && this.RedisConn==nil {
                continue
            }
            this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
        }
    }
    
    this.ScheduleWaitGroup.Add(1)
    go this.scheduleCycle()
    return nil
}

func (this *VascScheduler) LoadScheduleFromDB() ([]ScheduleInfo, error) {
    this.DBConn.Sync2(new(VascSchedulerDB))
    
    result := make([]VascSchedulerDB, 0)
    err := this.DBConn.Find(&result)
    if err!=nil {
        return nil, err
    }
    
    scheduleInfo := make([]ScheduleInfo, len(result), len(result))
    for index, value := range result {
        scheduleInfo[index].Key         = value.ScheduleKey       
        scheduleInfo[index].Routine     = this.FuncMap[value.ScheduleFuncName]
        scheduleInfo[index].Type        = value.ScheduleType      
        scheduleInfo[index].Timestamp   = value.ScheduleTimestamp 
        scheduleInfo[index].Interval    = value.ScheduleInterval  
        scheduleInfo[index].Scope       = value.ScheduleScope     
    }
    
    return scheduleInfo, nil
}

func (this *VascScheduler) StartSerialSchedule(scheduleKey string, schedule VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    this.ScheduleWaitGroup.Add(1)
    go func(key string, interval int64) {
        for ;this.Runnable; {
            if scope==VASC_SCHEDULE_SCOPE_NATIVE {
                schedule(key)
                this.smartSleep(interval)
            } else if scope==VASC_SCHEDULE_SCOPE_GLOBAL {
                lockValue, _ := this.GetGlobalToken(key, interval)
                if lockValue!="" {
                    schedule(key)
                    this.smartSleep(interval)
                    this.ReleaseToken(key, lockValue)
                } else {
                    fmt.Printf("%s has been locked\n", key)
                    this.smartSleep(interval)
                }
            }
        }
        this.ScheduleWaitGroup.Done()
    }(scheduleKey, interval)
    
    return nil
}

func (this *VascScheduler) StartOverlappedSchedule(scheduleKey string, schedule VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if this.CycleScheduleList[scheduleKey]!=nil {
        return errors.New("Duplicated key")
    }
    this.CycleScheduleList[scheduleKey] = &ScheduleInfo {
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

func (this *VascScheduler) StartFixedSchedule(scheduleKey string, schedule VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if interval==0 && time.Now().Unix() > timestamp {
        return errors.New("invalid schedule: timestamp expired with zero-interval")
    }
    this.ScheduleWaitGroup.Add(1)
    go func() {
        timeline := timestamp
        for ; this.Runnable; {
            now := time.Now().Unix()
            over := int64(0)
            if interval!=0 {
                over = (now - timeline) % interval
            }
            fmt.Printf("now=%d, timeline=%d, over=%d, next=%d\n", now, timeline, over, interval - over)
            if scope==VASC_SCHEDULE_SCOPE_NATIVE {
                if now >= timeline {
                    if over==0 {
                        schedule(scheduleKey)
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
            } else if scope==VASC_SCHEDULE_SCOPE_GLOBAL {
                if  now >= timeline {
                    if over==0 {
                        lockValue, _ := this.GetGlobalToken(scheduleKey, interval)
                        if lockValue!="" {
                            schedule(scheduleKey)
                            if this.smartSleep(interval)==false {
                                break
                            }
                            if interval==0 {
                                break
                            }
                            this.ReleaseToken(scheduleKey, lockValue)
                        } else {
                            fmt.Printf("%s has been locked:%d\n", scheduleKey, now)
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

func (this *VascScheduler) setSchedule(scheduleKey string, schedule VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    switch scheduleType {
        case VASC_SCHEDULE_OVERLAPPED:
            return this.StartOverlappedSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        case VASC_SCHEDULE_SERIAL:
            return this.StartSerialSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        case VASC_SCHEDULE_FIXED:
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
	    //fmt.Sprintf("lockfailed:[%s][%v]\n", key, err)
		return "", err
	}
	
	if err!=nil {
	    //fmt.Printf("lockerror:[%s][%v]\n", key, err)
		return "", err
	}
	
	//fmt.Printf("locksuccess:[%s][%s]\n", key, lockValue)
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
    
    releaseLockScript.Do(redisConn, this.RedisPrefix + "token:" + key, lockValue)
    return nil
}

func (this *VascScheduler) SetGlobalScheduleStatus(key string, info *ScheduleInfo, life int64) error {
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

func (this *VascScheduler) GetGlobalScheduleStatus(key string) (*ScheduleInfo, error) {
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
    
    var jsonResult ScheduleInfo
    err = json.Unmarshal([]byte(scheduleInfo), &jsonResult)
    if err != nil {
        return nil, err
    }
    
    return &jsonResult, nil
}