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
import "math/rand"
import "encoding/json"
import "github.com/go-xorm/xorm"
import "github.com/garyburd/redigo/redis"
import "github.com/marxn/vasc/global" 
import vredis "github.com/marxn/vasc/redis" 
import "github.com/marxn/vasc/database" 

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
}

func (this *VascScheduler) LoadConfig(config *global.ScheduleConfig, redisPoolList *vredis.VascRedis, dbList *database.VascDataBase, projectName string) error {
    this.ProjectName = projectName

    if redisPoolList!=nil && config.GlobalLockRedis!=""{
        redis := redisPoolList.Get(config.GlobalLockRedis)
        if redis==nil {
            return errors.New("cannot get redis instance for global lock")
        }
        this.RedisConn = redis
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
    		    this.traverseCycleScheduleList()
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
        if scheduleItem.Scope==global.VASC_SCHEDULE_SCOPE_NATIVE {
            this.ScheduleWaitGroup.Add(1)
        	go func (key string, scheduleFunc global.VascRoutine) {
        	    info := this.CycleScheduleList[key]
        	    if info.LastRunTime + info.Interval <= now {
                    scheduleFunc(key)
                    info.LastRunTime = now
                }
                this.ScheduleWaitGroup.Done()
        	}(scheduleItem.Key, scheduleItem.Routine)
        } else if scheduleItem.Scope==global.VASC_SCHEDULE_SCOPE_GLOBAL {
            this.ScheduleWaitGroup.Add(1)
            go func (key string, scheduleFunc global.VascRoutine, interval int64) {
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

func (this * VascScheduler) LoadSchedule(app *global.VascApplication) error {
    if app==nil {
        return nil
    }
    this.Application = app
    this.needReload  = false
    this.loadSchedule()
    
    go func() {
        for ;this.runnable; {
            this.ScheduleWaitGroup.Wait()
            if !this.runnable {
                break
            }
            if this.needReload {
                this.needReload = false
                this.loadSchedule()
            }
            time.Sleep(time.Millisecond * 100)
        }
    }()

    return nil
}

func (this *VascScheduler) loadSchedule() error {
    this.ScheduleWaitGroup.Add(1)
    this.CycleScheduleList  = make(map[string]*global.ScheduleInfo)
    for _, info := range this.Application.ScheduleList {
        if info.Scope==global.VASC_SCHEDULE_SCOPE_GLOBAL && this.RedisConn==nil {
            //ErrorLog("cannot load global schedule [%s]", info.Key)
            continue
        }
        if info.Routine==nil {
            info.Routine = global.VascRoutine(this.Application.FuncMap[info.HandlerName].(global.VascRoutine))
        }
        this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
    }
    
    if this.DBConn!=nil {
        dbScheduleList, err := this.LoadScheduleFromDB()
        if err!=nil {
            return err
        }
        for _, info := range dbScheduleList {
            if info.Scope==global.VASC_SCHEDULE_SCOPE_GLOBAL && this.RedisConn==nil {
                //ErrorLog("cannot load global schedule [%s]", info.Key)
                continue
            }
            if info.Routine==nil {
                info.Routine = global.VascRoutine(this.Application.FuncMap[info.HandlerName].(global.VascRoutine))
            }
            this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
        }
    }
    
    this.ScheduleWaitGroup.Add(1)
    go this.scheduleCycle()
    this.ScheduleWaitGroup.Done()
    return nil
}

func (this *VascScheduler) LoadScheduleFromDB() ([]global.ScheduleInfo, error) {
    this.DBConn.Sync2(new(global.VascSchedulerDB))
    
    result := make([]global.VascSchedulerDB, 0)
    err := this.DBConn.Find(&result)
    if err!=nil {
        return nil, err
    }
    
    scheduleInfo := make([]global.ScheduleInfo, len(result), len(result))
    for index, value := range result {
        scheduleInfo[index].Key         = value.ScheduleKey       
        scheduleInfo[index].Routine     = global.VascRoutine(this.Application.FuncMap[value.ScheduleFuncName].(global.VascRoutine))
        scheduleInfo[index].Type        = value.ScheduleType      
        scheduleInfo[index].Timestamp   = value.ScheduleTimestamp 
        scheduleInfo[index].Interval    = value.ScheduleInterval  
        scheduleInfo[index].Scope       = value.ScheduleScope     
    }
    
    return scheduleInfo, nil
}

func (this *VascScheduler) StartSerialSchedule(scheduleKey string, schedule global.VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    this.ScheduleWaitGroup.Add(1)
    go func(key string, interval int64) {
        for ;this.runnable && !this.needReload; {
            if scope==global.VASC_SCHEDULE_SCOPE_NATIVE {
                schedule(key)
                this.smartSleep(interval)
            } else if scope==global.VASC_SCHEDULE_SCOPE_GLOBAL {
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

func (this *VascScheduler) StartOverlappedSchedule(scheduleKey string, schedule global.VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
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

func (this *VascScheduler) StartFixedSchedule(scheduleKey string, schedule global.VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
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
            fmt.Printf("now=%d, timeline=%d, over=%d, next=%d\n", now, timeline, over, interval - over)
            if scope==global.VASC_SCHEDULE_SCOPE_NATIVE {
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
            } else if scope==global.VASC_SCHEDULE_SCOPE_GLOBAL {
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

func (this *VascScheduler) setSchedule(scheduleKey string, schedule global.VascRoutine, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if schedule==nil {
        return errors.New("invalid schedule handler")
    }
    switch scheduleType {
        case global.VASC_SCHEDULE_OVERLAPPED:
            return this.StartOverlappedSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        case global.VASC_SCHEDULE_SERIAL:
            return this.StartSerialSchedule(scheduleKey, schedule, scheduleType, interval, timestamp, scope)
        case global.VASC_SCHEDULE_FIXED:
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

func (this *VascScheduler) CreateNewPersistentSchedule(schedule *global.VascSchedulerDB) error {
    _, err := this.DBConn.Insert(schedule)
    return err
}

func (this *VascScheduler) ReloadSchedule() error {
    this.needReload = true
    return nil
}