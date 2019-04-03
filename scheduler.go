package vasc

import "fmt"
import "time"
import "errors"
import "sync"
import "math/rand"
import "io/ioutil"
import "encoding/json"
import "github.com/garyburd/redigo/redis"

type scheduleConfig struct {
    SchedulerRedisHost    string         `json:"scheduler_redis_host"`
    SchedulerRedisPasswd  string         `json:"scheduler_redis_passwd"`
    SchedulerRedisPrefix  string         `json:"scheduler_redis_prefix"`
    SchedulerDBConnStr    string         `json:"scheduler_db_connstr"`
}

type VascSchedule func (scheduleKey string) error

type ScheduleInfo struct {
    Key         string                   `json:"key"`  
    Routine     VascSchedule             `json:"-"`
    Type        uint64                   `json:"type"`
    Timestamp   int64                    `json:"timestamp"`  
    Interval    int64                    `json:"interval"`
    Scope       int64                    `json:"scope"`
    LastRunTime int64                    `json:"last_run_time"`
}

type VascScheduler struct {
    ProjectName        string
    RedisHost          string
    RedisPasswd        string
    RedisConn         *VascRedis
    RedisPrefix        string
    DBConnStr          string
    Runnable           bool
    CycleScheduleList  map[string]*ScheduleInfo
    ScheduleWaitGroup  sync.WaitGroup
}

const VASC_SCHEDULE_FIXED      = 1
const VASC_SCHEDULE_OVERLAPPED = 2
const VASC_SCHEDULE_SERIAL     = 3

const VASC_SCHEDULE_SCOPE_NATIVE = 1
const VASC_SCHEDULE_SCOPE_HOST   = 2
const VASC_SCHEDULE_SCOPE_GLOBAL = 3

func (this *VascScheduler) LoadConfig(configFile string, projectName string, profile string) error {
    this.ProjectName = projectName
    
    config, err := ioutil.ReadFile(configFile + "/" + projectName + "/scheduler.json")
    
    if err != nil{
        return errors.New("Cannot find scheduler config file for project:" + projectName)
    }
    
    var jsonResult scheduleConfig
    err = json.Unmarshal([]byte(config), &jsonResult)
    if err != nil {
        return errors.New("Cannot parse scheduler config file for project:" + projectName)
    }
    
    this.RedisHost   = jsonResult.SchedulerRedisHost
    this.RedisPasswd = jsonResult.SchedulerRedisPasswd
    this.RedisPrefix = jsonResult.SchedulerRedisPrefix
    this.DBConnStr   = jsonResult.SchedulerDBConnStr
    
    return this.InitScheduler()
}

func (this * VascScheduler) InitScheduler() error {   
    this.RedisConn = new(VascRedis)
    this.RedisConn.SetConfig(this.RedisHost, this.RedisPasswd, this.RedisPrefix)
    
    return nil
}

func (this * VascScheduler) Close() {
    this.Runnable = false
    this.ScheduleWaitGroup.Wait()
    this.RedisConn.Close()
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
        	go func (key string, scheduleFunc VascSchedule) {
        	    info := this.CycleScheduleList[key]
        	    if info.LastRunTime + info.Interval <= now {
                    scheduleFunc(key)
                    info.LastRunTime = now
                }
                this.ScheduleWaitGroup.Done()
        	}(scheduleItem.Key, scheduleItem.Routine)
        } else if scheduleItem.Scope==VASC_SCHEDULE_SCOPE_GLOBAL {
            this.ScheduleWaitGroup.Add(1)
            go func (key string, scheduleFunc VascSchedule, interval int64) {
                lockValue := this.GetGlobalToken(key, interval)
                if lockValue!="" {
                    info := this.GetGlobalScheduleStatus(key)
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

func (this *VascScheduler) LoadSchedule(scheduleList []ScheduleInfo) error {
    this.Runnable = false
    this.ScheduleWaitGroup.Wait()
    this.CycleScheduleList  = make(map[string]*ScheduleInfo)
    
    this.Runnable = true
    
    for _, info := range scheduleList {
        this.setSchedule(info.Key, info.Routine, info.Type, info.Interval, info.Timestamp, info.Scope)
    }
    
    this.ScheduleWaitGroup.Add(1)
    go this.scheduleCycle()
    return nil
}

func (this *VascScheduler) setSchedule(scheduleKey string, schedule VascSchedule, scheduleType uint64, interval int64, timestamp int64, scope int64) error {
    if scheduleType==VASC_SCHEDULE_OVERLAPPED {
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
    } else if scheduleType==VASC_SCHEDULE_SERIAL {
        
        this.ScheduleWaitGroup.Add(1)
        go func(key string, interval int64) {
            for ;this.Runnable; {
                if scope==VASC_SCHEDULE_SCOPE_NATIVE {
                    schedule(key)
                    time.Sleep(time.Second * time.Duration(interval))
                } else if scope==VASC_SCHEDULE_SCOPE_GLOBAL {
                    lockValue := this.GetGlobalToken(key, interval)
                    if lockValue!="" {
                        schedule(key)
                        time.Sleep(time.Second * time.Duration(interval))
                        
                        this.ReleaseToken(key, lockValue)
                    } else {
                        fmt.Printf("%s has been locked\n", key)
                        time.Sleep(time.Second * time.Duration(interval))
                    }
                }
            }
            this.ScheduleWaitGroup.Done()
        }(scheduleKey, interval)
    } else if scheduleType==VASC_SCHEDULE_FIXED {
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
                            lockValue := this.GetGlobalToken(scheduleKey, interval)
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
    } else {
        return errors.New("Invalid schedule type")
    }
    
    return nil
}

func (this *VascScheduler) GetGlobalToken(key string, life int64) string {
    lockValue := fmt.Sprintf("%d:%d", time.Now().UnixNano(), rand.Int())
    //fmt.Printf("trylock:[%s][%s]", key, lockValue)
    
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    //In case of run-once schedule
    if life==0 {
        life = 10
    }
    _, err := redis.String(redisConn.Do("SET", this.RedisPrefix + "token:" + key, lockValue, "EX", life, "NX")) 
    
	if err==redis.ErrNil {
	    //fmt.Sprintf("lockfailed:[%s][%v]\n", key, err)
		return ""
	}
	
	if err!=nil {
	    //fmt.Printf("lockerror:[%s][%v]\n", key, err)
		return ""
	}
	
	//fmt.Printf("locksuccess:[%s][%s]\n", key, lockValue)
	return lockValue
}

func (this *VascScheduler) ReleaseToken(key string, lockValue string) {
    //fmt.Printf("release:[%s][%s]\n", key, lockValue)
    
    redisConn := this.RedisConn.Get()
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
}

func (this *VascScheduler) SetGlobalScheduleStatus(key string, info *ScheduleInfo, life int64) error {
    scheduleInfo, err := json.Marshal(*info)
    if err!=nil {
        return err
    }
    
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    aKey := fmt.Sprintf("%sschedule:%s", this.RedisPrefix, key)
    redisConn.Do("SET",  aKey, string(scheduleInfo), "EX", life)
    return nil
}

func (this *VascScheduler) GetGlobalScheduleStatus(key string) *ScheduleInfo {
    aKey := fmt.Sprintf("%sschedule:%s", this.RedisPrefix, key)
    
    redisConn := this.RedisConn.Get()
    defer redisConn.Close()
    
    scheduleInfo, err := redis.String(redisConn.Do("GET", aKey))
    if err!=nil {
        return nil
    }
    
    var jsonResult ScheduleInfo
    err = json.Unmarshal([]byte(scheduleInfo), &jsonResult)
    if err != nil {
        return nil
    }
    
    return &jsonResult
}