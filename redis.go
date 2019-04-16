package vasc

//import "fmt"
import "time"
//import "runtime/debug"
import "github.com/garyburd/redigo/redis"

type redisInstanceConfig struct {
    Key            string               `json:"key"`
    RedisHost      string               `json:"redis_host"`
    RedisPasswd    string               `json:"redis_passwd"`
    MaxIdle        int                  `json:"max_idle"`
    IdleTimeout    int                  `json:"idle_timeout"`
    Wait           bool                 `json:"wait"`
}

type redisConfig struct {
    Enable         bool                 `json:"enable"`
    InstanceList []redisInstanceConfig  `json:"instance_list"`
}

type VascRedis struct {
    RedisPool    map[string]*redis.Pool 
    Runnable     bool
}

func (this *VascRedis) LoadConfig(config *redisConfig, projectName string) error {
    this.RedisPool = make(map[string]*redis.Pool)
    for _, value := range config.InstanceList {
        this.RedisPool[value.Key] = &redis.Pool{
            MaxIdle: value.MaxIdle,
            IdleTimeout: time.Duration(value.IdleTimeout) * time.Second,
            Dial: func () (redis.Conn, error) {
                c, err := redis.Dial("tcp", value.RedisHost)
                if err != nil {
                    return nil, err
                }
                if _, err := c.Do("AUTH", value.RedisPasswd); err != nil {
                    c.Close()
                    return nil, err
                }
                return c, err
            },
            Wait: value.Wait,
            TestOnBorrow: func(c redis.Conn, t time.Time) error {
                _, err := c.Do("PING")
                return err
            },
        }
    }
    this.Runnable = true
    
    return nil
}

func (this *VascRedis) Get(key string) *redis.Pool {
    if this.Runnable {
        result := this.RedisPool[key]
        if result==nil {
            return nil
        }
        return result
    }
    return nil
}

func (this *VascRedis) Close() {
    this.Runnable = false
    for _, value := range this.RedisPool {
        value.Close()
    }
}