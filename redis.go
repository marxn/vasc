package vasc

import "time"
import "github.com/garyburd/redigo/redis"

type redisConfig struct {
    RedisHost    string         `json:"redis_host"`
    RedisPasswd  string         `json:"redis_passwd"`
}

type VascRedis struct {
    RedisHost    string     
    RedisPasswd  string     
    RedisPool   *redis.Pool 
}

func (this *VascRedis) LoadConfig(config *redisConfig, projectName string) error {
    this.RedisHost   = config.RedisHost
    this.RedisPasswd = config.RedisPasswd
    this.InitRedis()
    
    return nil
}

func (this *VascRedis) InitRedis() {
    this.RedisPool = &redis.Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", this.RedisHost)
            if err != nil {
                return nil, err
            }
            if _, err := c.Do("AUTH", this.RedisPasswd); err != nil {
                c.Close()
                return nil, err
            }
            return c, err
        },
        Wait: true,
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

func (this *VascRedis) Get() redis.Conn {
    return this.RedisPool.Get()
}
func (this *VascRedis) Close() {
    this.RedisPool.Close()
}