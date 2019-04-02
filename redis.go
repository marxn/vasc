package vasc

import "time"
import "github.com/garyburd/redigo/redis"

type redisConfig struct {
    Host         string         `json:"host"`
    Passwd       string         `json:"passwd"`
}

type VascRedis struct {
    RedisHost    string     
    RedisPasswd  string     
    RedisPrefix  string     
    RedisPool   *redis.Pool 
}

func (this *VascRedis) LoadConfig(configFile string, projectName string, profile string) error {
    return nil
}

func (this *VascRedis) SetConfig(host string, passwd string, prefix string) {
    this.RedisHost   = host
    this.RedisPasswd = passwd
    this.RedisPrefix = prefix
    this.InitRedis()
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