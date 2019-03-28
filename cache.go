////////////Some k-v cache implementation based on POSIX File System - Do not try to migrate to other platforms.
package vasc

import "os"
import "fmt"
import "time"
import "errors"
import "io/ioutil"
import "encoding/hex"
import "encoding/json"
import "crypto/md5"
import "math/rand"
import "github.com/garyburd/redigo/redis"

const mod_factor1 = 13
const mod_factor2 = 101

type CacheManager struct {
    ProjectName string
    FSRoot      string
    RedisHost   string
    RedisPasswd string
    RedisConn   redis.Conn
    RedisPrefix string
    Expiration  map[string]int64
}

type cacheConfigFile struct {
    CacheRootPath     string         `json:"cache_rootpath"`
    CacheRedisHost    string         `json:"cache_redis_host"`
    CacheRedisPasswd  string         `json:"cache_redis_passwd"`
    CacheRedisPrefix  string         `json:"cache_redis_prefix"`
}

func (this * CacheManager) InitKVStore() error {
    rand.Seed(time.Now().UnixNano())

    conn, err := redis.Dial("tcp", this.RedisHost, redis.DialPassword(this.RedisPasswd))
    if err!=nil {
        return errors.New("Cannot connect to cache redis instance:" + this.RedisHost)
    }

    this.RedisConn = conn
    this.Expiration = make(map[string]int64)
    return nil
}

func (this * CacheManager) LoadConfig(configPath string, projectName string, profile string) error {
    this.ProjectName = projectName

    config, err  := ioutil.ReadFile(configPath + "/" + projectName + "/cache.json")

    if err != nil{
        return errors.New("Cannot find cache config file for project:" + projectName)
    }

    var jsonResult cacheConfigFile
    err = json.Unmarshal([]byte(config), &jsonResult)
    if err != nil {
        return errors.New("Cannot parse cache config file for project:" + projectName)
    }

    _, err = os.Stat(jsonResult.CacheRootPath)
    if err != nil {
        return errors.New("Cache directory does not exist")
    }

    this.RedisHost   = jsonResult.CacheRedisHost
    this.RedisPasswd = jsonResult.CacheRedisPasswd
    this.RedisPrefix = jsonResult.CacheRedisPrefix
    this.FSRoot      = jsonResult.CacheRootPath + "/" + projectName

    return this.InitKVStore()
}

func (this * CacheManager) Close() {
    this.RedisConn.Close()
}

func (this * CacheManager) WriteKV(key string, value string, expiration int64, needSync bool) error {
    err := this.SaveToFS(key, value, expiration)
    if err!=nil {
        return errors.New("Cannot write key/value into local FS:" + err.Error())
    }

    if needSync {
        return this.SaveRedis(key, value, expiration)
    }

    return nil
}

func (this * CacheManager) ReadKV(key string, needSync bool) (string, error) {
    value, err := this.GetFromFS(key)
    if value=="" && needSync {
        value = this.GetRedis(key)
        this.SaveToFS(key, value, this.Expiration[key])
    }

    return value, err
}

func (this * CacheManager) SaveToFS(key string, value string, expiration int64) error {
    randNum  := rand.Int()
    keyHash1 := pathHash(key, mod_factor1)
    keyHash2 := pathHash(key, mod_factor2)
    keyFile  := fileHash(key)

    path := fmt.Sprintf("%s/%d/%d", this.FSRoot, keyHash1, keyHash2)
    os.MkdirAll(path, os.ModePerm)

    tempFilePath := fmt.Sprintf("%s/%s.%d", path, keyFile, randNum)
    formalFilePath := fmt.Sprintf("%s/%s", path, keyFile)

	err := ioutil.WriteFile(tempFilePath, []byte(value), os.ModePerm)
	if err!=nil {
	    return err
	}

    err = os.Rename(tempFilePath, formalFilePath)
    if err!=nil {
	    return err
	}

	this.Expiration[key] = expiration

	return nil
}

func (this * CacheManager) GetFromFS(key string) (string, error) {
    keyHash1 := pathHash(key, mod_factor1)
    keyHash2 := pathHash(key, mod_factor2)
    keyFile  := fileHash(key)

    path := fmt.Sprintf("%s/%d/%d", this.FSRoot, keyHash1, keyHash2)
    valueFilePath := fmt.Sprintf("%s/%s", path, keyFile)

    statRet, err := os.Stat(valueFilePath)
    if err != nil{
        return "", errors.New("key/value does not exist")
    }

    keyExprTime := this.Expiration[key]

    if keyExprTime != 0 && statRet.ModTime().Unix() - time.Now().Unix() > keyExprTime {
        os.Remove(valueFilePath)
        return "", errors.New("value expired")
    }

    content, err  := ioutil.ReadFile(valueFilePath)
    if err != nil{
        return "", errors.New("key/value removed")
    }

    return string(content), nil
}

func pathHash(key string, factor uint32) uint32 {
    byteArray := []byte(key)
    var sum uint32 = 0

    for i:=0; i < len(byteArray); i++ {
        sum += uint32(byteArray[i])
    }

    return sum % factor
}

func fileHash(key string) string {
    return md5Hash(key)
}

func md5Hash(content string) string {
    h := md5.New()
    h.Write([]byte(content))
    bs := h.Sum(nil)
    return hex.EncodeToString(bs)
}

func (this * CacheManager) SaveRedis(key string, value string, expiration int64) error {
    redisKey := md5Hash(this.RedisPrefix + key)
    _, err := this.RedisConn.Do("SETEX", redisKey, expiration, value)
    return err
}

func (this * CacheManager) GetRedis(key string) string {
    redisKey := md5Hash(this.RedisPrefix + key)
    ret, _ := redis.String(this.RedisConn.Do("GET", redisKey, this.RedisPrefix + key))
    return ret
}
