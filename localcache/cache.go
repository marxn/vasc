/*
 * Some kind of Key-Value styled cache implementation based on Linux File System
 * It is LOCK-FREE - Do not try to migrate it to other platform.
 * Author: Kevin Wang
 */

package localcache

import "os"
import "fmt"
import "time"
import "errors"
import "io/ioutil"
import "encoding/hex"
import "crypto/md5"
import "math/rand"
import "github.com/garyburd/redigo/redis"
import "github.com/marxn/vasc/global" 
import vredis "github.com/marxn/vasc/redis" 

const mod_factor1 = 13
const mod_factor2 = 101

type CacheManager struct {
    ProjectName string
    FSRoot      string
    RedisConn  *redis.Pool
    RedisPrefix string
    Expiration  map[string]int64
}


func (this * CacheManager) LoadConfig(config *global.CacheConfigFile, redisPoolList *vredis.VascRedis, projectName string) error {
    _, err := os.Stat(config.CacheRootPath)
    if err != nil {
        return errors.New("Cache directory does not exist")
    }
    
    rand.Seed(time.Now().UnixNano())
    this.ProjectName = projectName
    if redisPoolList!=nil && config.CacheSourceRedis!=""{
        redis := redisPoolList.Get(config.CacheSourceRedis)
        if redis==nil {
            return errors.New("cannot get redis instance for cache sync")
        }
        this.RedisConn = redis
    }
    this.FSRoot      = config.CacheRootPath + "/" + projectName
    this.RedisPrefix = fmt.Sprintf("VASC:%s:CACHE:", projectName)
    this.Expiration  = make(map[string]int64)
    
    return nil
}

func (this * CacheManager) Close() {
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
        value, err := this.GetRedis(key)
        if err!=nil {
            return "", err
        }
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
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return errors.New("cannot get redis connection for writing")
    }
    defer redisConn.Close()
    
    _, err := redisConn.Do("SETEX", redisKey, expiration, value)
    return err
}

func (this * CacheManager) GetRedis(key string) (string, error) {
    redisKey := md5Hash(this.RedisPrefix + key)
    redisConn := this.RedisConn.Get()
    if redisConn==nil {
        return "", errors.New("cannot get redis connection for reading")
    }
    defer redisConn.Close()
    
    ret, _ := redis.String(redisConn.Do("GET", redisKey, this.RedisPrefix + key))
    return ret, nil
}
