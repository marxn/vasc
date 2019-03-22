////////////Some k-v store implementation based on POSIX File System - Do not try to migrate to other platforms.
package vasc

import "os"
import "fmt"
import "time"
import "errors"
import "io/ioutil"
import "encoding/hex"
import "crypto/md5"
import "math/rand"
import "github.com/garyburd/redigo/redis"

const mod_factor1 = 13
const mod_factor2 = 101

type KVManager struct {
    ProjectName string
    FSRoot      string
    RedisHost   string
    RedisPasswd string
    RedisConn   redis.Conn
    Expiration  map[string]int64
}

func (this * KVManager) InitKVStore() error {   
    this.FSRoot = fmt.Sprintf("./%s", this.ProjectName)
    rand.Seed(time.Now().UnixNano())
    
    conn, err := redis.Dial("tcp", this.RedisHost, redis.DialPassword(this.RedisPasswd))
    if err!=nil {
        return errors.New("Cannot connect to redis instance:" + this.RedisHost)
    }
    
    this.RedisConn = conn
    this.Expiration = make(map[string]int64)
    return nil
}

func (this * KVManager) LoadConfig(projectName string, profile string) error {
    this.ProjectName = projectName
    this.RedisHost   = "3a6c14d1c517408d.redis.rds.aliyuncs.com:6379"
    this.RedisPasswd = "Ieq6kabZiGTgQPt"
    return this.InitKVStore()
}

func (this * KVManager) WriteKV(key string, value string, expiration int64, needSync bool) error {
    err := this.SaveToFS(key, value, expiration)
    if err!=nil {
        return errors.New("Cannot write key/value into local FS:" + err.Error())
    }
    
    if needSync {
        return this.SaveRedis(key, value, expiration)
    }
    
    return nil
}

func (this * KVManager) ReadKV(key string, needSync bool) (string, error) {
    value, err := this.GetFromFS(key)
    if value=="" && needSync {
        value = this.GetRedis(key)
        this.SaveToFS(key, value, this.Expiration[key])
    }
    
    return value, err
}

func (this * KVManager) SaveToFS(key string, value string, expiration int64) error {
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

func (this * KVManager) GetFromFS(key string) (string, error) {
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

func (this * KVManager) SaveRedis(key string, value string, expiration int64) error {
    _, err := this.RedisConn.Do("SETEX", key, expiration, value)
    return err
}

func (this * KVManager) GetRedis(key string) string {
    ret, _ := redis.String(this.RedisConn.Do("GET", key))    
    return ret
}
