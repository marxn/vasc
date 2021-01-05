package database

import "errors"
import "fmt"
import "time"
import "log/syslog"
import _ "github.com/go-sql-driver/mysql"
import "github.com/go-xorm/xorm"
import "github.com/marxn/vasc/global"

type VascDataBase struct {
    Engine map[string]*xorm.Engine
}

func (this *VascDataBase) LoadConfig(config *global.DatabaseConfig, projectName string) error {
    dbNum := len(config.InstanceList)
    if dbNum < 1 {
        return errors.New("empty database config")
    }
    
    var logger *xorm.SimpleLogger
    if config.EnableLogger {
        logWriter, err := syslog.New(syslog.LOG_INFO|syslog.LOG_LOCAL6, projectName + "/_sql")
        if err != nil {
            return err
        }
        
        logger = xorm.NewSimpleLogger(logWriter)
        logger.ShowSQL(true)
    }
    
    this.Engine = make(map[string]*xorm.Engine)

    for index, value := range config.InstanceList {
        if config.InstanceList[index].DatabaseConnstr == "" {
            return errors.New(fmt.Sprintf("empty database connection string for index:%d, total:%d", index, dbNum))
        }

        conn, err := xorm.NewEngine("mysql", config.InstanceList[index].DatabaseConnstr)
        if err != nil {
            return errors.New("cannot connect to database: " + config.InstanceList[index].DatabaseConnstr)
        }

        this.Engine[value.Key] = conn

        this.Engine[value.Key].ShowSQL(false)

        if config.InstanceList[index].MaxIdelConns > 0 {
            this.Engine[value.Key].SetMaxIdleConns(config.InstanceList[index].MaxIdelConns)
        } else {
            this.Engine[value.Key].SetMaxIdleConns(10)
        }

        if config.InstanceList[index].MaxOpenConns > 0 {
            this.Engine[value.Key].SetMaxOpenConns(config.InstanceList[index].MaxOpenConns)
        } else {
            this.Engine[value.Key].SetMaxOpenConns(100)
        }

        if config.InstanceList[index].Location != "" {
            this.Engine[value.Key].TZLocation, _ = time.LoadLocation(config.InstanceList[index].Location)
        } else {
            this.Engine[value.Key].TZLocation, _ = time.LoadLocation("Asia/Shanghai")
        }
        
        if config.EnableLogger {
            this.Engine[value.Key].SetLogger(logger)
        }
    }

    return this.InitDatabase()
}

func (this *VascDataBase) InitDatabase() error {
    for _, value := range this.Engine {
        err := value.Ping()
        if err != nil {
            return err
        }
    }
    return nil
}

func (this *VascDataBase) Close() {
    for _, value := range this.Engine {
        value.Close()
    }
}

func (this *VascDataBase) GetEngine(key string) (*xorm.Engine, error) {
    result := this.Engine[key]
    if result != nil {
        return result, nil
    }

    return nil, errors.New("cannot find database: " + key)
}

func (this *VascDataBase) GetEngineList() ([]string, error) {
    var result []string
    for k, _ := range this.Engine {
        result = append(result, k)
    }
    return result, nil
}
