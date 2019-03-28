package vasc

type databaseConfig struct {
    DatabaseConnstr   string         `json:"database_connstr"`
}

type VascDataBase struct {
    
}

func (this *VascDataBase) LoadConfig(configFile string, projectName string, profile string) error {
    return nil
}

func (this *VascDataBase) Close() {
}