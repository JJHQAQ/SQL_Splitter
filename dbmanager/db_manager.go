package dbmanager

import (
	"SQL_Splitter/util"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xwb1989/sqlparser"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type SqlAddress struct {
	Site_name string
	UserName  string
	Password  string
	Ip        string
	Port      string
	DbName    string
}

type Table struct {
	Name  string   `json:"name"`
	Mode  string   `json:"mode"`
	Sites []string `json:"sites"`
}

type DBM struct {
	Databases map[string]*sql.DB
	etcdbar   *clientv3.Client

	tables map[string]Table
}

func (dbmp *DBM) init_conf() {
	file, err := os.Open(util.Conf_path + "tables.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	byteValue, _ := ioutil.ReadAll(file)
	var tables []Table
	json.Unmarshal(byteValue, &tables)
	for _, x := range tables {
		dbmp.tables[x.Name] = x
	}

}
func (dbmp *DBM) init_etcd() {
	//TODO 这里啥也没干
	if util.Test {
		return
	}
	cfg := clientv3.Config{
		Endpoints:   []string{util.EtcdAddr}, //etcd服务器的地址
		DialTimeout: 5 * time.Second,         //建立连接的超时时间
	}
	// 创建etcd客户端
	cli, err := clientv3.New(cfg)
	if err != nil {
		fmt.Printf("创建etcd客户端失败：%v \n", err)
		return
	} else {
		fmt.Println("连接etcd成功! (" + util.EtcdAddr + ")")
	}

	dbmp.etcdbar = cli
}

func (dbmp *DBM) init_mysql() {
	var saddrs []SqlAddress
	//TODO 从配置文件global.json或者从etcd读取
	saddrs = []SqlAddress{
		SqlAddress{"site1", "root", "123456", "127.0.0.1", "3307", "orderDB"},
		SqlAddress{"site2", "root", "123456", "127.0.0.1", "3306", "orderDB"},
	}

	for _, addr := range saddrs {
		db, err := initDB(addr)
		if err != nil {
			fmt.Println(err)
			return
		}
		dbmp.Databases[addr.Site_name] = db
	}
}

func (dbmp *DBM) init() {
	dbmp.tables = make(map[string]Table)
	dbmp.Databases = make(map[string]*sql.DB)
	dbmp.init_conf()
	dbmp.init_etcd()
	dbmp.init_mysql()

}

func (dbmp *DBM) Do(sql_s string) {
	class_code := sqlparser.Preview(sql_s)
	if sqlparser.StmtType(class_code) == "SELECT" {
		dbmp.Select(sql_s)
	}
}

func New_DBM() *DBM {
	var dbs DBM
	dbmp := &dbs
	dbmp.init()
	return dbmp
}
