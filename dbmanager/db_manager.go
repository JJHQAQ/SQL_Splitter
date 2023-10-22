package dbmanager

import (
	"SQL_Splitter/util"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type SqlAddress struct {
	UserName string
	Password string
	Ip       string
	Port     string
	DbName   string
}

type DBM struct {
	Databases []*sql.DB
	etcdbar   *clientv3.Client
}

func (dbmp *DBM) init_etcd() {
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

func (dbmp *DBM) init() {
	dbmp.init_etcd()
	var saddrs []SqlAddress
	if util.Test {
		saddrs = []SqlAddress{
			SqlAddress{"root", "123456", "127.0.0.1", "3307", "orderDB"},
			SqlAddress{"root", "123456", "127.0.0.1", "3306", "orderDB"},
		}
	}
	for _, addr := range saddrs {
		db, err := initDB(addr)
		if err != nil {
			fmt.Println(err)
			return
		}
		dbmp.Databases = append(dbmp.Databases, db)
	}

}

func New_DBM() *DBM {
	var dbs DBM
	dbmp := &dbs
	dbmp.init()
	return dbmp
}
