package dbmanager

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
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
}

type DBMP *DBM

func (dbmp *DBM) init() {

	saddrs := []SqlAddress{
		SqlAddress{"root", "123456", "127.0.0.1", "3307", "orderDB"},
		SqlAddress{"root", "123456", "127.0.0.1", "3306", "orderDB"},
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

func New_DBM() DBMP {
	var dbs DBM
	dbmp := &dbs
	dbmp.init()
	return dbmp
}
