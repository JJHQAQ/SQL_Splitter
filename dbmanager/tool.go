package dbmanager

import (
	"database/sql"
	"fmt"
	"strings"
)

func initDB(saddr SqlAddress) (*sql.DB, error) {
	var DB *sql.DB
	//构建连接："用户名:密码@tcp(IP:端口)/数据库?charset=utf8"
	path := strings.Join([]string{saddr.UserName, ":", saddr.Password, "@tcp(", saddr.Ip, ":", saddr.Port, ")/", saddr.DbName, "?charset=utf8"}, "")
	//打开数据库,前者是驱动名，所以要导入： _ "github.com/go-sql-driver/mysql"
	DB, _ = sql.Open("mysql", path)
	//设置数据库最大连接数
	DB.SetConnMaxLifetime(100)
	//设置上数据库最大闲置连接数
	DB.SetMaxIdleConns(10)
	//验证连接
	if err := DB.Ping(); err != nil {
		fmt.Println("open database fail")
		return nil, err
	}
	fmt.Println("connnect success")
	return DB, nil
}
