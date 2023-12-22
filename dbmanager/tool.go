package dbmanager

import (
	"SQL_Splitter/datatype"
	"database/sql"
	"fmt"
	"strings"
)

func initDB(saddr datatype.SqlAddress) (*sql.DB, error) {
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
	fmt.Println("mysql connnect success(IP:PORT : " + saddr.Ip + ":" + saddr.Port + ")")
	return DB, nil
}

func PrintAll(tableName string, info map[string]interface{}) {
	fmt.Println("查询结果:")
	// 获取列名
	columns := getColumns(info["items"].([]map[string]interface{}))
	// 打印列名
	fmt.Println(strings.Join(columns, "\t"))
	// 打印数据
	for _, item := range info["items"].([]map[string]interface{}) {
		printRow(item, columns)
	}
	fmt.Printf("表名：%s\n", tableName)
	fmt.Printf("行数：%d\n", info["rowCount"].(int))
	fmt.Printf("列数：%d\n", info["colCount"].(int))
	fmt.Printf("站点：%v\n", info["siteNames"].([]string))
	fmt.Println("---------------")
}

func Merge(J_table map[string]interface{}, ori map[string]interface{}, table_name string) map[string]interface{} {
	if J_table == nil {
		items := []map[string]interface{}{}
		for _, ori_item := range ori["items"].([]map[string]interface{}) {
			temp := make(map[string]interface{})
			for k, v := range ori_item {
				temp[table_name+"."+k] = v
			}
			items = append(items, temp)
		}
		final := map[string]interface{}{
			"items":     items,
			"rowCount":  ori["rowCount"],
			"colCount":  ori["colCount"],
			"siteNames": ori["siteNames"],
		}
		return final
	}

	rowCount := J_table["rowCount"].(int) * ori["rowCount"].(int)
	colCount := J_table["colCount"].(int) * ori["colCount"].(int)
	unique_sites := make(map[string]bool)
	for _, name := range J_table["siteNames"].([]string) {
		unique_sites[name] = true
	}
	for _, name := range ori["siteNames"].([]string) {
		unique_sites[name] = true
	}
	siteNames := []string{}

	for k, _ := range unique_sites {
		siteNames = append(siteNames, k)
	}
	items := []map[string]interface{}{}
	for _, j_item := range J_table["items"].([]map[string]interface{}) {
		for _, ori_item := range ori["items"].([]map[string]interface{}) {
			temp := j_item
			for k, v := range ori_item {
				temp[table_name+"."+k] = v
			}
			items = append(items, temp)
		}
	}
	final := map[string]interface{}{
		"items":     items,
		"rowCount":  rowCount,
		"colCount":  colCount,
		"siteNames": siteNames,
	}
	return final
}

func Compare(a interface{}, b interface{}) bool {

	switch a.(type) {
	case string:
		// fmt.Println(a.(string), b.(string))
		return a.(string) == b.(string)
	case int:

		return a.(int) == b.(int)
	}
	return true
}
