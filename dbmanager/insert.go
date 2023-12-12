package dbmanager

import (
	"SQL_Splitter/util"
	"fmt"
)

type State string

// 定义一个常量集合模拟枚举
const (
	Defalut State = "DEFAULT"
	SUCCESS       = "SUCCESS"
	FAILED        = "FAILED"
)

func (dbmp *DBM) Insert(sql_s string) State {
	table_name, err := util.Get_insert_table(sql_s)
	if err != nil {
		fmt.Println("Error:", err)
		return FAILED
	}
	// book 垂直分片，其他水平分片
	if table_name == "book" {

	} else if table_name == "customer" {

	} else if table_name == "orders" {

	} else if table_name == "publisher" {

	} else {
		fmt.Println("The inserted data table does not exist!")
		return FAILED
	}
	return SUCCESS
}
