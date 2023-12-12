package dbmanager

import (
	"SQL_Splitter/util"
	"fmt"
)

func (dbmp *DBM) Insert(sql_s string) {
	table_name, err := util.Get_insert_table(sql_s);
	if err != nil {
		fmt.Println("Error:", err)
		return 
	}
	if table_name == 
}
