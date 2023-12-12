package dbmanager

import (
	"SQL_Splitter/util"
	"fmt"
)

func (dbmp *DBM) Delete(sql_s string) State {
	table_name, err := util.Get_delete_table(sql_s)
	if err != nil {
		return FAILED
	}
	predicates := util.Predicates(sql_s)
	if table_name == "book" {

	} else if table_name == "customer" {

	} else if table_name == "orders" {

	} else if table_name == "publisher" {

	} else {
		fmt.Println("The inserted data table does not exist!")
		return FAILED
	}

}
