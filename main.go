package main

import (
	"SQL_Splitter/dbmanager"
	"SQL_Splitter/util"
	"fmt"

	// "github.com/marianogappa/sqlparser"
	// "github.com/xwb1989/sqlparser"
	"github.com/zztroot/color"
)

func Init() {
	fmt.Println(color.Coat("SQL_Splitter running....", color.Green))
}

func main() {
	util.Init()
	dbm := dbmanager.New_DBM()

	Init()

	for {
		//Input the SQL
		SQL_s := "select * from customer"
		//SQL_s := "select * from customer where id <= 310000"
		// SQL_s := "select * from book where publisher_id<300050"
		// SQL_s := "select * from book,publisher where book.publisher_id = publisher.id and publisher.name= 'Twomorrows Publishing' and book.copies>6000"
		dbm.Do(SQL_s)

		break
	}

}
