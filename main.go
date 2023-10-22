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
	return
}

func main() {
	util.Init_DB()
	dbm := dbmanager.New_DBM()

	Init()

	for {
		//Input the SQL
		SQL_s := "select * from book where id<200050"
		//Parse the SQL

		//case Select

		dbm.Select(SQL_s)

		if util.Test {
			break
		}
	}

	// var book datatype.Book
	// rows, e := dbm.Databases[0].Query("select * from book where id in (200001,200002,200003)")
	// if e == nil {
	// 	errors.New("query incur error")
	// }
	// for rows.Next() {
	// 	e := rows.Scan(&book.Id, &book.Title, &book.Authors, &book.Publisher_id, &book.Copies)
	// 	if e == nil {
	// 		// fmt.Println(book)
	// 		// json.Unmarshal(book)
	// 		jbyte, _ := json.Marshal(book)
	// 		fmt.Println(string(jbyte))
	// 	} else {
	// 		fmt.Println(e)
	// 	}
	// }
	// rows.Close()

	// while(1){

	// }
	// sql := "SELECT * FROM table WHERE a = 'abc'"
	// _, err := sqlparser.Parse(sql)
	// if err != nil {
	// 	// Do something with the err
	// }

	// // Otherwise do something with stmt
	// switch stmt := stmt.(type) {
	// case *sqlparser.Select:
	// 	_ = stmt
	// case *sqlparser.Insert:
	// }
	// q, err := sqlparser.Parse("SELECT id FROM publisher WHERE id < '100010' ")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("%+#v", q)

	// query1 := q
	// query2 := q

	// query1.Conditions[0].Operand2 = "100005"
	// cod := query1.Conditions[0]
	// cod.Operator = query.Gte
	// query2.Conditions = append(query2.Conditions, cod)

	return
}
