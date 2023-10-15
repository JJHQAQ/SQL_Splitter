package main

import (
	"SQL_Splitter/dbmanager"
	"SQL_Splitter/util"
	// "github.com/marianogappa/sqlparser"
	// "github.com/xwb1989/sqlparser"
)

func main() {
	util.Init_DB()
	_ = dbmanager.New_DBM()

	// sql := "SELECT * FROM table WHERE a = 'abc'"
	// stmt, err := sqlparser.Parse(sql)
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
