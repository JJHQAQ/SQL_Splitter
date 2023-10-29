package dbmanager

import (
	"SQL_Splitter/datatype"
	"SQL_Splitter/util"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xwb1989/sqlparser"
)

func (dbmp *DBM) Select(sql_s string) {
	if util.Test {
		return
	}

	// function Parse parses the sql statement into a tree
	tree, _ := sqlparser.Parse(sql_s)
	for _, table := range tree.(*sqlparser.Select).From {
		table_name := sqlparser.GetTableName(table.(*sqlparser.AliasedTableExpr).Expr) //获取表名

		if dbmp.tables[table_name.String()].Mode == "h" { //水平分片
			items := dbmp.horizontal_fragmentation(sql_s, table_name.String())
			for _, x := range items {
				fmt.Println(x)
			}
		}
		if dbmp.tables[table_name.String()].Mode == "v" { //垂直分片
			//TODO
		}

	}

}

// TODO 加更多的表类型，以及改成泛型func
func (dbmp *DBM) horizontal_fragmentation(sql_s string, TableName string) []datatype.Customer {
	if TableName == "customer" {
		var customers []datatype.Customer
		for _, site := range dbmp.tables[TableName].Sites {
			rows, e := dbmp.Databases[site].Query(sql_s)
			if e != nil {
				// do something
			}
			for rows.Next() {
				var customer datatype.Customer
				e := rows.Scan(&customer.Id, &customer.Name, &customer.Gender)
				if e == nil {
					customers = append(customers, customer)
				} else {
					fmt.Println(e)
				}
			}
			rows.Close()
		}
		return customers
	}

	return nil
}

func (dbmp *DBM) vertical_fragmentation(sql_s string) []datatype.Book {
	var books []datatype.Book
	return books
}
