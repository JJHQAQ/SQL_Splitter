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
			PrintAll(items)
		}
		if dbmp.tables[table_name.String()].Mode == "v" { //垂直分片
			//TODO
			items := dbmp.vertical_fragmentation(sql_s, table_name.String())
			PrintAll(items)

		}

	}

}

// TODO 加更多的表类型，以及改成泛型func
// Customer
func (dbmp *DBM) horizontal_fragmentation(sql_s string, TableName string) interface{} {
	//customer
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
	//orders
	if TableName == "orders" {
		var orders []datatype.Orders
		for _, site := range dbmp.tables[TableName].Sites {
			rows, e := dbmp.Databases[site].Query(sql_s)
			if e != nil {
				// do something
			}
			for rows.Next() {
				var order datatype.Orders
				e := rows.Scan(&order.Customer_id, &order.Book_id, &order.Quantity)
				if e == nil {
					orders = append(orders, order)
				} else {
					fmt.Println(e)
				}
			}
			rows.Close()
		}
		return orders
	}
	//publishers
	if TableName == "publishers" {
		var publishers []datatype.Publishers
		for _, site := range dbmp.tables[TableName].Sites {
			rows, e := dbmp.Databases[site].Query(sql_s)
			if e != nil {
				// do something
			}
			for rows.Next() {
				var publisher datatype.Publishers
				e := rows.Scan(&publisher.Id, &publisher.Name, &publisher.State)
				if e == nil {
					publishers = append(publishers, publisher)
				} else {
					fmt.Println(e)
				}
			}
			rows.Close()
		}
		return publishers
	}
	return nil
}

func (dbmp *DBM) vertical_fragmentation(sql_s string, TableName string) []datatype.Book {
	var books []datatype.Book
	column_book1 := []string{"id", "title"}
	column_book2 := []string{"id", "authors", "publisher_id", "copies"}
	select_book1 := false
	select_book2 := false
	select_from_book1 := "select id"
	select_from_book2 := "select id"
	select_fields, err := util.Get_select_name(sql_s)
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}

	if select_fields[0] == "*" {
		select_book1 = true
		select_book2 = true
		select_from_book1 = "select *"
		select_from_book2 = "select *"
	} else {
		for _, select_column := range select_fields {
			if util.Contains(column_book1, select_column) {
				select_book1 = true
				select_from_book1 += ("," + select_column)

			} else if util.Contains(column_book2, select_column) {
				select_book2 = true
				select_from_book2 += ("," + select_column)

			}
		}
	}

	select_from_book1 += " from book"
	select_from_book2 += " from book"

	predicates := util.Predicates(sql_s)
	book1_has_predicate := false
	book2_has_predicate := false
	for _, predicate := range predicates {
		column, operator, value := util.Extract_predicate_info(predicate)
		if util.Contains(column_book1, column) {
			if !book1_has_predicate {
				select_book1 = true
				book1_has_predicate = true
				select_from_book1 += (" where " + column + " " + operator + " " + value)
			} else {
				select_from_book1 += (", " + column + " " + operator + " " + value)
			}
		}
		if util.Contains(column_book2, column) {
			if !book2_has_predicate {
				select_book2 = true
				book2_has_predicate = true
				select_from_book2 += (" where " + column + " " + operator + " " + value)
			} else {
				select_from_book2 += (", " + column + " " + operator + " " + value)
			}
		}
	}
	select_from_book1 += ";"
	select_from_book2 += ";"
	// var site1, site2 string
	if select_book1 {
		// rows_book1, err1 := dbmp.Databases[site1].Query(select_from_book1)
	}
	if select_book2 {
		// rows_book2, err2 := dbmp.Databases[site2].Query(select_from_book2)
	}
	fmt.Println(select_from_book1)
	fmt.Println(select_from_book2)
	// rows_book1和rows_book2的第一列都必定是id，根据id合并，
	// 最后判断select_fields是不是”*“，
	//    如果是就不需要处理了
	//    如果不是，那么就要把第一列的id删除
	return books
}
