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

	result := make(map[string]interface{})

	// function Parse parses the sql statement into a tree
	stmt, err := sqlparser.Parse(sql_s)
	if err != nil {
		fmt.Println("SQL解析失败: ", err)
		return
	}
	tree, ok := stmt.(*sqlparser.Select)
	if !ok {
		fmt.Println(ok)
		return
	}

	var sql_no_join string

	// Check if the SQL statement contains a WHERE clause
	if tree.Where != nil && tree.Where.Expr != nil {
		sql_no_join = util.Join_fileter(sql_s, dbmp.tables)
	} else {
		sql_no_join = sql_s
	}

	fmt.Println(sql_no_join)
	for i, table := range tree.From {
		table_name := sqlparser.GetTableName(table.(*sqlparser.AliasedTableExpr).Expr).String() //获取表名

		var table_sql string
		if tree.Where != nil && tree.Where.Expr != nil {
			table_sql = util.Table_filter(sql_no_join, i, dbmp.tables[table_name].Columns)
		} else {
			table_sql = sql_s
		}

		// fmt.Println(dbmp.tables[table_name].Columns)
		// fmt.Println(table_sql)

		if dbmp.tables[table_name].Mode == "h" { //水平分片
			items, rowCount, colCount, siteNames := dbmp.horizontal_fragmentation(table_sql, table_name)
			result[table_name] = map[string]interface{}{
				"items":     items,
				"rowCount":  rowCount,
				"colCount":  colCount,
				"siteNames": siteNames,
			}
			for tableName, info := range result {
				fmt.Printf("表名：%s\n", tableName)
				fmt.Printf("行数：%d\n", info.(map[string]interface{})["rowCount"].(int))
				fmt.Printf("列数：%d\n", info.(map[string]interface{})["colCount"].(int))
				fmt.Printf("站点：%v\n", info.(map[string]interface{})["siteNames"].([]string))
				fmt.Println("查询结果:")
				fmt.Println(info.(map[string]interface{})["items"])
				fmt.Println("---------------")
			}
			// PrintAll(items)
		}
		if dbmp.tables[table_name].Mode == "v" { //垂直分片
			items := dbmp.vertical_fragmentation(table_sql, table_name)
			// PrintAll(items)
			result[table_name] = items
		}

	}

}

// TODO 加更多的表类型，以及改成泛型func
// Customer
func (dbmp *DBM) horizontal_fragmentation(sql_s string, TableName string) ([]string, int, int, []string) {
	//customer
	if TableName == "customer" {
		var customers []datatype.Customer
		rowCount := 0
		colCount := 0
		var siteNames []string

		for _, site := range dbmp.tables[TableName].Sites {
			rows, e := dbmp.Databases[site].Query(sql_s)
			if e != nil {
				// do something
				fmt.Println(e)
				return nil, 0, 0, nil
			}
			// 获取列名
			columns, _ := rows.Columns()
			colCount = len(columns)

			hasResult := false

			for rows.Next() {
				hasResult = true
				rowCount++
				var customer datatype.Customer
				e := rows.Scan(&customer.Id, &customer.Name, &customer.Gender)
				if e == nil {
					customers = append(customers, customer)
				} else {
					fmt.Println(e)
				}
			}
			rows.Close()
			// 只有在站点有结果时才加入站点列表
			if hasResult {
				siteNames = append(siteNames, site)
			}
		}
		// 将customers转换为字符串切片
		var result []string
		for _, customer := range customers {
			result = append(result, fmt.Sprintf("%+v", customer))
		}
		return result, rowCount, colCount, siteNames
	}
	//orders
	if TableName == "orders" {
		var orders []datatype.Orders
		rowCount := 0
		colCount := 0
		var siteNames []string

		for _, site := range dbmp.tables[TableName].Sites {
			rows, e := dbmp.Databases[site].Query(sql_s)
			if e != nil {
				fmt.Println(e)
				return nil, 0, 0, nil
			}
			// 获取列名
			columns, _ := rows.Columns()
			colCount = len(columns)

			hasResult := false

			for rows.Next() {
				hasResult = true
				rowCount++
				var order datatype.Orders
				e := rows.Scan(&order.Customer_id, &order.Book_id, &order.Quantity)
				if e == nil {
					orders = append(orders, order)
				} else {
					fmt.Println(e)
				}
			}
			rows.Close()
			// 只有在站点有结果时才加入站点列表
			if hasResult {
				siteNames = append(siteNames, site)
			}
		}
		// 将orders转换为字符串切片
		var result []string
		for _, order := range orders {
			result = append(result, fmt.Sprintf("%+v", order))
		}
		return result, rowCount, colCount, siteNames
	}
	//publishers
	if TableName == "publishers" {
		var publishers []datatype.Publishers
		rowCount := 0
		colCount := 0
		var siteNames []string

		for _, site := range dbmp.tables[TableName].Sites {
			rows, e := dbmp.Databases[site].Query(sql_s)
			if e != nil {
				fmt.Println(e)
				return nil, 0, 0, nil
			}
			// 获取列名
			columns, _ := rows.Columns()
			colCount = len(columns)

			hasResult := false

			for rows.Next() {
				hasResult = true
				rowCount++
				var publisher datatype.Publishers
				e := rows.Scan(&publisher.Id, &publisher.Name, &publisher.State)
				if e == nil {
					publishers = append(publishers, publisher)
				} else {
					fmt.Println(e)
				}
			}
			rows.Close()
			// 只有在站点有结果时才加入站点列表
			if hasResult {
				siteNames = append(siteNames, site)
			}
		}
		// 将publishers转换为字符串切片
		var result []string
		for _, publisher := range publishers {
			result = append(result, fmt.Sprintf("%+v", publisher))
		}
		return result, rowCount, colCount, siteNames
	}
	return nil, 0, 0, nil
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
