package dbmanager

import (
	"SQL_Splitter/util"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xwb1989/sqlparser"
)

func (dbmp *DBM) Select(sql_s string) {
	if util.Test {
		return
	}

	result := make(map[string](map[string]interface{}))

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
	var predicates []sqlparser.Expr
	var origin_SelectExprs sqlparser.SelectExprs
	NeedJoin := false
	if len(tree.From) > 1 {
		NeedJoin = true
	}
	// Check if the SQL statement contains a WHERE clause
	if tree.Where != nil && tree.Where.Expr != nil && NeedJoin {

		sql_no_join, predicates, origin_SelectExprs = util.Join_fileter(sql_s, dbmp.tables)
	} else {
		sql_no_join = sql_s
	}

	fmt.Println(sql_no_join)
	if NeedJoin {
		fmt.Println(sqlparser.String(predicates[0]))
		fmt.Println(sqlparser.String(origin_SelectExprs))
	}
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
			if !NeedJoin {
				for tableName, info := range result {
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
			}
			// PrintAll(items)
		}
		if dbmp.tables[table_name].Mode == "v" { //垂直分片
			items, rowCount, colCount, siteNames := dbmp.vertical_fragmentation(table_sql, table_name)
			result[table_name] = map[string]interface{}{
				"items":     items,
				"rowCount":  rowCount,
				"colCount":  colCount,
				"siteNames": siteNames,
			}
			if !NeedJoin {
				for tableName, info := range result {
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
			}
			// PrintAll(items)
		}
	}

	if NeedJoin {
		var JoinResult map[string]interface{}
		JoinResult = nil
		colNum := 0
		for tableName, info := range result {
			// fmt.Println(tableName, ": ", len(info["items"].([]map[string]interface{})))
			// fmt.Println(info["items"].([]map[string]interface{}))
			JoinResult = Merge(JoinResult, info, tableName)
			colNum = colNum + info["colCount"].(int)
		}
		// fmt.Println(len(JoinResult["items"].([]map[string]interface{})))
		items := []map[string]interface{}{}
		for _, item := range JoinResult["items"].([]map[string]interface{}) {
			flag := true
			for _, predicate := range predicates {
				ColLeft := sqlparser.String(predicate.(*sqlparser.ComparisonExpr).Left)
				ColRight := sqlparser.String(predicate.(*sqlparser.ComparisonExpr).Right)
				if !Compare(item[ColLeft], item[ColRight]) {
					flag = false
				}
			}
			if flag {
				items = append(items, item)
			}
		}
		// fmt.Println(len(items))

		items_final := []map[string]interface{}{}
		colList := []string{}
		if sqlparser.String(origin_SelectExprs) != "*" {
			colNum = len(origin_SelectExprs)
			for _, it := range origin_SelectExprs {
				colList = append(colList, sqlparser.String(it))
			}
			for _, item := range items {
				temp := make(map[string]interface{})
				for _, col := range colList {
					temp[col] = item[col]
				}
				items_final = append(items_final, temp)
			}
		} else {
			items_final = items
		}
		JoinResult["items"] = items_final
		JoinResult["rowCount"] = len(JoinResult["items"].([]map[string]interface{}))
		JoinResult["colCount"] = colNum
		// fmt.Println(JoinResult["items"].([]map[string]interface{}))
		PrintAll("final result", JoinResult)
	}

}

// TODO 加更多的表类型，以及改成泛型func
// Customer
func (dbmp *DBM) horizontal_fragmentation(sql_s string, TableName string) ([]map[string]interface{}, int, int, []string) {

	if TableName == "customer" || TableName == "orders" || TableName == "publisher" {
		var results []map[string]interface{}
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
				result := make(map[string]interface{})
				// 创建一个切片来保存列指针
				columnPointers := make([]interface{}, len(columns))
				for i := range columns {
					columnPointers[i] = new(interface{})
				}

				// 将行扫描到列指针中
				if err := rows.Scan(columnPointers...); err != nil {
					fmt.Println("扫描行时发生错误:", err)
					continue
				}

				// 遍历列指针，将值填充到 map 中
				for i, col := range columns {
					switch val := (*columnPointers[i].(*interface{})).(type) {
					case []byte:
						result[col] = string(val)
					default:
						result[col] = val
					}
				}
				rowCount++
				results = append(results, result)
			}
			rows.Close()
			// 只有在站点有结果时才加入站点列表
			if hasResult {
				siteNames = append(siteNames, site)
			}
		}
		return results, rowCount, colCount, siteNames
	}

	return nil, 0, 0, nil
}

func (dbmp *DBM) vertical_fragmentation(sql_s string, TableName string) ([]map[string]interface{}, int, int, []string) {
	if TableName == "book" {
		var books, books1, books2 []map[string]interface{}
		var rowCount, colCount int
		var resultSites []string
		column_book1 := []string{"id", "title"}
		column_book2 := []string{"id", "authors", "publisher_id", "copies"}
		select_book1 := false
		select_book2 := false
		select_from_book1 := "select id"
		select_from_book2 := "select id"
		select_fields, err := util.Get_select_name(sql_s)
		if err != nil {
			fmt.Println("Error:", err)
			return nil, 0, 0, nil
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
					select_from_book2 += (" and " + column + " " + operator + " " + value)
				}
			}
		}
		select_from_book1 += ";"
		select_from_book2 += ";"
		// fmt.Println(select_from_book1)
		// fmt.Println(select_from_book2)

		// Query databases and merge results
		if select_book1 {
			rowsBook1, err1 := dbmp.Databases["site1"].Query(select_from_book1)
			if err1 != nil {
				fmt.Println("查询 site1 时出错:", err1)
				return nil, 0, 0, nil
			}
			defer rowsBook1.Close()
			columns, _ := rowsBook1.Columns()
			// Process rowsBook1 and append to books
			resultSites = append(resultSites, "site1")
			for rowsBook1.Next() {
				result := make(map[string]interface{})
				// 创建一个切片来保存列指针
				columnPointers := make([]interface{}, len(columns))
				for i := range columns {
					columnPointers[i] = new(interface{})
				}
				// 将行扫描到列指针中
				if err := rowsBook1.Scan(columnPointers...); err != nil {
					fmt.Println("扫描行时发生错误:", err)
					continue
				}
				// 遍历列指针，将值填充到 map 中
				for i, col := range columns {
					switch val := (*columnPointers[i].(*interface{})).(type) {
					case []byte:
						result[col] = string(val)
					default:
						result[col] = val
					}
				}
				books1 = append(books1, result)
			}
		}
		if select_book2 {
			rowsBook2, err2 := dbmp.Databases["site2"].Query(select_from_book2)
			if err2 != nil {
				fmt.Println("查询 site2 时出错:", err2)
				return nil, 0, 0, nil
			}
			defer rowsBook2.Close()
			columns, _ := rowsBook2.Columns()
			// 处理 rowsBook2 并附加到 books
			resultSites = append(resultSites, "site2")
			for rowsBook2.Next() {
				var result = make(map[string]interface{})
				// 创建一个切片来保存列指针
				columnPointers := make([]interface{}, len(columns))
				for i := range columns {
					columnPointers[i] = new(interface{})
				}
				// 将行扫描到列指针中
				if err := rowsBook2.Scan(columnPointers...); err != nil {
					fmt.Println("扫描行时发生错误:", err)
					continue
				}
				// 遍历列指针，将值填充到 map 中
				for i, col := range columns {
					switch val := (*columnPointers[i].(*interface{})).(type) {
					case []byte:
						result[col] = string(val)
					default:
						result[col] = val
					}
				}
				books2 = append(books2, result)
			}
		}
		books = mergeMapsByKey(books1, books2, "id")
		// 检查是否应该移除 "id" 列
		removeIDColumn := !(util.Contains(select_fields, "id") || util.Contains(select_fields, "*"))
		// 如果应该移除 "id" 列，则从每一行中移除它
		if removeIDColumn {
			for i := range books {
				delete(books[i], "id")
			}
		}
		// 获取行数和列数
		rowCount = len(books)
		if rowCount > 0 {
			colCount = len(books[0])
		}
		return books, rowCount, colCount, resultSites
	}
	return nil, 0, 0, nil
}

// 定义一个函数，根据共同的键合并两个 map 切片
func mergeMapsByKey(slice1, slice2 []map[string]interface{}, key string) []map[string]interface{} {
	merged := make([]map[string]interface{}, 0)

	// 如果其中一个切片为空，则直接返回另一个切片
	if len(slice1) == 0 {
		return slice2
	} else if len(slice2) == 0 {
		return slice1
	}

	// 创建一个 map 以存储基于键的记录
	records := make(map[interface{}]map[string]interface{})

	// 从第一个切片中填充记录
	for _, item := range slice1 {
		records[item[key]] = item
	}

	// 从第二个切片中更新或添加记录，并仅保留那些在第一个切片中存在的记录
	for _, item := range slice2 {
		if existing, ok := records[item[key]]; ok {
			// 如果键已经存在，则合并数据
			for k, v := range item {
				existing[k] = v
			}
			// 将该记录添加到结果中
			merged = append(merged, existing)
		}
	}

	return merged
}

// getColumns 获取列名
func getColumns(items []map[string]interface{}) []string {
	var columns []string
	for key := range items[0] {
		columns = append(columns, key)
	}
	return columns
}

// printRow 打印一行数据
func printRow(item map[string]interface{}, columns []string) {
	var values []string
	for _, column := range columns {
		values = append(values, fmt.Sprintf("%v", item[column]))
	}
	fmt.Println(strings.Join(values, "\t"))
}
