package util

import (
	"SQL_Splitter/datatype"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

/*
 * both the func Get_select_name and the func Predicates contain Parse module
 * maybe they can merge later...
 */
// get column name through sql
func Get_select_name(sql string) (selectedColumns []string, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}

	for _, expr := range selectStmt.SelectExprs {
		colName := Get_column_name(expr)
		if colName != "" {
			selectedColumns = append(selectedColumns, colName)
		}
	}
	return selectedColumns, err
}

// get the column name through the parsed sql statement
func Get_column_name(expr sqlparser.SelectExpr) string {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		if colName, ok := expr.Expr.(*sqlparser.ColName); ok {
			return colName.Name.String()
		}
	case *sqlparser.StarExpr:
		return "*"
	}
	return ""
}

// Determine if the slice contains target
func Contains(slice []string, target string) bool {
	for _, element := range slice {
		if element == target {
			return true
		}
	}
	return false
}

// get predicates through sql
func Predicates(sql string) []sqlparser.Expr {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Println("Error parsing SQL:", err)
		return nil
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		fmt.Println("Not a SELECT statement")
		return nil
	}

	var predicates []sqlparser.Expr
	if selectStmt.Where != nil {
		predicates = Get_predicates(selectStmt.Where.Expr)
	}
	return predicates
}

// get the predicates through the parsed sql statement
func Get_predicates(expr sqlparser.Expr) []sqlparser.Expr {
	var predicates []sqlparser.Expr
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		leftPredicates := Get_predicates(expr.Left)
		rightPredicates := Get_predicates(expr.Right)
		predicates = append(predicates, leftPredicates...)
		predicates = append(predicates, rightPredicates...)
	case *sqlparser.ComparisonExpr:
		predicates = append(predicates, expr)
	}
	return predicates
}

// Decomposes predicates into column names, operators, and values
func Extract_predicate_info(expr sqlparser.Expr) (string, string, string) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		column := sqlparser.String(expr.Left)
		operator := expr.Operator
		value := sqlparser.String(expr.Right)
		return column, operator, value
	default:
		return "", "", ""
	}
}

func Only_table(origin sqlparser.Expr, columns []string) sqlparser.Expr {

	switch expr := origin.(type) {
	case *sqlparser.AndExpr:
		// origin.(*sqlparser.AndExpr).Left = Only_table(expr.Left, columns)
		// origin.(*sqlparser.AndExpr).Right = Only_table(expr.Right, columns)
		expr.Left = Only_table(expr.Left, columns)
		expr.Right = Only_table(expr.Right, columns)
		if expr.Left == nil {
			return expr.Right
		}
		if expr.Right == nil {
			return expr.Left
		}
		return expr
	case *sqlparser.ComparisonExpr:
		cnt := 0
		if Contains(columns, sqlparser.String(expr.Left)) {
			cnt++
		}
		if Contains(columns, sqlparser.String(expr.Right)) {
			cnt++
		}
		if cnt == 0 {
			return nil
		} else {
			return expr
		}
	}
	return nil
}

func Table_filter(sql_s string, index int, columns []string) string {
	temp_stmt, _ := sqlparser.Parse(sql_s)
	temp_tree, _ := temp_stmt.(*sqlparser.Select)
	temp_tree.From = temp_tree.From[index : index+1]
	temp_tree.Where.Expr = Only_table(temp_tree.Where.Expr, columns)
	table_sql := sqlparser.String(temp_tree)
	return table_sql
}

func Only_one_table(origin sqlparser.Expr, columns []string) sqlparser.Expr {

	switch expr := origin.(type) {
	case *sqlparser.AndExpr:
		// origin.(*sqlparser.AndExpr).Left = Only_table(expr.Left, columns)
		// origin.(*sqlparser.AndExpr).Right = Only_table(expr.Right, columns)
		expr.Left = Only_one_table(expr.Left, columns)
		expr.Right = Only_one_table(expr.Right, columns)
		if expr.Left == nil {
			return expr.Right
		}
		if expr.Right == nil {
			return expr.Left
		}
		return expr
	case *sqlparser.ComparisonExpr:
		cnt := 0
		if Contains(columns, sqlparser.String(expr.Left)) {
			cnt++
		}
		if Contains(columns, sqlparser.String(expr.Right)) {
			cnt++
		}
		if cnt >= 2 {
			return nil
		} else {
			return expr
		}
	}
	return nil
}

func Join_fileter(sql_s string, tables map[string]datatype.Table) string {
	temp_stmt, _ := sqlparser.Parse(sql_s)
	temp_tree, _ := temp_stmt.(*sqlparser.Select)
	var columns []string
	for _, x := range tables {
		columns = append(columns, x.Columns...)
	}
	temp_tree.Where.Expr = Only_one_table(temp_tree.Where.Expr, columns)
	temp_tree.SelectExprs = All_expr()
	no_join_sql := sqlparser.String(temp_tree)
	return no_join_sql
}

var all_expr sqlparser.SelectExprs = nil

func All_expr() sqlparser.SelectExprs {
	if all_expr == nil {
		sql := "select * from book"
		temp_stmt, _ := sqlparser.Parse(sql)
		temp_tree, _ := temp_stmt.(*sqlparser.Select)
		all_expr = temp_tree.SelectExprs
	}
	return all_expr
}

// get insert table name
func Get_insert_table(sql string) (string, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}

	insertStmt, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return "", fmt.Errorf("not an INSERT statement")
	}

	return insertStmt.Table.Name.String(), nil
}
