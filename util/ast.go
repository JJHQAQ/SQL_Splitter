package util

import (
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
		return nil, fmt.Errorf("Not a SELECT statement")
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
