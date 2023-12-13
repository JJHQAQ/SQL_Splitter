package dbmanager

import (
	"SQL_Splitter/util"
	"fmt"
	"reflect"
	"strconv"
)

// Only the and predicate is supported
func (dbmp *DBM) Delete(sql_s string) State {
	table_name, err := util.Get_delete_table(sql_s)
	if err != nil {
		return FAILED
	}
	predicates := util.Get_delete_predicates(sql_s)
	do_on_site1 := false
	do_on_site2 := false
	do_on_site3 := false
	do_on_site4 := false
	fmt.Println(table_name)
	if table_name == "book" {
		fmt.Println("do delete on book..")
		set := NewIntSet()
		set_end := NewIntSet()
		select_from_book1 := "SELECT id FROM book "
		select_from_book2 := "SELECT id FROM book "
		column_book1 := []string{"id", "title"}
		column_book2 := []string{"authors", "publisher_id", "copies"}
		select_book1 := false
		select_book2 := false
		predicates := util.Get_delete_predicates(sql_s)
		book1_has_predicate := false
		book2_has_predicate := false
		for _, predicate := range predicates {
			// Predicate triplet
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
		if select_book1 {
			rows1, err1 := dbmp.Databases["site1"].Query(select_from_book1)
			if err1 != nil {
				fmt.Println(err1)
				return FAILED
			}
			defer rows1.Close()
			for rows1.Next() {
				var id int
				if err := rows1.Scan(&id); err != nil {
					fmt.Println(err)
					return FAILED
				}
				set.Add(id)
				fmt.Println(id)
			}
		}
		if select_book2 {
			rows2, err := dbmp.Databases["site2"].Query(select_from_book2)
			if err != nil {
				fmt.Println(err)
				return FAILED
			}
			defer rows2.Close()
			for rows2.Next() {
				var id int
				if err := rows2.Scan(&id); err != nil {
					fmt.Println(err)
					return FAILED
				}
				if set.Contains(id) {
					set_end.Add(id)
				}
				fmt.Println(id)
			}
		} else {
			fmt.Println("cndknjkd ")
			fmt.Println(select_from_book2)
		}
		sql_new := "DELETE FROM book WHERE id in ("
		first := true
		for num := range set_end {
			num_str := strconv.Itoa(num)
			if first {
				first = false
				sql_new += num_str
			} else {
				sql_new += ("," + num_str)
			}
		}

		if len(set_end) > 0 {
			sql_new += ");"
			fmt.Println(sql_new)
			_, err3 := dbmp.Databases["site1"].Exec(sql_new)
			if err3 != nil {
				fmt.Println("Error executing DELETE statement: ", err3)
			}
			fmt.Println("Site involved: site1.")
			_, err4 := dbmp.Databases["site2"].Exec(sql_new)
			if err4 != nil {
				fmt.Println("Error executing DELETE statement: ", err4)
			}
			fmt.Println("Site involved: site2.")
		}
	} else if table_name == "customer" {
		has_primary := false
		for _, predicate := range predicates {

			// Predicate triplet
			column, operator, value := util.Extract_predicate_info(predicate)
			if column == "id" {
				has_primary = true
				value_num, err := strconv.Atoi(value)
				if err != nil {
					fmt.Println("The type of value does not match:", err)
					return FAILED
				}
				switch operator {
				case "=":
					if value_num <= 305000 {
						do_on_site1 = true
					} else if value_num > 31000 {
						do_on_site3 = true
					} else {
						do_on_site2 = true
					}
				case "!=":
					do_on_site1 = true
					do_on_site2 = true
					do_on_site3 = true
				case ">":
					if value_num < 305000 {
						do_on_site1 = true
						do_on_site2 = true
						do_on_site3 = true
					} else if value_num >= 310000 {
						do_on_site3 = true
					} else {
						do_on_site2 = true
						do_on_site3 = true
					}
				case "<":
					if value_num <= 305001 {
						do_on_site1 = true
					} else if value_num > 310001 {
						do_on_site1 = true
						do_on_site2 = true
						do_on_site3 = true
					} else {
						do_on_site1 = true
						do_on_site2 = true
					}
				case ">=":
					if value_num <= 305000 {
						do_on_site1 = true
						do_on_site2 = true
						do_on_site3 = true
					} else if value_num > 310000 {
						do_on_site3 = true
					} else {
						do_on_site2 = true
						do_on_site3 = true
					}
				case "<=":
					if value_num <= 305000 {
						do_on_site1 = true
					} else if value_num > 310000 {
						do_on_site1 = true
						do_on_site2 = true
						do_on_site3 = true
					} else {
						do_on_site1 = true
						do_on_site2 = true
					}
				}
				break
			}
		} // end for
		// The primary key determines which table to execute the sql on,if dont have primary key, do on all table
		if !has_primary {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
		}
	} else if table_name == "orders" {
		submeter_key1 := false
		submeter_key2 := false
		for _, predicate := range predicates {

			// Predicate triplet
			column, operator, value := util.Extract_predicate_info(predicate)
			if column == "customer_id" {
				submeter_key1 = true
				value_num, err := strconv.Atoi(value)
				if err != nil {
					fmt.Println("The type of value does not match:", err)
					return FAILED
				}
				do_on_site1, do_on_site2, do_on_site3, do_on_site4 = Get_do(307500, value_num, operator)
			} else if column == "book_id" {
				submeter_key2 = true
				value_num, err := strconv.Atoi(value)
				if err != nil {
					fmt.Println("The type of value does not match:", err)
					return FAILED
				}
				do_on_site1, do_on_site2, do_on_site3, do_on_site4 = Get_do(245000, value_num, operator)
			}
		} // end for
		// if dont have submeter key, do on all table
		if !submeter_key1 && !submeter_key2 {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
			do_on_site4 = true
		}
	} else if table_name == "publisher" {
		submeter_key1 := false
		submeter_key2 := false
		for _, predicate := range predicates {
			column, operator, value := util.Extract_predicate_info(predicate)
			if column == "id" {
				submeter_key1 = true
				value_num, err := strconv.Atoi(value)
				if err != nil {
					fmt.Println("The type of value does not match:", err)
					return FAILED
				}
				do_on_site1, do_on_site2, do_on_site3, do_on_site4 = Get_do(102500, value_num, operator)
			} else if column == "state" {
				t := reflect.TypeOf(value)
				if t.Kind() != reflect.String {
					fmt.Println("The value of field state is not of string type.")
					return FAILED
				}
				if operator != "==" && operator != "!=" {
					fmt.Println("Strings cannot be compared.")
					return FAILED
				}
				switch operator {
				case "=":
					if value == "CA" {
						do_on_site1 = true
						do_on_site3 = true
					} else if value == "GA" {
						do_on_site2 = true
						do_on_site4 = true
					} else {
						// do nothing
					}
				case "!=":
					if value == "CA" {
						do_on_site2 = true
						do_on_site4 = true
					} else if value == "GA" {
						do_on_site1 = true
						do_on_site3 = true
					} else {
						do_on_site1 = true
						do_on_site2 = true
						do_on_site3 = true
						do_on_site4 = true
					}
				} // end switch
			}
		} // end for
		if !submeter_key1 && !submeter_key2 {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
			do_on_site4 = true
		}
	} else {
		fmt.Println("The deleted data table does not exist!")
		return FAILED
	}
	if do_on_site1 {
		_, err := dbmp.Databases["site1"].Exec(sql_s)
		if err != nil {
			fmt.Println("Error executing DELETE statement: ", err)
		}
		fmt.Println("Site involved: site1.")
	}
	if do_on_site2 {
		_, err := dbmp.Databases["site2"].Exec(sql_s)
		if err != nil {
			fmt.Println("Error executing DELETE statement: ", err)
		}
		fmt.Println("Site involved: site2.")
	}
	if do_on_site3 {
		_, err := dbmp.Databases["site3"].Exec(sql_s)
		if err != nil {
			fmt.Println("Error executing DELETE statement: ", err)
		}
		fmt.Println("Site involved: site3.")
	}
	if do_on_site4 {
		_, err := dbmp.Databases["site4"].Exec(sql_s)
		if err != nil {
			fmt.Println("Error executing DELETE statement: ", err)
		}
		fmt.Println("Site involved: site4.")
	}
	return SUCCESS
}
func Get_do(value int, value_num int, operator string) (bool, bool, bool, bool) {
	do_on_site1 := false
	do_on_site2 := false
	do_on_site3 := false
	do_on_site4 := false
	switch operator {
	case "=":
		if value_num <= value {
			do_on_site1 = true
			do_on_site2 = true
		} else {
			do_on_site3 = true
			do_on_site4 = true
		}
	case "!=":
		do_on_site1 = true
		do_on_site2 = true
		do_on_site3 = true
		do_on_site4 = true
	case ">":
		if value_num < value {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
			do_on_site4 = true
		} else {
			do_on_site3 = true
			do_on_site4 = true
		}

	case "<":
		if value_num <= value+1 {
			do_on_site1 = true
			do_on_site2 = true
		} else {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
			do_on_site4 = true
		}
	case ">=":
		if value_num <= value {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
			do_on_site4 = true
		} else {
			do_on_site1 = true
			do_on_site2 = true

		}
	case "<=":
		if value_num <= value {
			do_on_site1 = true
			do_on_site2 = true
		} else {
			do_on_site1 = true
			do_on_site2 = true
			do_on_site3 = true
			do_on_site4 = true
		}

	} // end switch
	return do_on_site1, do_on_site2, do_on_site3, do_on_site4
}

type IntSet map[int]bool

func NewIntSet() IntSet {
	return make(map[int]bool)
}

func (set IntSet) Add(element int) {
	set[element] = true
}

func (set IntSet) Remove(element int) {
	delete(set, element)
}

func (set IntSet) Contains(element int) bool {
	_, exists := set[element]
	return exists
}
