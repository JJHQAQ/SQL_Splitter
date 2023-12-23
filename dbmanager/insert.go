package dbmanager

import (
	"SQL_Splitter/util"
	"fmt"
	"strconv"
)

type State string

const (
	Defalut State = "DEFAULT"
	SUCCESS       = "SUCCESS"
	FAILED        = "FAILED"
)

func (dbmp *DBM) Insert(sql_s string) State {
	// fmt.Println("Do insert......")
	table_name, values, err := util.Get_insert_msg(sql_s)
	if err != nil {
		fmt.Println("Error:", err)
		return FAILED
	}
	table_field_num := map[string]int{
		"book":      5,
		"customer":  3,
		"orders":    3,
		"publisher": 3,
	}
	cell_num := len(values) // num of field
	if cell_num != table_field_num[table_name] {
		fmt.Println("The number of value does not match !")
		return FAILED
	}
	// fmt.Println("Match table......")
	if table_name == "book" {
		id := values[0]
		title := values[1]
		authors := values[2]
		publishers_id := values[3]
		select_from_publisher := "select * from publisher where id = "
		select_from_publisher += publishers_id
		id_num1, err := strconv.Atoi(publishers_id)
		if err != nil {
			fmt.Println("The type of value does not match:", err)
			return FAILED
		}
		if id_num1 <= 102500 {
			rows1, err1 := dbmp.Databases["site1"].Query(select_from_publisher)
			if err1 != nil {
				fmt.Println(err1)
				return FAILED
			}
			rows2, err2 := dbmp.Databases["site2"].Query(select_from_publisher)
			if err2 != nil {
				fmt.Println(err2)
				return FAILED
			}
			if !rows1.Next() && !rows2.Next() {
				fmt.Println("Violation of referential integrity constraint.")
				return FAILED
			}
		} else {
			rows3, err3 := dbmp.Databases["site3"].Query(select_from_publisher)
			if err3 != nil {
				fmt.Println(err3)
				return FAILED
			}
			rows4, err4 := dbmp.Databases["site4"].Query(select_from_publisher)
			if err4 != nil {
				fmt.Println(err4)
				return FAILED
			}
			if !rows3.Next() && !rows4.Next() {
				fmt.Println("Violation of referential integrity constraint.")
				return FAILED
			}
		}
		copies := values[4]
		insert_stmt1 := "INSERT INTO book VALUES("
		insert_stmt2 := "INSERT INTO book VALUES("
		insert_stmt1 += (id + "," + title + ")")
		insert_stmt2 += (id + "," + authors + "," + publishers_id + "," + copies + ")")
		// fmt.Println(insert_stmt1)
		// fmt.Println(insert_stmt2)
		_, err1 := dbmp.Databases["site1"].Exec(insert_stmt1)
		util.Handle_err(err1)
		fmt.Println("Site involved: site1.")
		_, err2 := dbmp.Databases["site2"].Exec(insert_stmt2)
		util.Handle_err(err2)
		fmt.Println("Site involved: site2.")
	} else if table_name == "customer" {
		id := values[0]
		id_num, err := strconv.Atoi(id)
		if err != nil {
			fmt.Println("The type of value does not match:", err)
			return FAILED
		}
		if id_num <= 305000 {
			_, err := dbmp.Databases["site1"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site1.")
		} else if id_num > 310000 {
			// fmt.Println("id_num > 310000!!!!! site3")
			_, err := dbmp.Databases["site3"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site3.")
		} else {
			_, err := dbmp.Databases["site2"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site2.")
		}
	} else if table_name == "orders" {
		customer_id := values[0]
		book_id := values[1]
		customer_id_num, err := strconv.Atoi(customer_id)
		if err != nil {
			fmt.Println("The type of value does not match:", err)
			return FAILED
		}
		book_id_num, err := strconv.Atoi(book_id)
		if err != nil {
			fmt.Println("The type of value does not match:", err)
			return FAILED
		}
		if customer_id_num <= 307500 && book_id_num <= 245000 {
			_, err := dbmp.Databases["site1"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site1.")
		} else if customer_id_num <= 307500 && book_id_num > 245000 {
			_, err := dbmp.Databases["site2"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site2.")
		} else if customer_id_num > 307500 && book_id_num <= 245000 {
			_, err := dbmp.Databases["site3"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site3.")
		} else {
			_, err := dbmp.Databases["site4"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site4.")
		}

	} else if table_name == "publisher" {
		id := values[0]
		state := values[2]
		id_num, err := strconv.Atoi(id)
		if err != nil {
			fmt.Println("The type of value does not match:", err)
			return FAILED
		}
		if id_num <= 102500 && state == "CA" {
			_, err := dbmp.Databases["site1"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site1.")
		} else if id_num <= 102500 && state == "GA" {
			_, err := dbmp.Databases["site2"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site2.")
		} else if id_num > 102500 && state == "GA" {
			_, err := dbmp.Databases["site3"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site3.")
		} else {
			_, err := dbmp.Databases["site4"].Exec(sql_s)
			util.Handle_err(err)
			fmt.Println("Site involved: site4.")
		}
	} else {
		fmt.Println("The inserted data table does not exist!")
		return FAILED
	}
	return SUCCESS
}
