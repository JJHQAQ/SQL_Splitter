package dbmanager

import (
	"SQL_Splitter/datatype"
	"SQL_Splitter/util"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func (dbmp *DBM) Select(sql_s string) {
	if util.Test {
		return
	}
	//Parse sql_s

	//if 水平分片
	items := dbmp.horizontal_fragmentation(sql_s)

	for _, x := range items {
		fmt.Println(x)
	}

	//if 垂直分片
	items = dbmp.vertical_fragmentation(sql_s)

	for _, x := range items {
		fmt.Println(x)
	}

}

func (dbmp *DBM) horizontal_fragmentation(sql_s string) []datatype.Book {
	var books []datatype.Book

	// for 每个分片
	rows, e := dbmp.Databases[0].Query("select * from book where id<=200025")
	if e == nil {
		// errors.New("query incur error")
	}
	for rows.Next() {
		var book datatype.Book
		e := rows.Scan(&book.Id, &book.Title, &book.Authors, &book.Publisher_id, &book.Copies)
		if e == nil {
			// fmt.Println(book)
			// json.Unmarshal(book)
			// jbyte, _ := json.Marshal(book)
			// fmt.Println(string(jbyte))
			books = append(books, book)
		} else {
			fmt.Println(e)
		}
	}
	rows.Close()

	rows, e = dbmp.Databases[1].Query("select * from book where id>200025 and id<200050")
	for rows.Next() {
		var book datatype.Book
		e := rows.Scan(&book.Id, &book.Title, &book.Authors, &book.Publisher_id, &book.Copies)
		if e == nil {
			// fmt.Println(book)
			// json.Unmarshal(book)
			// jbyte, _ := json.Marshal(book)
			// fmt.Println(string(jbyte))
			books = append(books, book)
		} else {
			fmt.Println(e)
		}
	}
	rows.Close()

	return books
}

func (dbmp *DBM) vertical_fragmentation(sql_s string) []datatype.Book {
	var books []datatype.Book
	return books
}
