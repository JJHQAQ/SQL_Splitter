package main

import (
	"SQL_Splitter/dbmanager"
	"SQL_Splitter/util"
	"fmt"

	// "github.com/marianogappa/sqlparser"
	// "github.com/xwb1989/sqlparser"
	"github.com/zztroot/color"
)

func Init() {
	fmt.Println(color.Coat("SQL_Splitter running....", color.Green))
}

func main() {
	util.Init()
	dbm := dbmanager.New_DBM()

	Init()

	for {
		//Input the SQL
		//1.水平查询
		//1.1基本测试
		SQL_s := "select * from customer" //行数：15000 列数：3 站点：[site1 site2 site3]
		// SQL_s := "select * from orders" //行数：100000 列数：3 站点：[site1 site2 site3 site4]
		// SQL_s := "select * from publisher" //行数：5000	列数：3 站点：[site1 site2 site3 site4]
		//1.2条件测试
		// SQL_s := "select * from customer where id <= 310000" //行数：10000 列数：3 站点：[site1 site2]
		// SQL_s := "select * from orders where customer_id > 307500" //行数：49808 列数：3 站点：[site3 site4]
		// SQL_s := "select * from publisher where state = \"GA\"" //行数：2474 列数：3 站点：[site2 site4]
		//1.3并操作测试
		// SQL_s := "select gender from customer" //行数：15000 列数：1 站点：[site1 site2 site3]
		// SQL_s := "select book_id,quantity from orders" //行数：100000 列数：2 站点：[site1 site2 site3 site4]
		// SQL_s := "select state from publisher" //行数：5000 列数：1 站点：[site1 site2 site3 site4]

		//2.垂直查询（目前的条件查询仅支持and连接符，其余没问题）
		// SQL_s := "select * from book" //行数：90000 列数：5 站点：[site1 site2]
		// SQL_s := "select title from book" //行数：90000 列数：1 站点：[site1]
		// SQL_s := "select * from book where  copies>3000 and publisher_id=101085" //行数：16 列数：5 站点：[site1 site2]
		//3.连接测试
		// SQL_s := "select * from book,publisher where book.publisher_id = publisher.id and publisher.name= 'Twomorrows Publishing' and book.copies>6000"
		// SQL_s := "select orders.book_id,customer.id,orders.customer_id from customer,orders where orders.customer_id = customer.id and customer.id=310119 and orders.customer_id=310119"
		// dbm.Do(SQL_s)
		//4.删除测试
		// SQL_s := "delete from book where title='Maria\\'s Diary (Plus S.)' and copies = 5991;"
		// SQL_s := "delete from publisher where id=104378"

		dbm.Do(SQL_s)
		// dbm.DoMany("./demo/test.sql")

		fmt.Println("Success:!!!")
		break

	}

}
