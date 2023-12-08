package datatype

type Any = interface{}

type Book struct {
	Id           int // 200001-290000 Without repetition
	Title        string
	Authors      string
	Publisher_id int
	Copies       int
}

type Customer struct {
	Id     int // 300000-315000 Without repetition
	Name   string
	Gender string // F:40%,M:60%
}

type Orders struct {
	Customer_id int // Customer.id
	Book_id     int // Book.id
	Quantity    int
}

type Publishers struct {
	Id    int
	Name  string
	State string // GA:49%,CA:51%
}

// 数据库表，包括表名、模式和关联的站点
type Table struct {
	Name    string   `json:"name"`
	Mode    string   `json:"mode"`
	Sites   []string `json:"sites"`
	Columns []string `json:"columns"`
}

// MySQL数据库的连接详情
type SqlAddress struct {
	Site_name string
	UserName  string
	Password  string
	Ip        string
	Port      string
	DbName    string
}
