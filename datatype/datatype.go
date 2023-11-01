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
