package datatype

type Any = interface{}

type Book struct {
	Id           int
	Title        string
	Authors      string
	Publisher_id int
	Copies       int
}

type Customer struct {
	Id     int
	Name   string
	Gender string
}
