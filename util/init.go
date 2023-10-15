package util

import (
	"github.com/marianogappa/sqlparser/query"
)

func Init_DB() {
	return
}

// TOBE delete
func RecoverQ(q query.Query) string {
	var ret = ""
	if q.Type == query.Select {
		ret += "Select "
	}
	for _, f := range q.Fields {
		ret += f
		ret += " "
	}
	ret += "From "
	// for _, f := range q.TableName {
	// 	ret +=
	// 	ret += " "
	// }
	return ret
}
