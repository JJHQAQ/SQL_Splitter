package util

import (
	"flag"

	"github.com/marianogappa/sqlparser/query"
)

// 实际中应该用更好的变量名
var (
	Test bool

	// v, V bool
	// t, T bool
	// q    *bool

	// s string
	EtcdAddr string
	// c string
	// g string
)

func init() {
	flag.BoolVar(&Test, "test", false, "debug use")

	// flag.BoolVar(&v, "v", false, "show version and exit")
	// flag.BoolVar(&V, "V", false, "show version and configure options then exit")

	// flag.BoolVar(&t, "t", false, "test configuration and exit")
	// flag.BoolVar(&T, "T", false, "test configuration, dump it and exit")

	// 另一种绑定方式
	// q = flag.Bool("q", false, "suppress non-error messages during configuration testing")

	// 注意 `signal`。默认是 -s string，有了 `signal` 之后，变为 -s signal
	// flag.StringVar(&s, "s", "", "send `signal` to a master process: stop, quit, reopen, reload")
	flag.StringVar(&EtcdAddr, "etcd", "127.0.0.1:20002", "set etcdaddr")
	// flag.StringVar(&c, "c", "conf/nginx.conf", "set configuration `file`")
	// flag.StringVar(&g, "g", "conf/nginx.conf", "set global `directives` out of configuration file")

	flag.Parse()
	// 改变默认的 Usage，flag包中的Usage 其实是一个函数类型。这里是覆盖默认函数实现，具体见后面Usage部分的分析
	// flag.Usage = usage
}

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
