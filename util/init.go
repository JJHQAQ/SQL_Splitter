package util

import (
	"flag"
)

// 实际中应该用更好的变量名
var (
	Test bool

	EtcdAddr  string
	Conf_path string
)

func Init() {
	flag.BoolVar(&Test, "test", false, "debug use")
	flag.StringVar(&EtcdAddr, "etcd", "127.0.0.1:20002", "set etcdaddr")
	flag.StringVar(&Conf_path, "conf", "./config/conf.json", "配置文件路径")

	flag.Parse()

}
