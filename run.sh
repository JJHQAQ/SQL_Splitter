# address
ETCD='127.0.0.1:20002'

# test flag
TEST=true

go run main.go  \
    -test $TEST \
    -etcd $ETCD 

