# address
ETCD='127.0.0.1:20002'

# test flag
# TEST='-test'
TEST=''
CONF='config/'

go run main.go $TEST -etcd $ETCD -conf $CONF



