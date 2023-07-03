cd kubernetes
go build
./kubernetes > kube1.log &
./kubernetes > kube2.log &
./kubernetes > kube3.log &
cd ../git
go build
./git > gitout1.log &
./git > gitout2.log &
./git > gitout3.log &
cd ../remote-executor
go build
./remote-executor > remote1.log &
./remote-executor > remote2.log &
./remote-executor > remote3.log &