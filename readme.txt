编译步骤：
1、安装cmake3和openssl-devel
yum install cmake3
yum install openssl-devel

2、./build下执行cmake3
打开md5校验：cmake3 .. -DMD5=ON
关闭md5校验：cmake3 .. -DMD5=OFF
默认是关闭： cmake3 ..

3、./build下执行make
make

4、生成的目标文件在./bin下