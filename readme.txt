一、编译步骤：
1、安装cmake3和openssl-devel(1.1.1+)
yum install cmake3
yum install openssl-devel
手动安装openssl
./config
make && make install
echo "/usr/local/lib64/" >> /etc/ld.so.conf
ldconfig

2、./build下执行cmake3
打开md5校验：cmake3 .. -DMD5=ON
关闭md5校验：cmake3 .. -DMD5=OFF
启用TLS：    cmake3 .. -DTLS=ON
关闭TLS：    cmake3 .. -DTLS=OFF
同时打开：   cmake3 .. -DMD5=ON -DTLS=ON
指定版本号：  cmake3 .. -DVER=1.0
MD5和TLS默认都是是关闭

3、./build下执行make
make

4、生成的目标文件在./bin下

二、安装证书（启用TLS时需要安装证书）
1、生成证书和秘钥
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout mykey.pem -out mycert.pem
2、证书和秘钥放置
mykey.pem和mycert.pem放在与sgw相同目录下
mycert.pem放在与agent相同目录下