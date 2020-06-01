一、编译步骤：
1、安装cmake3和openssl-devel
yum install cmake3
yum install openssl-devel

2、./build下执行cmake3
打开md5校验：cmake3 .. -DMD5=ON
关闭md5校验：cmake3 .. -DMD5=OFF
启用TLS：cmake3 .. -DTLS=ON
关闭TLS：cmake3 .. -DTLS=OFF
MD5和TLS默认都是是关闭

3、./build下执行make
make

4、生成的目标文件在./bin下

二、生成证书和私钥（启动TLS时服务器需要证书和私钥，因为是自签证书，需要把证书复制一份到对端）
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout mykey.pem -out mycert.pem