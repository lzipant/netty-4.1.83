
如果要使用native transport，那么需要安装动态库文件。该文件位于transport-native-epoll模块，通过下面命令来安装：
```shell
ln -s /root/projects/netty/netty-4.1.83/transport-native-epoll/target/native-build/target/lib/libnetty_transport_native_epoll_x86_64.so /usr/local/lib/libnetty_transport_native_epoll_x86_64.so
```
另外，加载该库文件时是在java.library.path变量所指向的目录中加载的，在openjdk8中，发现该变量没有包含/usr/local/lib，那么要么通过参数指定：
```shell
-Djava.library.path=/usr/local/lib
```
要么修改源码：在文件os_linux.cpp中，修改DEFAULT_LIBPATH变量的值。