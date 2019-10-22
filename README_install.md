要求python3.6环境

```
cd root
git clone https://github.com/yzybackup/bitmex.git
cd bitmex
pip3 install -r requirements.txt
pip3 install .
```

安装talib
```
wget prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure --prefix=/usr
make
make install
```

pip3 install ta-lib

修改settings.py中的接口秘钥配置

然后启动服务：marketmaker

常驻启动方式：

```
crontab /root/bitmex/market_maker/bin/crontab
```
