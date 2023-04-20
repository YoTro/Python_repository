# Boss爬虫
## 原理:根据RPC获取zp_stoken
!["RPC"](https://static.javatpoint.com/operating-system/images/what-is-rpc-in-operating-system4.png)
# How to Work
1. 把官网对应main.js文件用本项目的top文件夹覆盖掉
2. ```sh
cd ./zhipin
```
然后启动websocketserver.py文件
```py
python3 websocketserver.py
```
3. 刷新官网
4. 运行main.py文件
```py
python3 main.py
```
5. 自动保存zhipinjobs.csv到当前文件夹中
