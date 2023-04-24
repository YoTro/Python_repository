# Boss爬虫
## 原理:根据RPC获取zp_stoken
!["RPC"](RPC.png)

## 为什么使用RPC抓取

js逆向破解难点: 每请求4次数据, `__zp_stoken__`就会刷新一次, js页面补环境要补10000行代码

selenium自动化破解难点:非常容易被识别

RPC优点:无需知道具体加密过程, 不用补环境, 被检测的概率最低

缺点:请求150+时要求验证码识别, 暂时未破解geetest,需要搜集图片标框后进行yolo训练

# 参数说明
```js
scene = "1"#场景
queryjob = "亚马逊运营"#岗位关键词
city = "深圳"#城市
experience = ""#工作经验
payType = ""#工资结算周期
partTime = ""#兼职时间
degree = ""#学历要求
industry = ""#公司行业
scale = ""#公司规模
stage = ""#融资阶段
position = ""#职位类型
jobType = ""#求职类型(全职,兼职)
salary = ""#薪资待遇
multiBusinessDistrict = "440307"#区,县
multiSubway = ""#地铁线与站点
page = 1#页数
pageSize = 30#默认一页最多30条招聘信息
````
# 文件结构

```html
├── README.md
├── main.py
├── requirements.txt
├── top
│   └── static.zhipin.com
│       └── zhipin-geek
│           └── v648
│               └── web
│                   └── geek
│                       └── js
│                           └── main.js
├── websocketclient.py
├── websocketserver.py
├── zhipin.py
└── zp_stoken.txt
```
1. `websocketclient.py`websocket客户端定义发送与接收信息
2. `websocketserver.py`websocket服务端接收`__zp_stoken__`
3. `zhipin.py`定义了初始化,搜索和保存函数
4. `main.py`定义所有搜索参数并运行文件
5. `top`需要注入js文件的文件夹, 路径与官网必须一致,其中文件夹名为v648每天会变

# How to Work

二选一部署

## 本地部署

1. 安装依赖库
```sh
pip3 install -r requirements.txt
```
2. 把官网对应main.js文件用本项目的top文件夹覆盖掉, 文件夹路径必须与官网一致, 持久化存储即使关机重启依然存在
```sh
F12打开谷歌工具 > 点击source > 点击page左边>>的Overrides > 勾选Enable Local Overrides > 点击+Select folder for overrides
```
3. 
```sh
cd ./zhipin
```
然后启动websocketserver.py文件
```python
python3 websocketserver.py
```
4. 刷新官网
5. 运行main.py文件
```python
python3 main.py
```
6. 自动保存zhipinjobs.csv到本地的当前文件夹中

## 远程服务器部署

1. 登陆远程Linux服务器,把整个zhipin文件夹下载下来
2. 安装Nginx
```sh
sudo apt-get update
sudo apt-get install nginx
```
3. 配置NGINX
```sh
sudo vim /etc/nginx/nginx.conf
```
“your.domain.com”应替换为您的实际域名，“/path/to/your/certificate.pem”和“/path/to/your/privatekey.pem”应替换为您的SSL证书和私钥的实际路径，如果你尚未配置ssl,请使用letsencrypt配置免费的ssl
“/websocket”是您的WebSocket应用程序的路径
```js
        server {
            listen 443 ssl;
            listen [::]:443 ssl;
		    server_name your.domain.com;

		    ssl_certificate /path/to/your/certificate.pem;
		    ssl_certificate_key /path/to/your/privatekey.pem;
            location /websocket {
                proxy_pass http://localhost:8765/websocket;
                proxy_http_version 1.1;
                proxy_set_header Upgrade $http_upgrade;
                proxy_set_header Connection "Upgrade";
                proxy_set_header Host $host;
            }
            location /zp_stoken {
                proxy_pass http://0.0.0.0:8080/zp_stoken;
                proxy_set_header Host $host;
                proxy_set_header X-Real-IP $remote_addr;
                proxy_set_header X-Forwarded-Proto $scheme;
                proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            }
        }
```
重置NGINX
```sh
sudo systemctl reload nginx
```

4. 安装依赖库
```sh
pip3 install -r requirements.txt
```
5. 修改top文件夹中的main.js第10025行代码, 协议必须是wss否则会报错
```js
var socket = new WebSocket("wss://www.yourdomain.com/websocket");
```
```js
DOMException: Failed to construct 'WebSocket': An insecure WebSocket connection may not be initiated from a page loaded over HTTPS.
```
6. 把官网对应main.js文件用本项目的top文件夹覆盖掉, 文件夹路径必须与官网一致, 持久化存储即使关机重启依然存在
```sh
F12打开谷歌工具 > 点击source > 点击page左边>>的Overrides > 勾选Enable Local Overrides > 点击+Select folder for overrides
```
7. 启动app.py和websocketserver.py
```sh
cd ./zhipin
```
```sh
nohup python3 websocketserver.py & nohup python3 app.py 
```
8. 刷新官网
9. 全局搜索get_zp_stoken函数, 填入你的zp_stoken远程服务器获取接口URL
```python
get_zp_stoken(url = "https://www.yourdomain.com/zp_stoken")
```
10. 在本地下载zhipin文件夹的所有文件然后运行main.py文件
```python
python3 main.py
```
11. 自动保存zhipinjobs.csv到本地的当前文件夹中或者远程服务器的当前文件夹中

# TODO
- [ ] 验证码破解
