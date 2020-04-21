#!/bin/sh

# 记录一下开始时间
echo `date` >> /Users/jin/Documents/GitHub/Python_repository/bilibili/log &&
# 进入helloworld.py程序所在目录
cd /Users/jin/Documents/GitHub/Python_repository/bilibili &&
# 执行python脚本（注意前面要指定python运行环境/usr/bin/python，根据自己的情况改变）
python bilibili_ranking_data_everyday.py
#运行完成
echo 'finish' >> /Users/jin/Documents/GitHub/Python_repository/bilibili/log
