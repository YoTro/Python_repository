import urllib2,pprint
import cPickle as pickle
b=urllib2.urlopen('http://www.pythonchallenge.com/pc/def/banner.p')#get该地址文件，b类型为实例（isinstant）
result=pickle.Unpickler(b).load()#创造一个unpickler文件反序列化成list
pprint.pprint(result)#打印result
output=open('c:\\Users\\Administrator\\Desktop\\5text.txt','w')
for line in result:
    print >> output, ' '.join([c[0]*c[1] for c in line])#元组（tuple)元素相乘转成字符串写入5text文件
output.close()#关闭文件

