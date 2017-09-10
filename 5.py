import urllib2,pprint
import cPickle as pickle
b=urllib2.urlopen('http://www.pythonchallenge.com/pc/def/banner.p')
result=pickle.Unpickler(b).load()
pprint.pprint(result)
output=open('c:\\Users\\Administrator\\Desktop\\5text.txt','w')
for line in result:
    print >> output, ' '.join([c[0]*[1] for c in line])
output.close()
