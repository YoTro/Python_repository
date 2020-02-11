import urllib2,re 
r=re.compile(r'.*?(\d+)$')  
nextnothing='12345'  
i=1
t=True
while t:
                try:

                        f=urllib2.urlopen('http://www.pythonchallenge.com/pc/def/linkedlist.php?nothing=%s'% nextnothing)
                        result=f.read()
                        f.close()
                        print i,result
                        nextnothing=r.search(result).group(1)
                        if  nextnothing=='16044':
                                nextnothing='8022'
                        if result=='peak.html':
                                t=False
                        i+=1
                except:
                        if result=='peak.html':
                                t=False
                        print 'error'
                      
