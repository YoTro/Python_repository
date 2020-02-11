str="g fmnc wms bgblr rpylqjyrc gr zw fylb. rfyrq ufyr amknsrcpq ypc dmp. bmgle gr gl zw fylb gq glcddgagclr ylb rfyr'q ufw rfgq rcvr gq qm jmle. sqgle qrpgle.kyicrpylq() gq pcamkkclbcb. lmu ynnjw ml rfc spj."
key=[chr(i) for i in range(97,123)]#字典键值为a-z
value=[chr(i) for i in range(99,123)]
for i in range(97,99):
	value.append(chr(i))
#打包到一个字典里
z=dict(zip(key,value))
#添加字典
z[' ']=' '
z['(']='('
z[')']=')'
z['.']='.'
z['\'']='\''
t=False

u=list(str)
k=len(u)+1
for i in range(k):
        
        print z[u[i]],
        if z[u[i]]=='.':
                print '\n'
        if i==k:
                t=True#循环到最后一个字符退出循环
                       
        
                        

                        
        
                
                
        
