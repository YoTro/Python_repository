str="g fmnc wms bgblr rpylqjyrc gr zw fylb. rfyrq ufyr amknsrcpq ypc dmp. bmgle gr gl zw fylb gq glcddgagclr ylb rfyr'q ufw rfgq rcvr gq qm jmle. sqgle qrpgle.kyicrpylq() gq pcamkkclbcb. lmu ynnjw ml rfc spj."
key=[chr(i) for i in range(97,123)]#�ֵ��ֵΪa-z
value=[chr(i) for i in range(99,123)]
for i in range(97,99):
	value.append(chr(i))
#�����һ���ֵ���
z=dict(zip(key,value))
#����ֵ�
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
                t=True#ѭ�������һ���ַ��˳�ѭ��
                       
        
                        

                        
        
                
                
        
