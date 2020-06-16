#coding: utf-8

from PIL import Image
p = '/Users/jin/Desktop/'
f = open('/Users/jin/Downloads/evil2.gfx', 'rb')
f_info = f.read()
f.close()
for i in range(5):
    with open(p+str(i)+'.jpg', 'wb') as fp:
        fp.write(f_info[i::5])

