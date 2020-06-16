import requests
from PIL import Image
url = 'http://www.pythonchallenge.com/pc/def/oxygen.png'
r = requests.get(url)
f1 = '/Users/jin/Desktop/oxygen.png'
with open(f1,'wb') as f:
    f.write(r.content)
    f.close()

im = Image.open(f1)
s = ''
for i in range(0, im.size[0], 7):
    if im.getpixel((i,47))[0] == im.getpixel((i,47))[1] and im.getpixel((i,47))[0] == im.getpixel((i,47))[2]:
        s += ''.join(chr(im.getpixel((i,47))[0]))
print s
