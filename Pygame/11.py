from PIL import Image
import requests
f1 = "/Users/jin/Desktop/cave.jpg"
'''
r = requests.get('http://www.pythonchallenge.com/pc/return/cave.jpg')
with open(f1,'wb') as f:
   f.write(r.content)
'''
im = Image.open(f1)
odd = Image.new(im.mode, (im.size[0]/2, im.size[1]/2))
even = Image.new(im.mode, (im.size[0]/2, im.size[1]/2))
 
for x in range(1,im.size[0],2):
   for y in range(1,im.size[1],2):
       odd.putpixel(((x-1)/2,(y-1)/2),im.getpixel((x,y)))
 
for x in range(1,im.size[0],2):
   for y in range(1,im.size[1],2):
       even.putpixel((x/2,y/2),im.getpixel((x,y)))
 
odd.save('/Users/jin/Desktop/odd.jpg')
even.save('/Users/jin/Desktop/even.jpg')
