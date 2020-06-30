from PIL import Image 
im = Image.open('/Users/jin/Desktop/mozart.gif')
im_new = Image.new('RGB',(640, 480))#make a new image
w, h = im.size

im_rgb = im.convert("RGB")
p = []
purple = (255, 0, 255)

for i in range(h):
    p.append([im_rgb.getpixel((c, i)) for c in range(w)])
for i in range(h):
    pos = p[i].index(purple)
    p[i] = p[i][pos:] + p[i][:pos]
    for j in range(w):
        im_new.putpixel((j,i), p[i][j])
im_new.show()
