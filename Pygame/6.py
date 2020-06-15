import re
c = 0
n = ['67824']
while n[0].isdigit():
    c += 1
    with open("/Users/jin/Downloads/channel/{}.txt".format(n[0]),"r+") as f:
              r = f.readlines()
              n = re.findall(r'(\d+)',r[0])
    print(c, n[0])
        
