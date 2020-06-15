import re
import zipfile
'''
c = 0
n = ['67824']
while n[0].isdigit():
    c += 1
    with open("/Users/jin/Downloads/channel/{}.txt".format(n[0]),"r+") as f:
              r = f.readlines()
              n = re.findall(r'(\d+)',r[0])
    print(c, n[0])

 **************************************************************
****************************************************************
****************************************************************
**                                                            **
**   OO    OO    XX      YYYY    GG    GG  EEEEEE NN      NN  **
**   OO    OO  XXXXXX   YYYYYY   GG   GG   EEEEEE  NN    NN   **
**   OO    OO XXX  XXX YYY   YY  GG GG     EE       NN  NN    **
**   OOOOOOOO XX    XX YY        GGG       EEEEE     NNNN     **
**   OOOOOOOO XX    XX YY        GGG       EEEEE      NN      **
**   OO    OO XXX  XXX YYY   YY  GG GG     EE         NN      **
**   OO    OO  XXXXXX   YYYYYY   GG   GG   EEEEEE     NN      **
**   OO    OO    XX      YYYY    GG    GG  EEEEEE     NN      **
**                                                            **
****************************************************************
 **************************************************************
'''     
s = '/Users/jin/Downloads/channel.zip'
z = zipfile.ZipFile(s,"r")
n = '90052.txt'
convert = '90052'
comments = []
while convert.isdigit():
    #read the information in the archive
    info = z.read(n)
    #get comment from the txt
    comment0 = z.getinfo(n).comment
    #push to comments
    comments.append(comment0)
    #It need to translate byte to str to match numbers
    info=str(info).encode("utf-8")
    res = re.findall('\d', info)
    convert = ''.join(res)
    n = convert + '.txt'
    end = ''
    for c in comments:
        end += str(c).encode("utf-8")
    print(end)
