#coding:utf-8
#Author: Toryun
#Date: 2020-06-16 21:35:00
#Function: descript previous number
a = '111221'
k = 4
while k < 30:
    j = 0
    s = ''
    while j < len(a):
        c = 1
        while j < len(a)-1 and a[j] == a[j+1]:
            c += 1
            j += 1
        s = "{}{}{}".format(s, c, a[j])
        j += 1
    k += 1
    a = s
print len(a)
