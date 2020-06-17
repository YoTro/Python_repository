
from datetime import date

year = [i for i in xrange(1006, 1997) if ((i%4 == 0 or i%400 == 0) and i%100 != 0) and str(i)[-1] == '6']

birthday = []

for i in year:
    if date(i, 1, 27).weekday() == 1:
        birthday.append(i)

print(birthday)
