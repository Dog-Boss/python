from datetime import datetime,date,time,timedelta
import re




now = datetime.combine(date.today(), time.min)

pattern = re.compile("auth.log.{}{:0>2}_(.*).gz".format(now.strftime('%Y%m'), 
	now.day - 2))
file_names = [
"auth.log.{}{:0>2}_{:0>2}.gz".format(now.strftime('%Y%m'), now.day-2, i)
for i in range(4)
]
file_names.append("auth.log.20150706_02.gz")
file_names.append("auth.log.20150706_05.gz")
file_names.append("auth.log.20150706_06.gz")
for file in file_names:
    if not re.search(pattern, file):
        continue
    else:
    	print(file)



td = datetime.fromtimestamp(1421077403)
now = datetime.combine(date.today(), time.min)
before = now - timedelta(hours=21)
after = now + timedelta(hours=3)
print(before)
print(after)
if after > td > before:
    print("yes")
else:
    print("no")