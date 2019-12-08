
import datetime
import time
from urllib.request import urlopen
res = urlopen('http://just-the-time.appspot.com/')
result = res.read().strip()
result_str = result.decode('utf-8')
print(result_str)

x = result_str.split(" ")
x1 = x[0].split("-")
date = x1[1] + "/" + x1[2] + "/" + x1[0] + " " + x[1]

print(date)

timestamp = time.mktime(datetime.datetime.strptime(date, '%m/%d/%Y %H:%M:%S').timetuple())
print(timestamp)
print(type(timestamp))
#now = datetime.now()
#print(now)
#date_time = now.strftime('%m/%d/%Y %H:%M:%S')
#print(date_time)
#timestamp = datetime.timestamp(now)
#print(type(timestamp))
#import ntplib
#from time import ctime
#c = ntplib.NTPClient()
#response = c.request('europe.pool.ntp.org', version=3)
#print(ctime(response.tx_time))