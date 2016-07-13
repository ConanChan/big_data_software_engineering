#!/usr/bin/python
import sys
oldIp = None
dic = {}

for line in sys.stdin:
  data = line.strip().split('\t')
  if len(data) != 2:
    # something wrong
    continue
  ip,jpg = data
  # initialization
  if oldIp == None:
    oldIp = ip

  if ip == oldIp:
    dic[jpg] = 1
  else:
    print dic
    dic = {}
    oldIp = ip
    dic[jpg] = 1
#end loop
if oldIp != None:
  print dic
