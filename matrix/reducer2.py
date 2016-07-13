#!/usr/bin/python

import sys
oldIp = None
count1 = 0
count2 = 0
jps = {}

for line in sys.stdin:
  data = line.strip().split('\t')
  if len(data) != 2:
    # something wrong
    continue

  ip, jj=data
  # force type transfer
  jj = int(jj)
# initialization
  if oldIp == None:
    oldIp = ip

  if ip == oldIp:
    count1 += 1
    count2 += jj
  else:
    if count2 != 0:
      print oldIp, '\t', float(count2) / count1
    oldIp = ip
    count1 = 1
    count2 = jj
# judge if it is none
if oldIp != None and count2 != 0:
  print oldIp, '\t', float(count2) / count1
