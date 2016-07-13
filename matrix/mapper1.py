#!/usr/bin/python
import sys
for line in sys.stdin:
  data = line.strip().split(' ')
  flag = False

  if len(data)<2:
    continue

  for x in range(len(data)):
    if data[x] == "\"GET" or data[x] == "\"POST":
      flag = True

  ip = None
  jpg = None
  
  for i in data:
    if i[0] == '/':
      elem = i.split('/')
      if len(elem) == 0:
        continue
  # path = data[-4]
  # ip = data[0]
  # elem = path.strip().split('/')
  if elem[-1].find(".jpg") != -1 and flag == True:
    ip = data[0]
    jpg = elem[-1]
  if ip != None:
    print "%s\t%s" % (ip, jpg)
