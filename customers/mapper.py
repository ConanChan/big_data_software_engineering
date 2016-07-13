#!/usr/bin/python
import sys
for line in sys.stdin:
  data = line.strip().split('|')
  if len(data)<4:
	continue
  key=data[0]
  name=data[1]
  length = len(data[3])
  s = data[3][-length:-6] # get the year
  if len(data)==4 and int(s)<1996:
	print "%d\t%s\t%d" % (int(key), name, 1)
