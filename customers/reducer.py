#!/usr/bin/python
import sys

dic={}
s=None
for line in sys.stdin:
  data = line.strip().split('\t')
  if len(data) != 3:
  	continue
  thisKey,thisName,thisCount=data
  s = thisKey+'\t'+thisName
  if not s in dic.keys():
	dic[s] = 0
  dic[s] += 1
# lambda parameters: expression
# lambda expression equals an anonymous function
result=sorted(dic.items(), key=lambda s:s[1], reverse=True)
l = len(result)
for i in range(l):
  (key, value) = result[i]
  if int(value) >= 25:
	print "%s\t%d" % (key, int(value))
