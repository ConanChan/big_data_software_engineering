#!/usr/bin/python

import sys

for line in sys.stdin:
  data = line.strip()
  # eval()将字符串str当成有效的表达式来求值
  
  # 并返回计算结果。
  js = eval(data)
  flag = 0
  if "primary-news-1.jpg" in js.keys():
    flag = 1
  for j in js:
    if j == "primary-news-1.jpg":
      continue
    print "%s\t%s" % (j, flag)
