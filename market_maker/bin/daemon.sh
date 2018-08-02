#!/bin/sh

p=`ps aux | grep marketmaker | grep -v grep`
if [ -z "$p" ]; then
  cd /home/py3
  nohup /home/py3/bin/marketmaker > /dev/null &
fi