#!/bin/sh

p=`ps aux | grep marketmaker | grep -v grep`
if [ -z "$p" ]; then
  cd /root/bitmex
  nohup python3 /root/bitmex/marketmaker > /dev/null &
fi
