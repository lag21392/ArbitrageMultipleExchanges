#!/bin/bash

PATH=$PATH:$HOME/bin; export PATH
#/usr/bin/pgrep "node" -u "$(whoami)" | xargs kill >/dev/null
pkill -9 -f app.js
(cd /home/lag/Repos/api_1/; node app.js > output.log 2>&1 &)
#sleep 1m;
#/usr/bin/pgrep "python" -u "$(whoami)" | xargs kill >/dev/null
pkill -9 -f arbitrajeMultiple.py
(cd /home/lag/Repos/api_1/ArbitrajeMultiple/;api_1/bin/python arbitrajeMultiple.py > output.log 2>&1 &);