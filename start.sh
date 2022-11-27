#!/bin/bash

PATH=$PATH:$HOME/bin; export PATH; /usr/bin/pgrep "node" -u "$(whoami)" >/dev/null || (cd /home/lag/Repos/api_1/; node app.js > output.log 2>&1 &)
sleep 1m
(cd /home/lag/Repos/api_1/ArbitrajeMultiple/;api_1/bin/python arbitrajeMultiple.py > output.log 2>&1 &)