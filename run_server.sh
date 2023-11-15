#!/usr/bin/env bash

# compile lock
cd src/lockmgr
make

# compile server & client
cd ../transaction
make clean
make server
make client

# run server
make runregistry &
sleep 5  # 防止报错
make runtm & 
sleep 5
make runrmflights &
sleep 5
make runrmrooms &
sleep 5
make runrmcars &
sleep 5
make runrmcustomers &
sleep 5
make runwc &
