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

# run test
cd ../cs223test
# rm test log
rm results/* -rf
rm data/* -rf
export CLASSPATH=.:gnujaxp.jar
javac RunTests.java
java -DrmiPort=8080 RunTests MASTER.xml
