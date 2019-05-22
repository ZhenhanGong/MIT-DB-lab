#!/bin/bash
GREEN='\033[0;32m'
NC='\033[0m' # No Color

if [ $1 == "--help" ]
then
    printf "./generate_dat.sh ${GREEN} convert file.txt  N ${NC} to convert file.txt to file.dat (N = #colums) \n"
    printf "./generate_dat.sh ${GREEN} print   file.dat  N ${NC} to print table layout (N = #colums) \n"
fi
java -jar dist/simpledb-lab1.jar $1 $2 $3
