#!/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

COLOR=${RED}
for TESTNAME in TupleTest TupleDescTest \
                CatalogTest \
                HeapPageIdTest RecordIdTest HeapPageReadTest \
                HeapFileReadTest
do
    ant runtest -Dtest=${TESTNAME}
    if [ $? == 0 ]
    then
        COLOR=${GREEN}
    else
        COLOR=${RED}
    fi
    printf "${COLOR} ${TESTNAME} unit test result\n"
    printf "${COLOR} ---------------------------------------------------------------------${NC}\n"
done

for TESTNAME in ScanTest
do
    ant runsystest -Dtest=${TESTNAME}
    if [ $? == 0 ]
    then
        COLOR=${GREEN}
    else
        COLOR=${RED}
    fi
    printf "${COLOR} ${TESTNAME} system test result\n"
    printf "${COLOR} ---------------------------------------------------------------------${NC}\n"
done
