#!/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

COLOR=${RED}
for TESTNAME in PredicateTest JoinPredicateTest FilterTest JoinTest \
                IntegerAggregatorTest StringAggregatorTest AggregateTest \
                HeapPageWriteTest HeapFileWriteTest BufferPoolWriteTest \
                InsertTest
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

for TESTNAME in FilterTest JoinTest \
                AggregateTest \
                InsertTest DeleteTest \
                EvictionTest
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
