#!/bin/sh
#
# Quick and dirty script to exercise all of the cli functions

set -e

TEST_DIR=/tmp/s3ts-test
rm -rf $TEST_DIR

export S3TS_LOCALCACHE=$TEST_DIR/cache

s3ts() {
    PYTHONPATH=src python src/s3ts/main.py "$@"
}

s3ts init
s3ts upload --dry-run src-1.0 src
s3ts upload --description "Some package" src-1.0 src
s3ts upload --verbose test test
s3ts prime-cache src
s3ts download --dry-run src-1.0
s3ts download src-1.0
s3ts download --verbose test
s3ts install src-1.0 $TEST_DIR/src-1.0
s3ts install --verbose test $TEST_DIR/test
s3ts verify-install test $TEST_DIR/test
s3ts list
s3ts info src-1.0
s3ts create-merged merge dir1:src-1.0 dir2:test
s3ts rename src-1.0 src-1.1
s3ts remove --yes src-1.1
s3ts flush --verbose --dry-run
s3ts flush --verbose
s3ts flush-cache --verbose test
