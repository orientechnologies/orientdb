#!/bin/bash
if [ -z "$1" ]
then
    echo "SYNTAX ERROR, USE: cd.sh <version>"
    exit
fi

echo "Switching to the fresh built OrientDB $1"

DIR=distribution/target/orientdb-community-$1-distribution.dir/orientdb-community-$1/

cd $DIR
