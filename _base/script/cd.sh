#!/bin/bash

export VER=`grep "<version>" pom.xml | head -n 1|awk -F "[<>]" '{print $3}'`

if [ -z "$VER" ]
then
    echo "ERROR: CANNOT FIND CURRENT ORIENTDB RELEASE!"
    exit
fi

echo "Switching to the fresh built OrientDB $VER"

DIR=distribution/target/orientdb-community-$VER.dir/orientdb-community-$VER/

cd $DIR
