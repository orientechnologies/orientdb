#!/bin/bash

export VER=`grep "<version>" pom.xml | head -n 1|awk -F "[<>]" '{print $3}'`

if [ -z "$VER" ]
then
    echo "ERROR: CANNOT FIND CURRENT ORIENTDB RELEASE!"
    exit
fi

echo "Building OrientDB $VER..."

mvn clean install -DskipTests=true -DlocalDeploy=true

DIR=distribution/target/orientdb-community-$VER.dir/orientdb-community-$VER/

echo "Releasing OrientDB $VER to $DIR..."

cd $DIR

if [ -n "$1" ]
then
    echo "Linking databases folder in $1..."
    rm -rf databases
    ln -s $1 databases
fi

echo "Switching to the fresh built OrientDB $VER"

