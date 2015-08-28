#!/bin/bash

export VER=`grep "<version>" pom.xml | head -n 1|awk -F "[<>]" '{print $3}'`

if [ -z "$VER" ]
then
    echo "ERROR: CANNOT FIND CURRENT ORIENTDB RELEASE!"
    exit
fi

echo "Building OrientDB $VER..."

mvn clean install -DskipTests=true -Dmaven.findbugs.enable=false -DlocalDeploy=true

DIR=distribution/target/orientdb-community-$VER-distribution.dir/orientdb-community-$VER/

echo "Releasing OrientDB $VER to $DIR..."

cp ../drivers/orientdb-jdbc/target/orientdb-jdbc-$VER.jar $DIR/lib/
cp ../modules/orientdb-lucene/target/orientdb-lucene-$VER-dist.jar $DIR/plugins/
cp ../modules/orientdb-etl/target/orientdb-etl-$VER.jar $DIR/lib/
cp ../modules/orientdb-etl/script/oetl.* $DIR/bin/

cd $DIR

if [ -n "$1" ]
then
    echo "Linking databases folder in $1..."
    rm -rf databases
    ln -s $1 databases
fi

echo "Switching to the fresh built OrientDB $VER"

