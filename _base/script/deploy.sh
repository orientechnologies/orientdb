#!/bin/bash
if [ -z "$1" ]
then
    echo "SYNTAX ERROR, USE: deploy.sh <version> [<path-of-databases-to-link>]"
    exit
fi

echo "Building OrientDB $1..."

mvn clean install -DskipTests=true -Dmaven.findbugs.enable=false -DlocalDeploy=true

DIR=distribution/target/orientdb-community-$1-distribution.dir/orientdb-community-$1/

echo "Releasing OrientDB $1 to $DIR..."

cp ../drivers/orientdb-jdbc/target/orientdb-jdbc-$1.jar $DIR/lib/
cp ../modules/orientdb-lucene/target/orientdb-lucene-$1-dist.jar $DIR/plugins/
cp ../modules/orientdb-etl/target/orientdb-etl-$1.jar $DIR/lib/
cp ../modules/orientdb-etl/script/oetl.* $DIR/bin/

cd $DIR

if [ -n "$2" ]
then
    echo "Linking databases folder in $2..."
    rm -rf databases
    ln -s $2 databases
fi

echo "Switching to the fresh built OrientDB $1"

