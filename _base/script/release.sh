#!/bin/bash
if [ -z "$1" ]
then
    echo "SYNTAX ERROR, USE: release.sh <version>"
    exit
fi

echo "Releasing OrientDB $1..."

DIR=distribution/target/orientdb-community-$1-distribution.dir/orientdb-community-$1/

cp ../drivers/orientdb-jdbc/target/orientdb-jdbc-$1.jar $DIR/lib/

cp ../modules/orientdb-lucene/target/orientdb-lucene-$1-dist.jar $DIR/plugins/

cp ../modules/orientdb-etl/target/orientdb-etl-$1.jar $DIR/lib/
cp ../modules/orientdb-etl/script/oetl.* $DIR/bin/

cd distribution/target/orientdb-community-$1-distribution.dir

rm orientdb-community-$1-distribution.tar.gz 
tar cvzf ../orientdb-community-$1-distribution.tar.gz orientdb-community-2.0.3

rm orientdb-community-$1-distribution.zip
zip -X -r -9 ../orientdb-community-$1-distribution.zip orientdb-community-2.0.3
