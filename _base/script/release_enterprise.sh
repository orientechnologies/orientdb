#!/bin/bash
if [ -z "$1" ]
then
    echo "SYNTAX ERROR, USE: release.sh <version>"
    exit
fi

echo "Releasing OrientDB Enterprise $1..."

cp -R distribution/target/orientdb-community-$1-distribution.dir/orientdb-community-$1/ distribution/target/orientdb-community-$1-distribution.dir/orientdb-enterprise-$1/

DIR=distribution/target/orientdb-community-$1-distribution.dir/orientdb-enterprise-$1/

#cp ../drivers/orientdb-jdbc/target/orientdb-jdbc-$1.jar $DIR/lib/

#cp ../modules/orientdb-lucene/target/orientdb-lucene-$1-dist.jar $DIR/plugins/

#cp ../modules/orientdb-etl/target/orientdb-etl-$1.jar $DIR/lib/
#cp ../modules/orientdb-etl/script/oetl.* $DIR/bin/

cp ../../orientdb-enterprise/agent/target/agent-$1.zip $DIR/plugins/

cd distribution/target/
rm orientdb-enterprise-$1.tar.gz 
rm orientdb-enterprise-$1.zip

cd orientdb-community-$1-distribution.dir
rm `find . -name ".DS_Store" -print`
rm `find . -name "*.wal" -print`

tar cvzf ../orientdb-enterprise-$1.tar.gz orientdb-enterprise-$1
zip -X -r -9 ../orientdb-enterprise-$1.zip orientdb-enterprise-$1
