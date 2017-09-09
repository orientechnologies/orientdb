#!/bin/sh


#point to your orientDB installation: absolute path required
ORIENTDB_HOME="/opt/orientdb/orientdb-community-3.0.0m2/"

oetl_sh=$ORIENTDB_HOME/bin/oetl.sh

echo "removing previous database"

rm -rf ./openbeerdb/

csvs=( "beers.csv" "styles.csv"  "categories.csv" "breweries.csv" "geocodes.csv")

for i in "${csvs[@]}"
do
    if [ ! -f $i ]; then
        echo "-------- downloading $i ------------"

        wget https://github.com/brewdega/open-beer-database-dumps/raw/master/dumps/$i -O $i

        echo "-------- downloaded $i ------------"

    fi
done

configs=( "categories.json" "styles.json" "breweries.json" "beers.json" )

for i in "${configs[@]}"
do
    echo "\n-------- importing $i ------------\n"

    $oetl_sh $(pwd)/$i -databasePath=$(pwd) -sourceDirPath=$(pwd)

    echo "\n-------- imported $i ------------\n"
done

