#!/bin/bash

if [ -z "$1" ]
then
    echo "ERROR: syntax <branch>"
    exit
fi

echo "Switching all OrientDB projects to branch: $1"

echo 
echo "********************************"
echo " KERNEL module"
echo "********************************"

git checkout $1
git pull

cd ../modules

echo 
echo "********************************"
echo " LUCENE module"
echo "********************************"
cd orientdb-lucene
git checkout $1
git pull

echo 
echo "********************************"
echo " ETL module"
echo "********************************"
cd ../orientdb-etl
git checkout $1
git pull

cd ../../drivers

echo 
echo "********************************"
echo " JDBC module"
echo "********************************"
cd orientdb-jdbc
git checkout $1
git pull
