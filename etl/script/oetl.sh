#!/bin/sh
#
# Copyright (c) 2014 Luca Garulli
#

#set current working directory
cd `dirname $0`

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set ORIENTDB_HOME if not already set
[ -f "$ORIENTDB_HOME"/lib/orientdb-etl-@VERSION@.jar ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

if [ -z "$ORIENTDB_ETL_LOG_CONF" ] ; then
    ORIENTDB_ETL_LOG_CONF=$ORIENTDB_HOME/config/orientdb-etl-log.properties
fi


# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA


if [ -z "$ORIENTDB_OPTS_MEMORY" ] ; then
    ORIENTDB_OPTS_MEMORY="-Xms2G -Xmx2G"
fi

if [ -z "$JAVA_OPTS_SCRIPT" ] ; then
    JAVA_OPTS_SCRIPT="-Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8 -Drhino.opt.level=9"
fi

# ORIENTDB SETTINGS LIKE DISKCACHE, ETC
if [ -z "$ORIENTDB_SETTINGS" ]; then
    ORIENTDB_SETTINGS="" # HERE YOU CAN PUT YOUR DEFAULT SETTINGS
fi


ORIENTDB_SETTINGS="-Djava.awt.headless=true"
JAVA_OPTS=-Xmx512m
KEYSTORE=$ORIENTDB_HOME/config/cert/orientdb-console.ks
KEYSTORE_PASS=password
TRUSTSTORE=$ORIENTDB_HOME/config/cert/orientdb-console.ts
TRUSTSTORE_PASS=password
SSL_OPTS="-Dclient.ssl.enabled=false -Djavax.net.ssl.keyStore=$KEYSTORE -Djavax.net.ssl.keyStorePassword=$KEYSTORE_PASS -Djavax.net.ssl.trustStore=$TRUSTSTORE -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASS"

exec "$JAVA" $JAVA_OPTS \
    $ORIENTDB_OPTS_MEMORY \
    $JAVA_OPTS_SCRIPT \
    $ORIENTDB_SETTINGS \
    $SSL_OPTS \
    -Djava.util.logging.config.file="$ORIENTDB_ETL_LOG_CONF" \
    -Dfile.encoding=utf-8 -Dorientdb.build.number="@BUILD@" \
    -cp "$ORIENTDB_HOME/lib/*:$ORIENTDB_HOME/plugins/*" com.orientechnologies.orient.etl.OETLProcessor $*