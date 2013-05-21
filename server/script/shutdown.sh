#!/bin/bash
#
# Copyright (c) 1999-2010 Luca Garulli
#
# 2012-07-31 - Added -w option 
#
# resolve links - $0 may be a softlink
PRG="$0"

while [ $# -gt 0 ]; do
  case "$1" in
    -w|--wait)
      wait="yes"
      shift 1 ;;
  esac
done

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
[ -f "$ORIENTDB_HOME"/bin/orient.sh ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

if [ ! -f "${CONFIG_FILE}" ]
then
  CONFIG_FILE=$ORIENTDB_HOME/config/orientdb-server-config.xml
fi

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
LOG_LEVEL=warning
WWW_PATH=$ORIENTDB_HOME/www
JAVA_OPTS=-Djava.awt.headless=true

$JAVA -client $JAVA_OPTS -Dorientdb.config.file="$CONFIG_FILE" -cp "$ORIENTDB_HOME/lib/orientdb-tools-@VERSION@.jar:$ORIENTDB_HOME/lib/*" com.orientechnologies.orient.server.OServerShutdownMain $*

if [ "x$wait" = "xyes" ] ; then
  while true ; do
    ps -ef | grep java | grep $ORIENTDB_HOME/lib/orientdb-server > /dev/null || break
    sleep 1;
  done
fi