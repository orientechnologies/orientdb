#!/bin/sh
#
# Copyright (c) OrientDB LTD (http://www.orientdb.com)
#
# HISTORY:
# 2012-07-31: Added -w option
#

# resolve links - $0 may be a softlink
PRG="$0"

if [ $# -gt 0 ]; then
  case "$1" in
    -w|--wait)
      wait="yes"
      shift 1 ;;
  esac
fi

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
[ -f "$ORIENTDB_HOME"/bin/server.sh ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
cd "$ORIENTDB_HOME/bin"

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

LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
JAVA_OPTS=-Djava.awt.headless=true

if [ -z "$ORIENTDB_PID" ] ; then
    ORIENTDB_PID=$ORIENTDB_HOME/bin/orient.pid
fi

PARAMS=$*

if [ -f "$ORIENTDB_PID" ] && [ "${#PARAMS}" -eq 0 ] ; then
    echo "pid file detected, killing process"
    kill -15 `cat "$ORIENTDB_PID"` >/dev/null 2>&1
    echo "waiting for OrientDB server to shutdown"
    while ps -p `cat $ORIENTDB_PID` > /dev/null; do sleep 1; done
    rm "$ORIENTDB_PID"
else
    echo "pid file not present or params detected"
    "$JAVA" -client $JAVA_OPTS -Dorientdb.config.file="$CONFIG_FILE" \
        -cp "$ORIENTDB_HOME/lib/orientdb-tools-@VERSION@.jar:$ORIENTDB_HOME/lib/*" \
        com.orientechnologies.orient.server.OServerShutdownMain $*

    if [ "x$wait" = "xyes" ] ; then
      echo "wait for OrientDB server to shutdown"

      while true ; do
        ps auxw | grep java | grep $ORIENTDB_HOME/lib/orientdb-server > /dev/null || break
        sleep 1;
      done
    fi
fi