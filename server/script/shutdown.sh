#!/bin/sh
#
# Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)
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
export ORIENTDB_HOME
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
export JAVA

LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
LOG_LEVEL=warning
WWW_PATH=$ORIENTDB_HOME/www
JAVA_OPTS=-Djava.awt.headless=true

"$JAVA" -client $JAVA_OPTS -Dorientdb.config.file="$CONFIG_FILE" -cp "$ORIENTDB_HOME/lib/orientdb-tools-@VERSION@.jar:$ORIENTDB_HOME/lib/*" com.orientechnologies.orient.server.OServerShutdownMain $*

if [ "x$wait" = "xyes" ] ; then
  while true ; do
        ps auxw | grep java | grep $ORIENTDB_HOME/lib/orientdb-server > /dev/null || break
    sleep 1;
  done
fi
