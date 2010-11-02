#!/bin/sh
#
# Copyright (c) 1999-2010 Luca Garulli
#

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
[ -f "$ORIENTDB_HOME"/bin/orient.sh ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

CONFIG_FILE=$ORIENTDB_HOME/config/orientdb-server-config.xml
LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
LOG_LEVEL=warning
WWW_PATH=$ORIENTDB_HOME/www
#JAVA_OPTS=-Xms1024m -Xmx1024m

java -client $JAVA_OPTS -Dorientdb.config.file="$CONFIG_FILE" -cp "$ORIENTDB_HOME/lib/orientdb-server-@VERSION@.jar" com.orientechnologies.orient.server.OServerShutdownMain $*
