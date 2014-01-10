#!/bin/sh
#
# Copyright (c) 1999-2010 Luca Garulli
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
[ -f "$ORIENTDB_HOME"/lib/orientdb-tools-@VERSION@.jar ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

ORIENTDB_SETTINGS="-Dcache.level1.enabled=false -Dcache.level2.enabled=false -Djava.util.logging.config.file="$ORIENTDB_HOME/config/orientdb-client-log.properties" -Djava.awt.headless=true"
#JAVA_OPTS=-Xmx1024m

$JAVA -client $JAVA_OPTS $ORIENTDB_SETTINGS -Dfile.encoding=utf-8 -Dorientdb.build.number="@BUILD@" -cp "$ORIENTDB_HOME/lib/orientdb-tools-@VERSION@.jar:$ORIENTDB_HOME/lib/*" com.orientechnologies.orient.graph.console.OGremlinConsole $*
