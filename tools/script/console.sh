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
[ -f "$ORIENTDB_HOME"/lib/orientdb-tools-@VERSION@.jar ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

# Only set ORIENTDB_HOME if not already set correctly

java -client -Dorientdb.build.number=@BUILD@ -jar $ORIENTDB_HOME/lib/orientdb-tools-@VERSION@.jar $*
