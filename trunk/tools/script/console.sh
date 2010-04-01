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

# Only set CATALINA_HOME if not already set
[ -z "$ORIENT_HOME" ] && ORIENT_HOME=`cd "$PRGDIR/.." ; pwd`

"$JAVA_HOME/bin/java" -server -jar $ORIENT_HOME/lib/orient-database-tools.jar
