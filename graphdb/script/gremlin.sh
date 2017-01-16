#!/bin/sh

#set current working directory
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

case `uname` in
  CYGWIN*)
    CP=$( echo `dirname $PRG`/../lib/*.jar . | sed 's/ /;/g')
    ;;
  *)
    CP=$( echo `dirname $PRG`/../lib/*.jar . | sed 's/ /:/g')
esac

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-Xms32m -Xmx512m"
fi

ORIENTDB_SETTINGS="-XX:MaxDirectMemorySize=512g"

# Launch the application
if [ "$1" = "-e" ]; then
  k=$2
  if [ $# -gt 2 ]; then
    i=0 ;
    while [ "$i" -lt $# +1 ]
    do
        eval a=\$$i
        k="$k \"$a\""
        i=$(($i+1))
    done
  fi
    eval "$JAVA" $JAVA_OPTIONS $ORIENTDB_SETTINGS -cp $CP:../plugins/*.jar com.tinkerpop.gremlin.groovy.jsr223.ScriptExecutor $k
else
  if [ "$1" = "-v" ]; then
    "$JAVA" -server $JAVA_OPTIONS $ORIENTDB_SETTINGS -cp $CP:../plugins/*.jar com.tinkerpop.gremlin.Version
  else
    "$JAVA" -server $JAVA_OPTIONS $ORIENTDB_SETTINGS -cp $CP:../plugins/*.jar com.tinkerpop.gremlin.groovy.console.Console
  fi
fi





# Return the program's exit code
exit $?
