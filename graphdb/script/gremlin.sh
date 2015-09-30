#!/bin/bash

#set current working directory
cd `dirname $0`

case `uname` in
  CYGWIN*)
    CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /;/g')
    ;;
  *)
    CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
esac
#echo $CP

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

# Launch the application
if [ "$1" = "-e" ]; then
  k=$2
  if [ $# -gt 2 ]; then
    for (( i=3 ; i -lt $# + 1 ; i++ ))
    do
      eval a=\$$i
      k="$k \"$a\""
    done
  fi
  eval "$JAVA" $JAVA_OPTIONS -cp $CP:../plugins/*.jar com.tinkerpop.gremlin.groovy.jsr223.ScriptExecutor $k
else
  if [ "$1" = "-v" ]; then
    "$JAVA" -server $JAVA_OPTIONS -cp $CP:../plugins/*.jar com.tinkerpop.gremlin.Version
  else
    "$JAVA" -server $JAVA_OPTIONS -cp $CP:../plugins/*.jar com.tinkerpop.gremlin.groovy.console.Console
  fi
fi





# Return the program's exit code
exit $?