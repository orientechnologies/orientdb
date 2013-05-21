#!/bin/sh
#
# Copyright (c) 1999-2010 Luca Garulli
#

echo "           .                                              "
echo "          .\`        \`                                     "
echo "          ,      \`:.                                      "
echo "         \`,\`    ,:\`                                       "
echo "         .,.   :,,                                        "
echo "         .,,  ,,,                                         "
echo "    .    .,.:::::  \`\`\`\`                                   "
echo "    ,\`   .::,,,,::.,,,,,,\`;;                      .:      "
echo "    \`,.  ::,,,,,,,:.,,.\`  \`                       .:      "
echo "     ,,:,:,,,,,,,,::.   \`        \`         \`\`     .:      "
echo "      ,,:.,,,,,,,,,: \`::, ,,   ::,::\`   : :,::\`  ::::     "
echo "       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:      "
echo "        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:      "
echo "  \`     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:      "
echo "  \`,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:      "
echo "    .,,,,::,,,,,,,:  \`: , ,,  :     \`   :     :   .:      "
echo "      ...,::,,,,::.. \`:  .,,  :,    :   :     :   .:      "
echo "           ,::::,,,. \`:   ,,   :::::    :     :   .:      "
echo "           ,,:\` \`,,.                                      "
echo "          ,,,    .,\`                                      "
echo "         ,,.     \`,                     GRAPH-DB Server   "
echo "       \`\`        \`.                                       "
echo "                 \`\`                                       "
echo "                 \`                                        "

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

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

CONFIG_FILE=$ORIENTDB_HOME/config/orientdb-server-config.xml
LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
LOG_CONSOLE_LEVEL=info
LOG_FILE_LEVEL=fine
WWW_PATH=$ORIENTDB_HOME/www
ORIENTDB_SETTINGS="-Dprofiler.enabled=true -Dcache.level1.enabled=false -Dcache.level2.strategy=1"
JAVA_OPTS_SCRIPT="-XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true"

$JAVA -server $JAVA_OPTS $JAVA_OPTS_SCRIPT $ORIENTDB_SETTINGS -Dfile.encoding=UTF8 -Djava.util.logging.config.file="$LOG_FILE" -Dorientdb.config.file="$CONFIG_FILE" -Dorientdb.www.path="$WWW_PATH" -Dlog.console.level=$LOG_CONSOLE_LEVEL -Dlog.file.level=$LOG_FILE_LEVEL -Dorientdb.build.number="@BUILD@" -cp "$ORIENTDB_HOME/lib/*:" com.orientechnologies.orient.server.OServerMain
