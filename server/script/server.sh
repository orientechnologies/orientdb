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
echo "         ,,.     \`,                      S E R V E R        "
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

CONFIG_FILE=$ORIENTDB_HOME/config/orientdb-server-config.xml
LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
LOG_CONSOLE_LEVEL=info
LOG_FILE_LEVEL=fine
WWW_PATH=$ORIENTDB_HOME/www
#JAVA_OPTS=-Xms1024m -Xmx1024m

java -server $JAVA_OPTS -XX:+UseParallelGC -XX:+AggressiveOpts -XX:CompileThreshold=200 -Djava.util.logging.config.file="$LOG_FILE" -Dorientdb.config.file="$CONFIG_FILE" -Dorientdb.www.path="$WWW_PATH" -Dlog.console.level=$LOG_CONSOLE_LEVEL -Dlog.file.level=$LOG_FILE_LEVEL -Dorientdb.build.number=@BUILD@ -cp "$ORIENTDB_HOME/lib/*" com.orientechnologies.orient.server.OServerMain
