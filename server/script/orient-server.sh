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

# Only set ORIENT_HOME if not already set
[ -f "$ORIENT_HOME"/lib/orient-database-server.jar ] || ORIENT_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENT_HOME

CONFIG_FILE=$ORIENT_HOME/config/orient-server-config.xml
LOG_LEVEL=warning
WWW_PATH=$ORIENT_HOME/www
#JAVA_OPTS=-Xms1024m -Xmx1024m

java -server $JAVA_OPTS -XX:+UseParallelGC -XX:+AggressiveOpts -XX:CompileThreshold=200 -Dorient.config.file="$CONFIG_FILE" -Dorient.www.path="$WWW_PATH" -Dorient.log.level=$LOG_LEVEL -jar $ORIENT_HOME/lib/orient-database-server.jar
