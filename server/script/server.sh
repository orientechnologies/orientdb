#!/bin/sh
#
# Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)
#

echo "           .                                          "
echo "          .\`        \`                                 "
echo "          ,      \`:.                                  "
echo "         \`,\`    ,:\`                                   "
echo "         .,.   :,,                                    "
echo "         .,,  ,,,                                     "
echo "    .    .,.:::::  \`\`\`\`                                 :::::::::     :::::::::   "
echo "    ,\`   .::,,,,::.,,,,,,\`;;                      .:    ::::::::::    :::    :::  "
echo "    \`,.  ::,,,,,,,:.,,.\`  \`                       .:    :::      :::  :::     ::: "
echo "     ,,:,:,,,,,,,,::.   \`        \`         \`\`     .:    :::      :::  :::     ::: "
echo "      ,,:.,,,,,,,,,: \`::, ,,   ::,::\`   : :,::\`  ::::   :::      :::  :::    :::  "
echo "       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:    :::      :::  :::::::     "
echo "        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:    :::      :::  :::::::::   "
echo "  \`     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:    :::      :::  :::     ::: "
echo "  \`,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:    :::      :::  :::     ::: "
echo "    .,,,,::,,,,,,,:  \`: , ,,  :     \`   :     :   .:    :::      :::  :::     ::: "
echo "      ...,::,,,,::.. \`:  .,,  :,    :   :     :   .:    :::::::::::   :::     ::: "
echo "           ,::::,,,. \`:   ,,   :::::    :     :   .:    :::::::::     ::::::::::  "
echo "           ,,:\` \`,,.                                  "
echo "          ,,,    .,\`                                  "
echo "         ,,.     \`,                                          GRAPH DATABASE  "
echo "       \`\`        \`.                                                          "
echo "                 \`\`                                         www.orientdb.org "
echo "                 \`                                    "

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
[ -f "$ORIENTDB_HOME"/bin/orient.sh ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

if [ ! -f "${CONFIG_FILE}" ]
then
  CONFIG_FILE=$ORIENTDB_HOME/config/orientdb-server-config.xml
fi

# Raspberry Pi check (Java VM does not run with -server argument on ARMv6)
if [ `uname -m` != "armv6l" ]; then
  JAVA_OPTS="$JAVA_OPTS -server "
fi
export JAVA_OPTS

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
WWW_PATH=$ORIENTDB_HOME/www
ORIENTDB_SETTINGS="-Dprofiler.enabled=true"
JAVA_OPTS_SCRIPT="-Xmx512m -Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8 -Drhino.opt.level=9"

"$JAVA" $JAVA_OPTS $JAVA_OPTS_SCRIPT $ORIENTDB_SETTINGS -Djava.util.logging.config.file="$LOG_FILE" -Dorientdb.config.file="$CONFIG_FILE" -Dorientdb.www.path="$WWW_PATH" -Dorientdb.build.number="@BUILD@" -cp "$ORIENTDB_HOME/lib/orientdb-server-@VERSION@.jar:$ORIENTDB_HOME/lib/*" $* com.orientechnologies.orient.server.OServerMain
