#!/bin/sh
#
# Copyright (c) OrientDB LTD (http://orientdb.com/)
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
echo "           ,,:\` \`,,.                                                          "
echo "          ,,,    .,\`                                                           "
echo "         ,,.     \`,                                                            "
echo "       \`\`        \`.                                                          "
echo "                 \`\`                                       www.orientdb.com"
echo "                 \`                                    "

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
[ -f "$ORIENTDB_HOME"/bin/server.sh ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME
cd "$ORIENTDB_HOME/bin"

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

if [ -z "$ORIENTDB_LOG_CONF" ] ; then
    ORIENTDB_LOG_CONF=$ORIENTDB_HOME/config/orientdb-server-log.properties
fi

if [ -z "$ORIENTDB_WWW_PATH" ] ; then
    ORIENTDB_WWW_PATH=$ORIENTDB_HOME/www
fi

if [ -z "$ORIENTDB_PID" ] ; then
ORIENTDB_PID=$ORIENTDB_HOME/bin/orient.pid
fi

if [ -f "$ORIENTDB_PID" ]; then
    echo "removing old pid file $ORIENTDB_PID"
    rm "$ORIENTDB_PID"
fi

# DEBUG OPTS, SIMPLY USE 'server.sh debug'
DEBUG_OPTS=""
ARGS='';
for var in "$@"; do
    if [ "$var" = "debug" ]; then
        DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"
    else
        ARGS="$ARGS $var"
    fi
done

# ORIENTDB memory options, default to 4GB of heap.

if [ -z "$ORIENTDB_OPTS_MEMORY" ] ; then
    ORIENTDB_OPTS_MEMORY="-Xms4G -Xmx4G"
fi

if [ -z "$JAVA_OPTS_SCRIPT" ] ; then
    JAVA_OPTS_SCRIPT="-Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8 -Drhino.opt.level=9"
fi

# ORIENTDB SETTINGS LIKE DISKCACHE, ETC
if [ -z "$ORIENTDB_SETTINGS" ]; then
    ORIENTDB_SETTINGS="" # HERE YOU CAN PUT YOUR DEFAULT SETTINGS
fi

echo $$ > $ORIENTDB_PID

exec "$JAVA" $JAVA_OPTS \
    $ORIENTDB_OPTS_MEMORY \
    $JAVA_OPTS_SCRIPT \
    $ORIENTDB_SETTINGS \
    $DEBUG_OPTS \
    -Ddistributed=true \
    -Djava.util.logging.manager=com.orientechnologies.common.log.ShutdownLogManager \
    -Djava.util.logging.config.file="$ORIENTDB_LOG_CONF" \
    -Dorientdb.config.file="$CONFIG_FILE" \
    -Dorientdb.www.path="$ORIENTDB_WWW_PATH" \
    -Dorientdb.build.number="@BUILD@" \
    -cp "$ORIENTDB_HOME/lib/orientdb-server-@VERSION@.jar:$ORIENTDB_HOME/lib/*:$ORIENTDB_HOME/plugins/*" \
    $ARGS com.orientechnologies.orient.server.OServerMain
