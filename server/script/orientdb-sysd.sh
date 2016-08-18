#!/bin/sh
# OrientDB service script
#
# Copyright (c) OrientDB LTD (http://www.orientdb.com)

# description: OrientDb init script for systemd
# processname: orientdb-sysd.sh

# You have to SET the OrientDB installation directory here
ORIENTDB_DIR="YOUR
ORIENTDB_LOG_DIR=$ORIENTDB_DIR/log
ORIENTDB_PID=$ORIENTDB_DIR/bin/orient.pid


usage() {
        echo "Usage: `basename $0`: <start|stop|status>"
        exit 1
}

start() {
        status
        if [ $PID -gt 0 ]
        then
                echo "OrientDB server daemon was already started. PID: $PID"
                return $PID
        fi
        echo "Starting OrientDB server daemon..."
        cd "$ORIENTDB_DIR/bin"; /usr/bin/nohup ./server.sh 1>$ORIENTDB_LOG_DIR/orientdb.log 2>$ORIENTDB_LOG_DIR/orientdb.err &
}

stop() {
        status
        if [ $PID -eq 0 ]
        then
                echo "OrientDB server daemon is already not running"
                return 0
        fi
        echo "Stopping OrientDB server daemon..."
        cd "$ORIENTDB_DIR/bin"; /usr/bin/nohup ./shutdown.sh 1>>$ORIENTDB_LOG_DIR/orientdb.log 2>>$ORIENTDB_LOG_DIR/orientdb.err &
}

status() {
        if [ -f "$ORIENTDB_PID" ]  ; then
                PID=`cat "$ORIENTDB_PID"`
        fi

        if [ "x$PID" = "x" ]
        then
                PID=0
        fi

        # if PID is greater than 0 then OrientDB is running, else it is not
        return $PID
}

if [ "x$1" = "xstart" ]
then
        start
        exit 0
fi

if [ "x$1" = "xstop" ]
then
        stop
        exit 0
fi

if [ "x$1" = "xstatus" ]
then
        status
        if [ $PID -gt 0 ]
        then
                echo "OrientDB server daemon is running with PID: $PID"
                exit 0
        else
                echo "OrientDB server daemon is NOT running"
                exit 3
        fi
fi

usage