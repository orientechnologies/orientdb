#!/bin/sh
# OrientDB service script
#
# Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)

# chkconfig: 2345 20 80
# description: OrientDb init script
# processname: orientdb.sh

# You have to SET the OrientDB installation directory here
ORIENTDB_DIR="YOUR_ORIENTDB_INSTALLATION_PATH"
ORIENTDB_USER="USER_YOU_WANT_ORIENTDB_RUN_WITH"

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
	cd "$ORIENTDB_DIR/bin"
	su $ORIENTDB_USER -c "cd \"$ORIENTDB_DIR/bin\"; /usr/bin/nohup ./server.sh 1>../log/orientdb.log 2>../log/orientdb.err &"
}

stop() {
	status
	if [ $PID -eq 0 ]
	then
		echo "OrientDB server daemon is already not running"
		return 0
	fi
	echo "Stopping OrientDB server daemon..."
	cd "$ORIENTDB_DIR/bin"
	su $ORIENTDB_USER -c "cd \"$ORIENTDB_DIR/bin\"; /usr/bin/nohup ./shutdown.sh 1>>../log/orientdb.log 2>>../log/orientdb.err &"
}

status() {
	PID=` ps auxw | grep 'orientdb.www.path' | grep java | grep -v grep | awk '{print $2}'`
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
	else
		echo "OrientDB server daemon is NOT running"
	fi
	exit $PID
fi

usage
