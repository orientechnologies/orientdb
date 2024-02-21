#! /bin/sh
$* > /dev/null 2>&1 &
echo "Starting OrientDB..."
sleep 5
exit 0
