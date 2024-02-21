#! /bin/sh
echo "Stopping OrientDB..."
$*
cat target/test-status.txt  | grep 0
exit $?
