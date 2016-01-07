#!/bin/bash
if [ -z "$1" -o -z "$2" -o -z "$3" ]
then
    echo "SYNTAX ERROR, USE: upd-version.sh <root-path> <version-from> <version-to>"
    exit
fi

echo "This script is UNSAFE, please DOUBLE CHECK the result BEFORE COMMIT"

echo "Updating version from $2 to $3 in directory $1 and subfolders"

TFILE="/tmp/out.tmp.$$"
for filename in $(grep -r "$2" --include "*.java" --include "*.htm*" --include "*.xml" --exclude-dir "apidocs" --exclude-dir "target" --exclude-dir "test-output" $1|cut -f 1 -d :)
do
  if [ -f $filename -a -r $filename ]; then
    #/bin/cp -f $filename ${filename}.old
    sed "s/$2/$3/g" "$filename" > $TFILE && mv $TFILE "$filename"
  else
    echo "Error: Cannot read $filename"
  fi
done
/bin/rm $TFILE 2>/dev/null
