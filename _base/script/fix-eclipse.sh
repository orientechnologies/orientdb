#!/bin/bash
if [ -z "$1" ]
then
    echo "SYNTAX ERROR, USE: fix-eclipse.sh <root-path>"
    exit
fi

echo "Updating version in Eclipse .classpath files..."

TFILE="/tmp/out.tmp.$$"
for filename in $(grep -r "J2SE-1.5" --include "*.classpath" $1|cut -f 1 -d :)
do
  if [ -f $filename -a -r $filename ]; then
    #/bin/cp -f $filename ${filename}.old
    sed "s/J2SE-1.5/JavaSE-1.6/g" "$filename" > $TFILE && mv $TFILE "$filename"
  else
    echo "Error: Cannot read $filename"
  fi
done

for filename in $(grep -r "1.5" --include "*/.settings/org.eclipse.jdt.core.prefs" $1|cut -f 1 -d :)
do
  if [ -f $filename -a -r $filename ]; then
    #/bin/cp -f $filename ${filename}.old
    rm "$filename" > $TFILE && mv $TFILE "$filename"
  else
    echo "Error: Cannot read $filename"
  fi
done

for filename in $(find $1 -name bin -print)
do
  if [ -d $filename ]; then
    #/bin/cp -f $filename ${filename}.old
    rm -rf "$filename" > $TFILE && mv $TFILE "$filename"
  else
    echo "Error: Cannot read $filename"
  fi
done

/bin/rm $TFILE 2>/dev/null
