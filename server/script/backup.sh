#!/bin/sh

DB=$1
USER=$2
PASSWD=$3
DEST_BACKUP=$4
SNAPSHOT_NAME=orientdbbackup
ECHO_PATH=/dev/null
TMP_MOUNT=/tmp/orientdb/backup

if [ "$#" -lt "4" ]
then
	echo "\nOrientDB Backup www.orientechnologies.com\n"
	echo "Help on: https://github.com/orientechnologies/orientdb/wiki/Backup-and-Restore\n"
	echo "Usage: $0 <dburl> <user> <password> <destination> [<type>]\n"
	echo "Where:"
	echo "\tdburl........: database URL "
	echo "\tuser.........: database user allowed to run the backup "
	echo "\tpassword.....: database password for the specified user "
	echo "\tdestination..: destination file path (use .zip as extension) where the backup is created "
	echo "\ttype.........: optional backup type, supported types are: "
	echo "\t               - default, locks the database during the backup "
	echo "\t               - lvm, uses LVM copy-on-write snapshot to execute in background "
	echo "\n"	
	exit 1
fi

cd "$(dirname "$0")"

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


if [ "$#" != "5" ] || [ "$5" = "default" ]
then
	sh ./console.sh "connect $DB $USER $PASSWD;freeze database;backup database $DEST_BACKUP;release database;" 1> $ECHO_PATH
	exit 0 
fi
if [ "$5" != "lvm"  ]
then
	echo "impossible to find backup type: $5"
	exit 1
fi
check_errs()
{
  if [ "${1}" -ne "0" ]; then
    echo "\nERROR # ${1} : ${2}"
    exit ${1}
  fi
}

check_errs_no_exit()
{
  if [ "${1}" -ne "0" ]; then
    echo "\nERROR # ${1} : ${2}"
  else
    echo "$3"
  fi
}


which lvcreate 1> /dev/null
check_errs $? "Impossible to find lvcreate check if is correctly installed and if you have execution permission"

which lvremove 1> /dev/null
check_errs $? "Impossible to find lvremove check if is correctly installed and if you have execution permission"

which zip 1> /dev/null
check_errs $? "Impossible to find zip command check if is correctly installed"

ENGINE=`echo $DB | awk -F ':' '{print $1}'`
DB_NAME=` echo $DB | awk -F '/' '{print $NF}'`
echo $ORIENTDB_HOME
if [ "$ENGINE" = "plocal" ]
then
	DB_PATH=`echo $DB | awk -F ':' '{print $2}'`
	FREEZE=false
else
	DB_PATH="$ORIENTDB_HOME/databases/$DB_NAME"
	FREEZE=true
fi
TMP_PATH=`pwd`
cd $DB_PATH
DB_PATH=`pwd`
cd $TMP_PATH

DEVICE_PATH=`df -h $DB_PATH | awk '{if (NR !=1) {print $1}}'`
MOUNT_POINT=`df -h $DB_PATH | awk '{if (NR !=1) {print $6}}'`
PARTITION_PATH=`echo $DB_PATH | awk -v MOUNT_POINT=$MOUNT_POINT '{print substr($0,length(MOUNT_POINT)+1)}'`

DEVICE=`lvdisplay $DEVICE_PATH | grep 'LV Path' | awk '{print $3}'`
if [ "$FREEZE" = "true" ]
then
	sh ./console.sh "connect $DB $USER $PASSWD;freeze database;disconnect;" 1> $ECHO_PATH
	check_errs $? "database freeze failed"
else
	sh ./console.sh "connect $DB $USER $PASSWD;disconnect;" 1> $ECHO_PATH
	check_errs $? "failed to connect to $DB"
fi
lvcreate -L592M -s -n $SNAPSHOT_NAME $DEVICE 1> $ECHO_PATH
check_errs $? "create $DEVICE snapshot failed"

if [ "$FREEZE" = "true" ]
then
	sh ./console.sh "connect $DB $USER $PASSWD;release database;disconnect;" 1> $ECHO_PATH
	check_errs $? "database release failed"
fi

SNAPSHOT_PATH=`echo $DEVICE | awk -v SNAPSHOT_NAME=$SNAPSHOT_NAME -F '/' '{for(i=1;i<NF;i++){ printf "%s%s",$(i),"/"} print SNAPSHOT_NAME}'`
SNAPSHOT_DEVICE=`lvdisplay $SNAPSHOT_PATH | grep 'LV Path' | awk '{print $3}'`
mkdir -p $TMP_MOUNT
mount $SNAPSHOT_DEVICE $TMP_MOUNT
cd $TMP_MOUNT/$PARTITION_PATH

echo "starting backup of $DB_NAME in $DEST_BACKUP "
zip -r $DEST_BACKUP *
check_errs_no_exit $? "backup $DEST_BACKUP creation failed " "backup created correctly in $DEST_BACKUP "
cd $TMP_PATH
umount $SNAPSHOT_DEVICE
check_errs $? "umount of $SNAPSHOT_DEVICE failed please clean it manually "
lvremove $SNAPSHOT_DEVICE -f 1> $ECHO_PATH
check_errs $? "lvremove of $SNAPSHOT_DEVICE failed please clean it manually "


