Welcome to the OrientDB-ETL module. By using this module you can easily move data from and to OrientDB.

## Installation
Starting from OrientDB v2.0 the ETL module will be distributed in bundle with the official release. If you want to use it, then follow these steps:
- Clone the repository on your computer, by executing:
 - ```git clone https://github.com/orientechnologies/orientdb-etl.git```
- Compile the module, by executing:
 - ```mvn clean install```
- Copy ```script/oetl.sh``` (or .bat under Windows) to $ORIENTDB_HOME/bin
- Copy ```target/orientdb-etl-2.0-SNAPSHOT.jar``` to $ORIENTDB_HOME/lib

## Usage

```
$ cd $ORIENTDB_HOME/bin
$ ./oetl.sh config-dbpedia.json
```

## Available Components
- [Blocks](https://github.com/orientechnologies/orientdb-etl/wiki/Blocks)
- [Extractors](https://github.com/orientechnologies/orientdb-etl/wiki/Extractors)
- [Transformers](https://github.com/orientechnologies/orientdb-etl/wiki/Transformers)
- [Loaders](https://github.com/orientechnologies/orientdb-etl/wiki/Loaders)

Examples:
- [Import DBPedia](https://github.com/orientechnologies/orientdb-etl/wiki/Import-from-DBPedia)

## Configuration Syntax
```
{
  "config": {
    <name>: <value>
  },
  "begin": [
    { <block-name>: { <configuration> } }
  ],
  "extractor" : {
    { <extractor-name>: { <configuration> } }
  },
  "transformers" : [
    { <transformer-name>: { <configuration> } }
  ],
  "loader" : { <loader-name>: { <configuration> } },
  "end": [
   { <block-name>: { <configuration> } }
  ]
}
```
