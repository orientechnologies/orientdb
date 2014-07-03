Welcome to the OrientDB-ETL module. By using this module you can easily move data from and to OrientDB by executing an [ETL process](http://en.wikipedia.org/wiki/Extract,_transform,_load). OrientDB ETL is based on the following principles:
- one [configuration file](https://github.com/orientechnologies/orientdb-etl/wiki/Configuration-File) in [JSON](http://en.wikipedia.org/wiki/JSON) format
- one [Extractor](https://github.com/orientechnologies/orientdb-etl/wiki/Extractor) is allowed to extract data from a source
- one [Loader](https://github.com/orientechnologies/orientdb-etl/wiki/Loader) is allowed to load data to a destintion
- multiple [Transformers](https://github.com/orientechnologies/orientdb-etl/wiki/Transformer) that transform data in pipeline. They receive something in input, do something, return something as output that will be processed as input by the next component

## How ETL works
```
EXTRACTOR => TRANSFORMERS[] => LOADER
```
Example of a process that extract from a CSV file, apply some change, lookup if the record has already been created and then store the record as document against OrientDB database:

```
+----------------+-----------------------+-----------+
|   EXTRACTOR    | TRANSFORMERS pipeline |  LOADER   |
+----------------+-----------------------+-----------+
|    FILE       ==>  CSV->FIELD->MERGE  ==> OrientDB |
+----------------+-----------------------+-----------+
```

Look to the [Documentation](https://github.com/orientechnologies/orientdb-etl/wiki/Home) for more information.
