OrientDB Enterprise Agent
=======================

OrientDB Enterprise Agent an add-on to OrientDB Community edition, designed specifically for applications seeking a scalable, robust, and secure multi-model database. It add professional enterprise tools such as support forAuditing Tools, Metrics recording, Non-Stop Incremental Backups and Delta Synchronization in distributed.


### Installation

OrientDB Enterprise Agent is an additional package which can be installed in the [Community Edition](../gettingstarted/Tutorial-Installation.md). Download the Enterprise Agent jar package (or build it from sources) and copy it into the `/<orientdb>/plugins` directory, then start the server.

At run-time, the Enterprise edition logs this message:

```
2016-08-04 09:38:26:589 INFO  ***************************************************************************** [OEnterpriseAgent]
2016-08-04 09:38:26:589 INFO  *                     ORIENTDB  -  ENTERPRISE EDITION                       * [OEnterpriseAgent]
2016-08-04 09:38:26:589 INFO  ***************************************************************************** [OEnterpriseAgent]
2016-08-04 09:38:26:589 INFO  * If you are in Production or Test, you must purchase a commercial license. * [OEnterpriseAgent]
2016-08-04 09:38:26:589 INFO  * For more information look at: http://orientdb.com/orientdb-enterprise/    * [OEnterpriseAgent]
2016-08-04 09:38:26:590 INFO  ***************************************************************************** [OEnterpriseAgent]
```


## Accessing EE Features from Studio

To access the Enterprise features from OrientDB Studio, from the login page, click on `SERVER MANAGEMENT` (top right corner) and enter root username and password


## Building from sources

To build OrientDB Enterprise Agent you need java (at least 8) and maven installed on your machine.
From the mail directory, run the following command:

```
mvn clean install
```

You will find the agent jar file in the `/agent/target` directory

## Documentation

For further documentation, please refer to the official [OrientDB docs](https://orientdb.org/docs/3.2.x/ee/Enterprise-Edition.html)


## Licensing

Copyright 2022 SAP SE or an SAP affiliate company and OrientDB enterprise agent contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available via the REUSE tool (link to https://api.reuse.software/info/github.com/SAP/orientdb-enterprise-agent)
