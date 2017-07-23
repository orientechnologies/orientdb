## OrientDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
**Develop branch:** [![Build Status](http://helios.orientdb.com/view/multibranch/job/orientdb-multibranch/job/develop/badge/icon)](http://helios.orientdb.com/view/multibranch/job/orientdb-multibranch/job/develop/)  **2.2.x branch:** [![Build Status](http://helios.orientdb.com/view/multibranch/job/orientdb-multibranch/job/2.2.x/badge/icon)](http://helios.orientdb.com/view/multibranch/job/orientdb-multibranch/job/2.2.x/) **Chat with the community:** [![Gitter chat](https://badges.gitter.im/orientechnologies/orientdb.png)](https://gitter.im/orientechnologies/orientdb)

------

<img src="http://orientdb.com/orientdb-studio_800px.png">

## What is OrientDB?

**OrientDB** is an Open Source Multi-Model [NoSQL](http://en.wikipedia.org/wiki/NoSQL) DBMS with the support of Native Graphs, Documents Full-Text, Reactivity, Geo-Spatial and Object Oriented concepts. It's written in Java and it's amazingly fast: it can store 220,000 records per second on common hardware. No expensive run-time JOINs, connections are managed as persistent pointers between records. You can traverse thousands of records in a few milliseconds. Supports schema-less, schema-full and schema-mixed modes. Has a strong security profiling system based on user and roles and supports [SQL](http://orientdb.com/docs/last/SQL.html) amongst the query languages. Thanks to the [SQL](http://orientdb.com/docs/last/SQL.html) layer it's straightforward to use for people skilled in the Relational world.

[Get started with OrientDB](http://orientdb.com/getting-started/).

## Is OrientDB a Relational DBMS?

No. OrientDB adheres to the [NoSQL](http://en.wikipedia.org/wiki/NoSQL) movement even though it supports [ACID Transactions](https://orientdb.com/docs/2.2/Transactions.html) and [SQL](http://orientdb.com/docs/last/SQL.html) as query language. In this way it's easy to start using it without having to learn too much new stuff. 

## Scalability: the database is the bottleneck of most applications

The most common reason applications scale out badly is, very often, the database. The database is the bottleneck of most applications. OrientDB scales out very well on multiple machines, thanks to the Multi-Master replication where there is no single bottleneck on writes like with Master-Slave replication. The database can be up to 302,231,454,903,657 billion (2^78) records for the maximum capacity of 19,807,040,628,566,084 Terabytes of data on a single server or multiple nodes.

## I can't believe it! Why is it so fast?

OrientDB has been designed to be very fast. It inherits the best features and concepts from Object Databases, Graph DBMS and modern [NoSQL](http://en.wikipedia.org/wiki/NoSQL) engines. OrientDB manages relationships without the run-time costly join operation, but rather with direct pointers (links) between records. No matters if you have 1 or 1,000 Billion of records, the traversing cost remains constant. Download the Benchmark PDF <a href="https://docs.google.com/viewer?a=v&pid=sites&srcid=ZGVmYXVsdGRvbWFpbnx0b2t5b3RlY2hzdXp1bXVyYWxhYmVuZ3xneDoyMGRiOGFlM2Y2OGY5Mzhj">XGDBench: A Benchmarking Platform for Graph Stores in Exascale Clouds</a> by <a href="http://www.cs.titech.ac.jp/cs-home-e.html">Tokyo Institute of Technology</a> and <a href="http://www.research.ibm.com/labs/tokyo/">IBM Research</a>.

## How does it compare with other products?

As Multi-Model DBMS, OrientDB could work as an extended Document Database and an extended Graph Database. Take a look at [OrientDB vs MongoDB](http://orientdb.com/orientdb-vs-mongodb/) for Document Databases and [OrientDB vs Neo4j](http://orientdb.com/orientdb-vs-neo4j/) to have a comparison with a popular Graph Database.

## Easy to install and use

Yes. OrientDB is totally written in [Java](http://en.wikipedia.org/wiki/Java_%28programming_language%29) and can run on any platform without configuration and installation. The full Server distribution is a few MBs without the demo database. Do you develop with a language different than Java? No problem, look at the [Programming Language Binding](http://orientdb.com/docs/last/Programming-Language-Bindings.html).

## Professional services

OrientDB Community Edition is free for any use (Apache 2 license). If you are in production don't miss the [Enterprise Edition](http://orientdb.com/orientdb-enterprise/) and [professional support service](http://orientdb.com/support/). For courses and training look at the [on-line course catalog](http://orientdb.com/training/).

## Main References
- [Documentation](http://orientdb.com/docs/last/)
- For any questions visit the [OrientDB Community Group](http://orientdb.com/active-user-community/)
- [Professional Support](orientdb.com/support/).

[Get started with OrientDB](http://orientdb.com/getting-started/).

--------
### Sponsors

|[![](https://www.yourkit.com/images/yklogo.png)](https://www.yourkit.com/.net/profiler/index.jsp)|YourKit supports open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>, innovative and intelligent tools for profiling Java and .NET applications.|
|---|---|

--------

[![](http://s1.softpedia-static.com/_img/sp100free.png?1)](http://www.softpedia.com/get/Internet/Servers/Database-Utils/OrientDB.shtml#status)
