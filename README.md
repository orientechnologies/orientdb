## OrientDB

master branch [![Build Status](http://helios.orientdb.com/buildStatus/icon?job=orientdb-master-ci)](http://helios.orientdb.com/job/orientdb-master-ci/) develop branch [![Build Status](http://helios.orientdb.com/buildStatus/icon?job=orientdb-develop)](http://helios.orientdb.com/job/orientdb-develop/) [![Gitter chat](https://badges.gitter.im/orientechnologies/orientdb.png)](https://gitter.im/orientechnologies/orientdb)

------

<img src="http://orientdb.com/orientdb-studio_800px.png">

## What is OrientDB?

**OrientDB** is an Open Source [NoSQL](http://en.wikipedia.org/wiki/NoSQL) DBMS with the features of both Document and Graph DBMSs. It's written in Java and it's amazingly fast: it can store 220,000 records per second on common hardware. Even for a Document based database the relationships are managed as in [Graph Databases](http://en.wikipedia.org/wiki/Graph_database) with direct connections among records. You can traverse parts of or entire trees and graphs of records in a few milliseconds. Supports schema-less, schema-full and schema-mixed modes. Has a strong security profiling system based on user and roles and supports [SQL](http://orientdb.com/docs/last/SQL.html) amongst the query languages. Thanks to the [SQL](http://orientdb.com/docs/last/SQL.html) layer it's straightforward to use for people skilled in the Relational world.

[Get started with OrientDB](http://orientdb.com/getting-started/).

[![Gitter chat](https://badges.gitter.im/orientechnologies/orientdb.png)](https://gitter.im/orientechnologies/orientdb)


## Is OrientDB a Relational DBMS?

No. OrientDB adheres to the [NoSQL](http://en.wikipedia.org/wiki/NoSQL) movement even though it supports a subset of [SQL](http://orientdb.com/docs/last/SQL.html) as query language. In this way it's easy to start using it without having to learn too much new stuff. OrientDB is a [Document Database](http://en.wikipedia.org/wiki/Document-oriented_database) but has the best features of other DBMSs. For example relationships are handled as in [Graph Databases](http://en.wikipedia.org/wiki/Graph_database).

## Scalability: the database is the bottleneck of most applications

The most common reason applications scale out badly is, very often, the database. The database is the bottleneck of most applications. OrientDB scales out very well on multiple machines, thanks to the Multi-Master replication where there is no single bottleneck on writes like with Master-Slave replication. The database can be up to 302,231,454,903,657 billion (2^78) records for the maximum capacity of 19,807,040,628,566,084 Terabytes of data on a single server or multiple nodes.

## I can't believe it! Why is it so fast?

OrientDB has been designed to be very fast. It inherits the best features and concepts from Object Databases, Graph DBMS and modern [NoSQL](http://en.wikipedia.org/wiki/NoSQL) engines. OrientDB manages relationships without the run-time costly join operation, but rather with direct pointers (links) between records. No matters if you have 1 or 1,000 Billion of records, the traversing cost remains constant. Download the Benchmark PDF <a href="https://docs.google.com/viewer?a=v&pid=sites&srcid=ZGVmYXVsdGRvbWFpbnx0b2t5b3RlY2hzdXp1bXVyYWxhYmVuZ3xneDoyMGRiOGFlM2Y2OGY5Mzhj">XGDBench: A Benchmarking Platform for Graph Stores in Exascale Clouds</a> by <a href="http://www.cs.titech.ac.jp/cs-home-e.html">Tokyo Institute of Technology</a> and <a href="http://www.research.ibm.com/labs/tokyo/">IBM Research</a>.

## How does it compare with other products?

As Multi-Model DBMS, OrientDB could work as an extended Document Database and an extended Graph Database. Take a look at [OrientDB vs MongoDB](http://orientdb.com/orientdb-vs-mongodb/) for Document Databases and [OrientDB vs Neo4j](http://orientdb.com/orientdb-vs-neo4j/) to have a comparison with a popular Graph Database.

## Are you looking for benchmarks?

Download the Benchmark PDF <a href="https://docs.google.com/viewer?a=v&pid=sites&srcid=ZGVmYXVsdGRvbWFpbnx0b2t5b3RlY2hzdXp1bXVyYWxhYmVuZ3xneDoyMGRiOGFlM2Y2OGY5Mzhj">XGDBench: A Benchmarking Platform for Graph Stores in Exascale Clouds</a> by <a href="http://www.cs.titech.ac.jp/cs-home-e.html">Tokyo Institute of Technology</a> and <a href="http://www.research.ibm.com/labs/tokyo/">IBM Research</a>.

## Easy to install and use

Yes. OrientDB is totally written in [Java](http://en.wikipedia.org/wiki/Java_%28programming_language%29) and can run on any platform without configuration and installation. The full Server distribution is about 1Mb without the demo database. Do you develop with a language different than Java? No problem, look at the [Programming Language Binding](http://orientdb.com/docs/last/Programming-Language-Bindings.html).

## Professional services

OrientDB is free for any use (Apache 2 license). If you are in production don't miss the [professional support service](http://orientdb.com/support/). For courses and training look at the [on-line course catalog](http://orientdb.com/training/).

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

[![](http://www.softpedia.com/_img/softpedia_100_free.png)](http://mac.softpedia.com/get/Developer-Tools/Orient.shtml)

<script>
  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-28543690-2', 'orientdb.com');
  ga('send', 'pageview');

</script>
