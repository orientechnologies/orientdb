## OrientDB

[<img src="http://www.orientdb.org/images/graphed-tutorial-graph_small.png">](http://studio.nuvolabase.com/db/free/demo/GratefulDeadConcerts/studio/?user=reader&passthrough=false&database=/db/free/demo/GratefulDeadConcerts&password=reader)

## What is Orient?

**OrientDB** is an Open Source [NoSQL](http://en.wikipedia.org/wiki/NoSQL) DBMS with the features of both Document and Graph DBMSs. It's written in Java and it's amazingly fast: it can store up to 150,000 records per second on common hardware. Even for a Document based database the relationships are managed as in [Graph Databases](http://en.wikipedia.org/wiki/Graph_database) with direct connections among records. You can traverse parts of or entire trees and graphs of records in a few milliseconds. Supports schema-less, schema-full and schema-mixed modes. Has a strong security profiling system based on user and roles and supports [[SQL]] amongst the query languages. Thanks to the [[SQL]] layer it's straightforward to use for people skilled in the Relational world.

Look also at [Presentations](https://github.com/orientechnologies/orientdb/wiki/Presentations) with video and slides introducing OrientDB.

## Is OrientDB a Relational DBMS?

No. OrientDB adheres to the [NoSQL](http://en.wikipedia.org/wiki/NoSQL) movement even though it supports a subset of [[SQL]] as query language. In this way it's easy to start using it without having to learn too much new stuff. OrientDB is a [Document Database](http://en.wikipedia.org/wiki/Document-oriented_database) but has the best features of other DBMSs. For example relationships are handled as in [Graph Databases](http://en.wikipedia.org/wiki/Graph_database).

## Scalability: the database is the bottleneck of most of applications

The most common reason applications scale out badly is, very often, the database. The database is the bottleneck of most applications. OrientDB scales out very well on a single machine. A single server does the work of about 125 servers running [MySQL](http://en.wikipedia.org/wiki/Mysql). The transactional engine can run in distributed systems supporting up to 302,231,454,903,657 billion (2^78) records for the maximum capacity of 19,807,040,628,566,084 Terabytes of data distributed on multiple disks in multiple nodes.

## I can't believe it! Why is it so fast?

OrientDB has been designed to be very fast. It inherits the best features and concepts from Object Databases, Graph DBMS and modern [NoSQL](http://en.wikipedia.org/wiki/NoSQL) engines. Furthermore it uses the own **MVRB-Tree** algorithm as a mix of [Red-Black Tree](http://en.wikipedia.org/wiki/Red-black_tree) and [B+Tree](http://en.wikipedia.org/wiki/B%2Btree). MVRB-Tree consumes about half memory of the [Red-Black Tree](http://en.wikipedia.org/wiki/Red-black_tree) implementation maintaining the original speed while it balances the tree on insertion/update. Furthermore the MVRB-Tree allows fast retrieving and storing of nodes in persistent way.
Download the Benchmark PDF <a href="https://docs.google.com/viewer?a=v&pid=sites&srcid=ZGVmYXVsdGRvbWFpbnx0b2t5b3RlY2hzdXp1bXVyYWxhYmVuZ3xneDoyMGRiOGFlM2Y2OGY5Mzhj">XGDBench: A Benchmarking Platform for Graph Stores in Exascale Clouds</a> by <a href="http://www.cs.titech.ac.jp/cs-home-e.html">Tokyo Institute of Technology</a> and <a href="http://www.research.ibm.com/labs/tokyo/">IBM Research</a>.

## Why yet another NoSQL?

It all began on 2009 when [Luca Garulli](https://github.com/orientechnologies/orientdb/wiki/Team) was searching for super fast and flexible storage for an ambitious project. After having tried different RDBMSs he worked on the available NoSQL products. Not one had all the features he needed. So in a weekend he got the challenge to see if the "old" low-level storage algorithms of Orient ODBMS, an Object Database Luca created in 1999 written in C++, could be reused in Java to develop a brand new graph-document DBMS. It worked! And this is the reason OrientDB exists today.

## But wasn't OrientDB an ODBMS?

Orient ODBMS was the very first version of the Orient engine developed in C++ in 1998. Today OrientDB has been totally rewritten in Java in the form of a Document database but with the previous main goal: performance. However, now you can find the [Object Database], but it's a wrapper built on top of the [Document Database](https://github.com/orientechnologies/orientdb/wiki/Document-Database). It maps transparently OrientDB document records to POJOs.

## How does it compare with other products?

Take a look at [GraphDB comparison](https://github.com/orientechnologies/orientdb/wiki/GraphDB-Comparison) and [DocumentDB comparison](https://github.com/orientechnologies/orientdb/wiki/DocumentDB-Comparison).
Download the Benchmark PDF <a href="https://docs.google.com/viewer?a=v&pid=sites&srcid=ZGVmYXVsdGRvbWFpbnx0b2t5b3RlY2hzdXp1bXVyYWxhYmVuZ3xneDoyMGRiOGFlM2Y2OGY5Mzhj">XGDBench: A Benchmarking Platform for Graph Stores in Exascale Clouds</a> by <a href="http://www.cs.titech.ac.jp/cs-home-e.html">Tokyo Institute of Technology</a> and <a href="http://www.research.ibm.com/labs/tokyo/">IBM Research</a>.

## Easy to install and use

Yes. OrientDB is totally written in [Java](http://en.wikipedia.org/wiki/Java_%28programming_language%29) and can run on any platform without configuration and installation. The full Server distribution is about 1Mb without the demo database. Do you develop with a language different than Java? No problem, look at the [Programming Language Binding](https://github.com/orientechnologies/orientdb/wiki/Programming-Language-Bindings).

## Professional services

OrientDB is free for any use (Apache 2 license). If you are in production don't miss the [professional support service](http://orientechnologies.com/support.htm). For courses and training look at the [on-line course catalog](http://orientechnologies.com/training.htm).

## Know more

Start to learn about OrientDB from the [WiKi Main page](https://github.com/orientechnologies/orientdb/wiki). For any questions visit the [OrientDB Community Group](http://www.orientdb.org/community-group.htm). Need help? Go to the [Online support](http://chat.stackoverflow.com/rooms/6625/orientdb). Do you want to hear about OrientDB in a conference? Take a look at the [Events](https://github.com/orientechnologies/orientdb/wiki/) page.

[![](http://mac.softpedia.com/base_img/softpedia_free_award_f.gif)](http://mac.softpedia.com/get/Developer-Tools/Orient.shtml)

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/56a16d9c5e47a25019e0be3a52d8a366 "githalytics.com")](http://githalytics.com/orientechnologies/orientdb)

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/orientechnologies/orientdb/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

<script>
  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-28543690-2', 'orientechnologies.com');
  ga('send', 'pageview');

</script>
