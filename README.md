## OrientDB

[<img src="http://www.orientdb.org/images/graphed-tutorial-graph_small.png">](http://studio.nuvolabase.com/db/free/demo/GratefulDeadConcerts/studio/?user=reader&passthrough=false&database=/db/free/demo/GratefulDeadConcerts&password=reader)

## What is Orient?

**OrientDB** is an Open Source [NoSQL](http://en.wikipedia.org/wiki/NoSQL) DBMS with both the features of Document and Graph DBMSs. It's written in Java and it's amazing fast: can store up to 150,000 records per second on common hardware. Even if it's Document based database the relationships are managed as in [Graph Databases](http://en.wikipedia.org/wiki/Graph_database) with direct connections among records. You can traverse entire or part of trees and graphs of records in few milliseconds. Supports schema-less, schema-full and schema-mixed modes. Has a strong security profiling system based on user and roles and support the [SQL](SQLQuery) between the query languages. Thank to the [SQL](SQLQuery) layer it's straightforward to use it for people skilled in Relational world.

Look also at the [[Presentations]] with video and slides introduce OrientDB.

## Is OrientDB a Relational DBMS?

No. OrientDB adheres to the [NoSQL](http://en.wikipedia.org/wiki/NoSQL) movement even if supports a subset of [SQL](SQLQuery) as query language. In this way it's easy to start using it without to learn too much new stuff. OrientDB is a [[DocumentDatabase]] but has the best features of other DBMSs. For example the relationships are handled as in the [Graph Databases](http://en.wikipedia.org/wiki/Graph_database).

# Scalability: the database is the bottleneck of most of applications

The most common problem why applications scale out bad is, very often, the database. Database is the bottleneck of most of applications. OrientDB scales out very well on a single machine. A single server makes the work of about 125 servers running [MySQL](http://en.wikipedia.org/wiki/Mysql). The transactional engine can run in distributed systems supporting up to 302,231,454,903,657 Billions (2^78) of records for the maximum capacity of 19,807,040,628,566,084 Terabytes of data distributed on multiple disks in multiple nodes. Today only OrientDB Key/Value Server can run in a cluster with thousands of instances using a Distributed Hash Table algorithm. We're developing the distributed version of OrientDB as well.

## I can't believe! Why it's so fast?

OrientDB has been designed to be very fast. It inherits the best features and concepts from the Object Databases, Graph DBMS and the modern [NoSQL](http://en.wikipedia.org/wiki/NoSQL) engines. Furthermore it uses the own **RB+Tree** algorithm as mix of [Red-Black Tree](http://en.wikipedia.org/wiki/Red-black_tree) and [B+Tree](http://en.wikipedia.org/wiki/B%2Btree). RB+Tree consumes about half memory of the [Red-Black Tree](http://en.wikipedia.org/wiki/Red-black_tree) implementation mantaining the original speed while it balances the tree on insertion/update. Furthermore the RB+Tree allows fast retrieving and storing of nodes in persistent way.

## Why yet another NoSQL?

All is begun on 2009 when [[Team|Luca Garulli]] was searching a super fast and flexible storage for an ambitious project. After have tried different RDBMSs he worked on the available NoSQL products. No one had all the featured he needed. So in a weekend he got the challenge to try if the "old" low-level storage algorithms of Orient ODBMS, an Object Database Luca created in 1999 written in C++, could be reused in Java to develop a brand new graph-document DBMS. It worked! And this is the reason why today OrientDB is between us.

## But wasn't OrientDB a ODBMS?

Orient ODBMS was the very first version of the Orient engine developed in C++ in 1998. Today OrientDB has been totally rewritten in Java under the form of a Document database but with the previous main goal: performance. However now you can find the [[Object Database|Object Database]], but it's a wrapper built on top of the [[Document Database|Document Database]]. It maps transparently OrientDB document records with POJOs.

## How does it compares with other products?

Take a look to [GraphDB comparison](GraphDBComparison) and [DocumentDB comparison](DocumentDBComparison).

## Easy to install and use

Yes. OrientDB is totally written in [Java](http://en.wikipedia.org/wiki/Java_%28programming_language%29) and can run in any platform without configuration and installation. The full Server distribution is about 1Mb without the demo database. Do you develop with a language different than Java? No problem, look at the [[Programming Language Bindings|supported drivers]].

## Professional services

OrientDB is free for any use (Apache 2 license). If you are in production don't miss the [professional support service](http://www.nuvolabase.com/site/professional.html). For courses and training look at the [on-line course catalog](http://www.nuvolabase.com/site/training.html).

## Online cloud service

<table>
  <tr><td><a href="http://www.nuvolabase.com"><img src="http://www.nuvolabase.com/site/images/nuvola_small.png"></a>
  </td><td>OrientDB is available on the cloud through <a href="http://www.nuvolabase.com">NuvolaBase.com</a>. FREE accounts are available for small sized databases.</td></tr>
</table>

## Know more

Start to learn OrientDB from the [WiKi Main page](https://github.com/nuvolabase/orientdb/wiki). For any question visit the [OrientDB Community Group](http://www.orientdb.org/community-group.htm). Need help? Go to the [Online support](http://chat.stackoverflow.com/rooms/6625/orientdb). Do you want to hear about OrientDB in a conference? Take a look at the [[Events]] page.

[![](http://mac.softpedia.com/base_img/softpedia_free_award_f.gif)](http://mac.softpedia.com/get/Developer-Tools/Orient.shtml)
