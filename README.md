# orientdb-gremlin
TP3 driver for orientdb

[![Linux Build Status](https://travis-ci.org/mpollmeier/orientdb-gremlin.svg)](https://travis-ci.org/mpollmeier/orientdb-gremlin)
[![Coverage Status](https://coveralls.io/repos/mpollmeier/orientdb-gremlin/badge.svg?branch=master)](https://coveralls.io/r/mpollmeier/orientdb-gremlin?branch=master)  
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.michaelpollmeier/orientdb-gremlin/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.michaelpollmeier/orientdb-gremlin/) 

For now this is just a proof of concept, nowhere ready to use in production. But the structure is there, and it supports basic traversals. If you run into a `NotImplementedException` then it's a sign that you should help ;)

## Tests...
... are using Gremlin-Scala and can be run inside the `tests-scala` directory using `sbt test` This does not replace the standard TP3 testsuite for a graph vendor, but I didn't have the time to implement that. 

## Usage
Have a look at the tests-scala which demonstrates the usage. There's also an orientdb example project in [gremlin-scala-examples]([https://github.com/mpollmeier/gremlin-scala-examples).
