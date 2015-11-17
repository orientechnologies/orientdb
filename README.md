# orientdb-gremlin

[![Build Status](https://travis-ci.org/mpollmeier/orientdb-gremlin.svg)](https://travis-ci.org/mpollmeier/orientdb-gremlin) [![Coverage Status](https://coveralls.io/repos/mpollmeier/orientdb-gremlin/badge.svg?branch=master)](https://coveralls.io/r/mpollmeier/orientdb-gremlin?branch=master) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.michaelpollmeier/orientdb-gremlin/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.michaelpollmeier/orientdb-gremlin/) 

TP3 driver for orientdb. This started off as just a proof of concept, but thanks to a lot of help it's now in a really good shape.

The main area that need some more work is index lookups - currently it does find the right index for a simple case, e.g. `g.V.hasLabel("myLabel").has("someKey", "someValue")`. However if there are multiple indexes on the same property, or if there the traversal should better use a composite index, that's not handled well yet. If you feel inclined you can add these cases to the OrientGraphIndexTest.java.

## Tests
* It runs the standard tinkerpop test suite for database drivers.
* there are some additional tests that you can run with `mvn test`
* additionally there is a separate suite of tests in the `tests-scala` directory which you can run using `sbt test`

## Usage
Have a look at the tests-scala which demonstrates the usage. There's also an orientdb example project in [gremlin-scala-examples](https://github.com/mpollmeier/gremlin-scala-examples).
