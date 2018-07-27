# orientdb-gremlin

[![Build Status](https://travis-ci.org/mpollmeier/orientdb-gremlin.svg?branch=master)](https://travis-ci.org/mpollmeier/orientdb-gremlin?branch=master) [![Coverage Status](https://coveralls.io/repos/mpollmeier/orientdb-gremlin/badge.svg?branch=master)](https://coveralls.io/r/mpollmeier/orientdb-gremlin?branch=master) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.michaelpollmeier/orientdb-gremlin/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.michaelpollmeier/orientdb-gremlin/) 

Apache TinkerPop 3 Graph Structure Implementation for OrientDB. This started off as just a proof of concept, but thanks to a lot of help it's now in a really good shape and it has been officially adopted by OrientDB Team to be part of OrientDB v3.x. 

The main area that need some more work is index lookups - currently it does find the right index for a simple case, e.g. `g.V.hasLabel("myLabel").has("someKey", "someValue")`. However if there are multiple indexes on the same property, or if there the traversal should better use a composite index, that's not handled well yet. If you feel inclined you can add these cases to the `OrientGraphIndexTest.java`. The function that looks up indexes is `OrientGraphStep.findIndex`.

## Tests
* you can run the standard tinkerpop test suite with `mvn install -P release`
* there are some additional tests that you can run independently with `mvn test`
* additionally there is a separate suite of tests in the `tests-scala` directory which you can run using `sbt test`
* to automatically format the code (travis CI enforces a format check), just run `mvn clean install`

## Usage
Have a look at the tests-scala which demonstrates the usage. There's also an orientdb example project in [gremlin-scala-examples](https://github.com/mpollmeier/gremlin-scala-examples).

## Labels and classes
Vertices and Edges are stored as classes based on their label. In order to allow vertices and edges to use the same label, the implementation prepends `V_` or `E_` in the class name:
* vertex with label `user` -> classname `V_user`
* edge with label `user` -> classname `E_user`

## Migrations
You might want to use [orientdb-migrations](https://github.com/springnz/orientdb-migrations) to create a schema with indexes etc. 

## Release
* upgrade version: remove SNAPSHOT (driver/pom.xml and tests-scala/build.sbt)
* commit on branch, push, create PR on github
* await green light from travis
* merge PR on github
* mvn pull
* mvn clean deploy -Prelease
* git tag VERSION
* bump versions to next SNAPSHOT (pom.xml, build.sbt)
* git push
* git push --tags
