# orientdb-gremlin
TP3 driver for orientdb

For now this is just a proof of concept, nowhere ready to use in production. But the structure is there, and it
supports basic traversals. The easiest way to give it a shot is to use the orientdb project inside
[gremlin-scala-examples]([https://github.com/mpollmeier/gremlin-scala-examples).

## Tests...
... are written using Gremlin-Scala and can be run inside the `tests-scala` directory using `sbt test` This does not replace the standard TP3 testsuite for a graph vendor, but I didn't have the time to implement that. 

If you run into a `NotImplementedException()` then it's a sign that you should help ;)
