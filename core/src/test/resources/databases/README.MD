**Testing import / export**

The `databases_x` files are OrientDB exports in different databas eversions and used in several import / export and compatibility tests.

Import steps / phases:
- [x] Info
- [x] Clusters
- [ ] Schema
- [ ] Records
- [ ] Broken RIDs
- [ ] Indexes
- [ ] (Legacy: manual indexes)

Topics:
- [x] JSON parser selection
- [ ] Backpressure / JavaRx
- [ ] Batch insert / parallel import
- [ ] State machine refactoring

TODOs:
- [x] Add compatibility tests
    - [x] version `2.2.x` vs. `3.1.x`
- [x] Evaluate existing, standard parsers (JSON parsers in Java are quite behind the current state.
Selected Jackson streaming API due to its better performance - especially for larger JSON documents.)
- [ ] Replace custom parser by standard one - WIP
- [ ] Benchmark custom vs. Jackson performance
- [ ] ...
- [ ] Check "large number of edges between nodes" problem
- [ ] ...
- [ ] Check memory consumption
- [ ] ...
- [ ] Go through open customer tickets / issues

 
 


