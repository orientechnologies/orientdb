package org.apache.tinkerpop.gremlin.orientdb.gremlintest.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientGraphTxProvider;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.suite.OrientDBDebugSuite;
import org.junit.runner.RunWith;

/** Created by Enrico Risa on 10/11/16. */
@RunWith(OrientDBDebugSuite.class)
@GraphProviderClass(provider = OrientGraphTxProvider.class, graph = OrientGraph.class)
// @GraphProviderClass(provider = OrientStandardGraphProvider.class, graph =
// OrientStandardGraph.class)
public class OrientDebugGraphStructure {}
