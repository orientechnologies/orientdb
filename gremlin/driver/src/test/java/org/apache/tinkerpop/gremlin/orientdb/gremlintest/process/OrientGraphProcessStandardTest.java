package org.apache.tinkerpop.gremlin.orientdb.gremlintest.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientGraphTxProvider;

/** Created by Enrico Risa on 01/09/2017. */

// @RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = OrientGraphTxProvider.class, graph = OrientGraph.class)
public class OrientGraphProcessStandardTest {}
