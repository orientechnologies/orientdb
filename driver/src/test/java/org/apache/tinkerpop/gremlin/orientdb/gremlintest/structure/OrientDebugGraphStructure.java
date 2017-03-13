package org.apache.tinkerpop.gremlin.orientdb.gremlintest.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientGraphProvider;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.suite.OrientDBDebugSuite;
import org.junit.runner.RunWith;

/**
 * Created by Enrico Risa on 10/11/16.
 */

@RunWith(OrientDBDebugSuite.class)
@GraphProviderClass(provider = OrientGraphProvider.class, graph = OrientGraph.class)
public class OrientDebugGraphStructure {
}
