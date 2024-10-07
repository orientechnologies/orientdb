package org.apache.tinkerpop.gremlin.orientdb.gremlintest.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientStandardGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientStandardGraphProvider;

/** Created by Enrico Risa on 01/09/2017. */

// @RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = OrientStandardGraphProvider.class, graph = OrientStandardGraph.class)
public class OrientStandardGraphProcessStandardTest {}
