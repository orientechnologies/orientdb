package org.apache.tinkerpop.gremlin.orientdb.gremlintest.integration;

/** Created by Enrico Risa on 01/09/2017. */
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientStandardGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientStandardGraphProvider;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = OrientStandardGraphProvider.class, graph = OrientStandardGraph.class)
public class OrientStandardGraphStructureIntegrateTest {}
