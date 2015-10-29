package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = OrientGraphProvider.class, graph = OrientGraph.class)
public class OrientGraphStructureStandardIT {
}
