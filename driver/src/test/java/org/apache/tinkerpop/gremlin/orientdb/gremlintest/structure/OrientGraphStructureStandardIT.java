package org.apache.tinkerpop.gremlin.orientdb.gremlintest.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientGraphProvider;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

/**
 * Executes the Standard Gremlin Structure Test Suite using OrientGraph.
 *
 * <p>Extracted from TinkerGraph tests
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marvin Froeder (about.me/velo)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = OrientGraphProvider.class, graph = OrientGraph.class)
public class OrientGraphStructureStandardIT {}
