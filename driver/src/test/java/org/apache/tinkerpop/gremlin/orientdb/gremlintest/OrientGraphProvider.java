package org.apache.tinkerpop.gremlin.orientdb.gremlintest;

import static java.util.Arrays.asList;
import static org.junit.Assume.assumeFalse;

import com.google.common.collect.Sets;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.io.File;
import java.util.*;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.orientdb.*;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.FeatureSupportTest.GraphFunctionalityTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.GraphTest;
import org.apache.tinkerpop.gremlin.structure.SerializationTest;
import org.apache.tinkerpop.gremlin.structure.io.IoCustomTest;
import org.junit.AssumptionViolatedException;

public class OrientGraphProvider extends AbstractGraphProvider {

  static {
    File buildDir = new File("target/builddir");
    buildDir.mkdirs();
    System.setProperty("build.dir", buildDir.getAbsolutePath());
  }

  protected static final Map<Class<?>, List<String>> IGNORED_TESTS;

  static {
    IGNORED_TESTS = new HashMap<>();
    IGNORED_TESTS.put(
        GraphTest.class,
        asList(
            "shouldNotMixTypesForGettingSpecificEdgesWithStringFirst",
            "shouldNotMixTypesForGettingSpecificEdgesWithEdgeFirst",
            "shouldNotMixTypesForGettingSpecificVerticesWithStringFirst",
            "shouldNotMixTypesForGettingSpecificVerticesWithVertexFirst"));

    // OrientDB can not modify schema when the transaction is on, which
    // break the tests
    IGNORED_TESTS.put(
        GraphFunctionalityTest.class, asList("shouldSupportTransactionsIfAGraphConstructsATx"));

    // This tests become broken after gremlin 3.2.4
    IGNORED_TESTS.put(
        SerializationTest.GraphSONV3d0Test.class, asList("shouldSerializeTraversalMetrics"));

    // This tests become broken after gremlin 3.2.0
    IGNORED_TESTS.put(SerializationTest.GryoV1d0Test.class, asList("shouldSerializeTree"));
    IGNORED_TESTS.put(SerializationTest.GryoV3d0Test.class, asList("shouldSerializeTree"));

    IGNORED_TESTS.put(IoCustomTest.class, asList("shouldSerializeTree"));
  }

  @Override
  public Map<String, Object> getBaseConfiguration(
      String graphName,
      Class<?> test,
      String testMethodName,
      LoadGraphWith.GraphData loadGraphWith) {
    if (IGNORED_TESTS.containsKey(test) && IGNORED_TESTS.get(test).contains(testMethodName))
      throw new AssumptionViolatedException("We allow mixed ids");

    if (testMethodName.contains("graphson-v1-embedded"))
      throw new AssumptionViolatedException("graphson-v1-embedded support not implemented");

    HashMap<String, Object> configs = new HashMap<String, Object>();
    configs.put(Graph.GRAPH, OrientGraph.class.getName());
    configs.put("name", graphName);
    if (testMethodName.equals("shouldPersistDataOnClose"))
      configs.put(
          OrientGraph.CONFIG_URL,
          "plocal:./target/databases/test-"
              + graphName
              + "-"
              + test.getSimpleName()
              + "-"
              + testMethodName);

    configs.put(OrientGraph.CONFIG_POOL_SIZE, 20);

    return configs;
  }

  @SuppressWarnings({"rawtypes"})
  @Override
  public Set<Class> getImplementations() {
    return Sets.newHashSet(
        OrientEdge.class,
        OrientElement.class,
        OrientGraph.class,
        OrientProperty.class,
        OrientVertex.class,
        OrientVertexProperty.class);
  }

  @Override
  public void clear(Graph graph, Configuration configuration) throws Exception {
    if (graph != null) {
      OrientGraph g = (OrientGraph) graph;
      if (!g.isClosed()) {
        g.drop();
      }
    }
  }

  @Override
  public Graph openTestGraph(Configuration config) {
    if (config.getString("name").equals("readGraph"))
      // FIXME eventually ne need to get ride of this
      assumeFalse(
          "there is some technical limitation on orientDB that makes tests enter in an infinite loop when reading and writing to orientDB",
          true);

    return super.openTestGraph(config);
  }

  @Override
  public ORID convertId(Object id, Class<? extends Element> c) {
    if (id instanceof Number) {
      long numericId = ((Number) id).longValue();
      return new ORecordId(new Random(numericId).nextInt(32767), numericId);
    }
    if (id instanceof String) {
      Integer numericId;
      try {
        numericId = new Integer(id.toString());
      } catch (NumberFormatException e) {
        return new MockORID("Invalid id: " + id + " for " + c);
      }
      return new ORecordId(numericId, numericId.longValue());
    }

    return new MockORID("Invalid id: " + id + " for " + c);
  }
}
