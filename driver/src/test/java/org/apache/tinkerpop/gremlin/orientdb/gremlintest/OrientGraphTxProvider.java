package org.apache.tinkerpop.gremlin.orientdb.gremlintest;

import static java.util.Arrays.asList;
import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_TRANSACTIONAL;

import java.util.Map;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.GraphTest;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.apache.tinkerpop.gremlin.structure.VertexTest;

public class OrientGraphTxProvider extends OrientGraphProvider {

  static {
    IGNORED_TESTS.put(
        TransactionTest.class,
        asList(
            "shouldExecuteWithCompetingThreads",
            "shouldAllowReferenceOfEdgeIdOutsideOfOriginalThreadManual",
            "shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadManual",
            "shouldSupportTransactionIsolationCommitCheck",
            "shouldNotShareTransactionReadWriteConsumersAcrossThreads",
            "shouldNotShareTransactionCloseConsumersAcrossThreads",
            "shouldNotifyTransactionListenersInSameThreadOnlyOnCommitSuccess",
            "shouldNotifyTransactionListenersInSameThreadOnlyOnRollbackSuccess"));

    IGNORED_TESTS.put(GraphTest.class, asList("shouldRemoveVertices"));
    IGNORED_TESTS.put(
        VertexTest.BasicVertexTest.class, asList("shouldNotGetConcurrentModificationException"));
  }

  @Override
  public Map<String, Object> getBaseConfiguration(
      String graphName,
      Class<?> test,
      String testMethodName,
      LoadGraphWith.GraphData loadGraphWith) {
    Map<String, Object> baseConfiguration =
        super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
    baseConfiguration.put(CONFIG_TRANSACTIONAL, true);
    return baseConfiguration;
  }
}
