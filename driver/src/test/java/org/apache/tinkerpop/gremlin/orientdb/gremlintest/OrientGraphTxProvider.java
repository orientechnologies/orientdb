package org.apache.tinkerpop.gremlin.orientdb.gremlintest;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_TRANSACTIONAL;

public class OrientGraphTxProvider extends OrientGraphProvider {

  static {
    IGNORED_TESTS.put(TransactionTest.class,
        asList("shouldExecuteWithCompetingThreads", "shouldAllowReferenceOfEdgeIdOutsideOfOriginalThreadManual",
            "shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadManual", "shouldSupportTransactionIsolationCommitCheck",
            "shouldNotShareTransactionReadWriteConsumersAcrossThreads", "shouldNotShareTransactionCloseConsumersAcrossThreads",
            "shouldNotifyTransactionListenersInSameThreadOnlyOnCommitSuccess",
            "shouldNotifyTransactionListenersInSameThreadOnlyOnRollbackSuccess"));
  }

  @Override
  public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
      LoadGraphWith.GraphData loadGraphWith) {
    Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
    baseConfiguration.put(CONFIG_TRANSACTIONAL, true);
    return baseConfiguration;
  }
}
