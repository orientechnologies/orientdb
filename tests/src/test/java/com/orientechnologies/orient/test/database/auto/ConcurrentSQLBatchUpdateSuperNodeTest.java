/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;

@Test
public class ConcurrentSQLBatchUpdateSuperNodeTest extends DocumentDBBaseTest {

  private final static int OPTIMISTIC_CYCLES  = 100;
  private final static int PESSIMISTIC_CYCLES = 100;
  private final static int THREADS            = 10;
  private final static int MAX_RETRIES        = 100;
  private final AtomicLong counter            = new AtomicLong();
  private boolean          localCacheEnabled;
  private boolean          mvccEnabled;
  private long             startedOn;
  private AtomicLong       totalRetries       = new AtomicLong();

  @Parameters(value = "url")
  public ConcurrentSQLBatchUpdateSuperNodeTest(@Optional String url) {
    super(url);
  }

  class OptimisticThread implements Runnable {

    final OrientBaseGraph graph;
    final OrientVertex    superNode;
    final int             threadId;
    final String          threadName;

    public OptimisticThread(OrientBaseGraph iGraph, OrientVertex iSuperNode, int iThreadId, String iThreadName) {
      super();
      graph = iGraph;
      superNode = iSuperNode;
      threadId = iThreadId;
      threadName = iThreadName;
    }

    @Override
    public void run() {
      try {
        String cmd = "";
        for (int i = 0; i < OPTIMISTIC_CYCLES; ++i) {
          cmd = "begin;";
          cmd += "let a = create vertex set type = 'Citizen', id = '" + threadId + "-" + i + "';";
          cmd += "create edge from $a to " + superNode.getIdentity() + ";";
          cmd += "commit retry " + MAX_RETRIES + ";";
          cmd += "return $transactionRetries;";

          final OCommandRequest command = graph.command(new OCommandScript("sql", cmd));
          int retries = (Integer) command.execute();

          counter.incrementAndGet();

          totalRetries.addAndGet(retries);
        }
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  class PessimisticThread implements Runnable {

    final OrientBaseGraph graph;
    final OrientVertex    superNode;
    final int             threadId;
    final String          threadName;

    public PessimisticThread(OrientBaseGraph iGraph, OrientVertex iSuperNode, int iThreadId, String iThreadName) {
      super();
      graph = iGraph;
      superNode = iSuperNode;
      threadId = iThreadId;
      threadName = iThreadName;
    }

    @Override
    public void run() {
      try {
        String cmd = "";
        for (int i = 0; i < PESSIMISTIC_CYCLES; ++i) {
          cmd = "begin;";
          cmd += "select from " + superNode.getIdentity() + " lock record;";
          cmd += "let a = create vertex set type = 'Citizen', id = '" + threadId + "-" + i + "';";
          cmd += "create edge from $a to " + superNode.getIdentity() + ";";
          cmd += "commit;";

          Object result = graph.command(new OCommandScript("sql", cmd)).execute();

          counter.incrementAndGet();
        }
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  @BeforeClass
  public void init() {
    localCacheEnabled = OGlobalConfiguration.CACHE_LOCAL_ENABLED.getValueAsBoolean();
    mvccEnabled = OGlobalConfiguration.DB_MVCC.getValueAsBoolean();

    if (localCacheEnabled)
      OGlobalConfiguration.CACHE_LOCAL_ENABLED.setValue(false);
    if (!mvccEnabled)
      OGlobalConfiguration.DB_MVCC.setValue(true);
  }

  @AfterClass
  public void deinit() {
    OGlobalConfiguration.CACHE_LOCAL_ENABLED.setValue(localCacheEnabled);
    OGlobalConfiguration.DB_MVCC.setValue(mvccEnabled);
  }

  @Test(enabled = true)
  public void concurrentOptimisticUpdates() throws Exception {
    System.out.println("Started Test OPTIMISTIC Batch Update against SuperNode");

    counter.set(0);
    startedOn = System.currentTimeMillis();

    OrientBaseGraph[] graphPool = new OrientGraph[THREADS];
    for (int i = 0; i < THREADS; ++i)
      graphPool[i] = new OrientGraph(url);

    graphPool[0].setThreadMode(OrientBaseGraph.THREAD_MODE.ALWAYS_AUTOSET);
    OrientVertex superNode = graphPool[0].addVertex(null, "optimisticSuperNode", true);
    graphPool[0].commit();

    OptimisticThread[] ops = new OptimisticThread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new OptimisticThread(graphPool[i], superNode, i, "thread" + i);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(ops[i], "ConcurrentSQLBatchUpdateSuperNodeTest-optimistic" + i);

    for (int i = 0; i < THREADS; ++i)
      threads[i].start();

    for (int i = 0; i < THREADS; ++i)
      threads[i].join();

    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Optimistic Done! Total updates executed in parallel: "
        + counter.get() + " total retries: " + totalRetries.get() + " average retries: "
        + ((float) totalRetries.get() / (float) counter.get()));

    Assert.assertEquals(counter.get(), OPTIMISTIC_CYCLES * THREADS);

    OrientVertex loadedSuperNode = graphPool[0].getVertex(superNode.getIdentity());

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(loadedSuperNode.countEdges(Direction.IN), PESSIMISTIC_CYCLES * THREADS);

    for (int i = 0; i < THREADS; ++i)
      graphPool[i].shutdown();

    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Optimistic Test completed in "
        + (System.currentTimeMillis() - startedOn));
  }

  @Test
  public void concurrentPessimisticUpdates() throws Exception {
    System.out.println("Started Test PESSIMISTIC Batch Update against SuperNode");

    counter.set(0);
    startedOn = System.currentTimeMillis();

    OrientBaseGraph[] graphPool = new OrientGraph[THREADS];
    for (int i = 0; i < THREADS; ++i)
      graphPool[i] = new OrientGraph(url);

    graphPool[0].setThreadMode(OrientBaseGraph.THREAD_MODE.ALWAYS_AUTOSET);
    OrientVertex superNode = graphPool[0].addVertex(null, "pessimisticSuperNode", true);
    graphPool[0].commit();

    PessimisticThread[] ops = new PessimisticThread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new PessimisticThread(graphPool[i], superNode, i, "thread" + i);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(ops[i], "ConcurrentSQLBatchUpdateSuperNodeTest-pessimistic" + i);

    for (int i = 0; i < THREADS; ++i)
      threads[i].start();

    for (int i = 0; i < THREADS; ++i)
      threads[i].join();

    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Pessimistic Done! Total updates executed in parallel: "
        + counter.get() + " average retries: " + ((float) totalRetries.get() / (float) counter.get()));

    Assert.assertEquals(counter.get(), PESSIMISTIC_CYCLES * THREADS);

    OrientVertex loadedSuperNode = graphPool[0].getVertex(superNode.getIdentity());

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(loadedSuperNode.countEdges(Direction.IN), PESSIMISTIC_CYCLES * THREADS);

    for (int i = 0; i < THREADS; ++i)
      graphPool[i].shutdown();

    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Pessimistic Test completed in "
        + (System.currentTimeMillis() - startedOn));
  }
}
