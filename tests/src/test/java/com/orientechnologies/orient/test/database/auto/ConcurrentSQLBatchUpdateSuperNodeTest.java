/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ConcurrentSQLBatchUpdateSuperNodeTest extends DocumentDBBaseTest {

  private static final int OPTIMISTIC_CYCLES = 30;
  private static final int PESSIMISTIC_CYCLES = 30;
  private static final int THREADS = 256;
  private static final int MAX_RETRIES = 100;
  private final AtomicLong counter = new AtomicLong();
  private boolean mvccEnabled;
  private long startedOn;
  private AtomicLong totalRetries = new AtomicLong();

  @Parameters(value = "url")
  public ConcurrentSQLBatchUpdateSuperNodeTest(@Optional String url) {
    super(url);
  }

  class OptimisticThread implements Runnable {

    final String url;
    final OrientVertex superNode;
    final int threadId;
    final String threadName;

    public OptimisticThread(
        final String iURL, OrientVertex iSuperNode, int iThreadId, String iThreadName) {
      super();
      url = iURL;
      superNode = iSuperNode;
      threadId = iThreadId;
      threadName = iThreadName;
    }

    @Override
    public void run() {
      final OrientGraphNoTx graph = new OrientGraphNoTx(url);
      try {
        String cmd = "";
        for (int i = 0; i < OPTIMISTIC_CYCLES; ++i) {
          cmd = "begin;";
          cmd += "let a = create vertex set type = 'Citizen', id = '" + threadId + "-" + i + "';";
          cmd += "create edge from $a to " + superNode.getIdentity() + ";";
          cmd += "commit retry " + MAX_RETRIES + ";";
          cmd += "return $transactionRetries;";

          final OCommandRequest command = graph.command(new OCommandScript("sql", cmd));
          final Object res = command.execute();
          if (res instanceof Integer) {
            int retries = (Integer) res;

            counter.incrementAndGet();

            totalRetries.addAndGet(retries);
          } else if (res instanceof OrientDynaElementIterable) {
            //            System.out.println("RETURNED ITER");
            OrientDynaElementIterable it = (OrientDynaElementIterable) res;
            for (Object o : it) ;
            //              System.out.println("RETURNED: " + o);
          }
        }

        //        System.out.println("Thread " + threadId + " completed");

        graph.shutdown();
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  class PessimisticThread implements Runnable {

    final String url;
    final OrientVertex superNode;
    final int threadId;
    final String threadName;

    public PessimisticThread(
        final String iURL, OrientVertex iSuperNode, int iThreadId, String iThreadName) {
      super();
      url = iURL;
      superNode = iSuperNode;
      threadId = iThreadId;
      threadName = iThreadName;
    }

    @Override
    public void run() {
      final OrientGraphNoTx graph = new OrientGraphNoTx(url);
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
        graph.shutdown();

      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  @Test(enabled = true)
  public void concurrentOptimisticUpdates() throws Exception {
    //    System.out.println("Started Test OPTIMISTIC Batch Update against SuperNode");

    counter.set(0);
    startedOn = System.currentTimeMillis();

    OrientBaseGraph graphPool = new OrientGraph(url);

    OrientVertex superNode = graphPool.addVertex(null, "optimisticSuperNode", true);
    graphPool.commit();

    OptimisticThread[] ops = new OptimisticThread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new OptimisticThread(url, superNode, i, "thread" + i);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(ops[i], "ConcurrentSQLBatchUpdateSuperNodeTest-optimistic" + i);

    //    System.out.println("Starting " + THREADS + " threads, " + OPTIMISTIC_CYCLES + " operations
    // each");

    for (int i = 0; i < THREADS; ++i) threads[i].start();

    for (int i = 0; i < THREADS; ++i) {
      threads[i].join();
      //      System.out.println("Thread " + i + " completed");
    }

    //    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Optimistic Done! Total updates
    // executed in parallel: "
    //        + counter.get() + " total retries: " + totalRetries.get() + " average retries: "
    //        + ((float) totalRetries.get() / (float) counter.get()));

    Assert.assertEquals(counter.get(), OPTIMISTIC_CYCLES * THREADS);

    OrientVertex loadedSuperNode = graphPool.getVertex(superNode.getIdentity());

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(loadedSuperNode.countEdges(Direction.IN), OPTIMISTIC_CYCLES * THREADS);

    graphPool.shutdown();

    //    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Optimistic Test completed in "
    //        + (System.currentTimeMillis() - startedOn));
  }

  @Test(enabled = false)
  public void concurrentPessimisticUpdates() throws Exception {
    //    System.out.println("Started Test PESSIMISTIC Batch Update against SuperNode");

    counter.set(0);
    startedOn = System.currentTimeMillis();

    OrientBaseGraph graphPool = new OrientGraph(url);

    graphPool.setThreadMode(OrientBaseGraph.THREAD_MODE.ALWAYS_AUTOSET);
    OrientVertex superNode = graphPool.addVertex(null, "pessimisticSuperNode", true);
    graphPool.commit();

    PessimisticThread[] ops = new PessimisticThread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new PessimisticThread(url, superNode, i, "thread" + i);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(ops[i], "ConcurrentSQLBatchUpdateSuperNodeTest-pessimistic" + i);

    //    System.out.println("Starting " + THREADS + " threads, " + PESSIMISTIC_CYCLES + "
    // operations each");

    for (int i = 0; i < THREADS; ++i) threads[i].start();

    for (int i = 0; i < THREADS; ++i) {
      threads[i].join();
      //      System.out.println("Thread " + i + " completed");
    }

    //    System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Pessimistic Done! Total updates
    // executed in parallel: "
    //        + counter.get() + " average retries: " + ((float) totalRetries.get() / (float)
    // counter.get()));

    Assert.assertEquals(counter.get(), PESSIMISTIC_CYCLES * THREADS);

    OrientVertex loadedSuperNode = graphPool.getVertex(superNode.getIdentity());

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(loadedSuperNode.countEdges(Direction.IN), PESSIMISTIC_CYCLES * THREADS);

    graphPool.shutdown();

    // System.out.println("ConcurrentSQLBatchUpdateSuperNodeTest Pessimistic Test completed in "
    // + (System.currentTimeMillis() - startedOn));
  }
}
