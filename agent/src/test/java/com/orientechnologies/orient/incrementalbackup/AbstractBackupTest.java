/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.incrementalbackup;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;

/**
 * Abstract class for all test cases about the incremental backup.
 */
public abstract class AbstractBackupTest {

  protected final int numberOfThreads = 5;
  protected volatile int[] incrementalVerticesIdForThread;
  /*
   * A Callable task that inserts many vertices as indicated by the count variable on the specified server and cluster (shard).
   */

  public class VertexWriter implements Callable<Void> {
    protected final String databaseUrl;
    protected int          threadId;
    protected int          maxRetries;
    protected int          count;
    protected int          delayWriter;

    protected VertexWriter(final String db, final int iThreadId, int count) {
      this.databaseUrl = db;
      this.threadId = iThreadId;
      this.maxRetries = 1;
      this.count = count;
      this.delayWriter = 0;
    }

    @Override
    public Void call() throws Exception {

      try {

        String name = Integer.toString(threadId);
        OrientGraphFactory graphFactory = new OrientGraphFactory(databaseUrl);
        OrientBaseGraph graph = graphFactory.getNoTx();

        for (int i = 1; i <= count; i++) {

          int retry = 0;

          for (retry = 0; retry < maxRetries; retry++) {

            try {
              createVerticesWithEdge(graph, this.threadId);

              if (i % 1000 == 0)
                System.out.println("\nWriter (threadId=" + this.threadId + ") " + graph.getRawGraph().getURL() + " managed " + i + "/" + count + " triples so far");

              if (delayWriter > 0)
                Thread.sleep(delayWriter);

              // OK
              break;

            } catch (InterruptedException e) {
              System.out.println("Writer (threadId=" + this.threadId + ") received interrupt (db=" + graph.getRawGraph().getURL() + ")");
              Thread.currentThread().interrupt();
              break;
            } catch (ORecordDuplicatedException e) {
              // IGNORE IT
            } catch (ONeedRetryException e) {
              System.out.println("Writer (threadId=" + this.threadId + ") received exception (db=" + graph.getRawGraph().getURL() + ")");

              if (retry >= maxRetries)
                e.printStackTrace();

              break;
            } catch (ODistributedException e) {
              if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                graph.rollback();
                throw e;
              }
            } catch (Throwable e) {
              System.out.println("Writer (threadId=" + this.threadId + ") received exception (db=" + graph.getRawGraph().getURL() + ")");
              e.printStackTrace();
              return null;
            }
          }

        }

        graph.getRawGraph().close();

        System.out.println("\nWriter " + name + " END");
      }catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  // Atomic operations on the primaryGraph database
  protected OrientVertex createVerticesWithEdge(OrientBaseGraph graph, int threadId) {

    final String accountUniqueId = "User-t" + threadId + "-v" + incrementalVerticesIdForThread[threadId];
    final String productUniqueId = "Product-t" + threadId + "-v" + incrementalVerticesIdForThread[threadId];

    OrientVertex account = graph.addVertex("class:User");
    account.setProperties("name", accountUniqueId, "updated", false);
    account.save();

    OrientVertex product = graph.addVertex("class:Product");
    product.setProperties("name", productUniqueId, "updated", false);
    product.save();

    this.incrementThreadCount(threadId);

    OrientEdge edge = graph.addEdge(null, account, product, "bought");
    Date creationDate = new Date();
    edge.setProperties("purchaseDate", creationDate, "updated", true);
    edge.save();

    return account;
  }

  protected void updateVertex(OrientBaseGraph graph, OrientVertex vertex) {
    vertex.setProperty("updated", true);
    vertex.save();
  }

  protected void checkRecordIsDeleted(OrientBaseGraph graph, OrientVertex vertex) {
    try {
      vertex.reload();
      fail("Record found while it should be deleted");
    } catch (ORecordNotFoundException e) {
    }
  }

  // Increments the vertices id written from a thread identified through a threadId.
  protected synchronized void incrementThreadCount(int threadId) {
    this.incrementalVerticesIdForThread[threadId] = this.incrementalVerticesIdForThread[threadId]+1;
  }

  protected void banner(final String message) {
    System.out.println("\n**********************************************************************************************************");
    System.out.println(message);
    System.out.println("**********************************************************************************************************\n");
    System.out.flush();
  }

  // Writes the vertices on the database concurrently through several threads.
  public void executeWrites(String dbURL, int numberOfVertices) {

    try {

      ExecutorService writerExecutors = Executors.newCachedThreadPool();

      System.out.print("Creating writer workers...");

      Callable writer;
      List<Callable<Void>> writerWorkers = new ArrayList<Callable<Void>>();
      for (int threadId = 0; threadId < this.numberOfThreads; threadId++) {
        writer = new VertexWriter(dbURL, threadId, numberOfVertices);
        writerWorkers.add(writer);
      }

      System.out.println("\tDone.");
      List<Future<Void>> futures = writerExecutors.invokeAll(writerWorkers);

      System.out.println("Threads started, waiting for the end");

      for (Future<Void> future : futures) {
        future.get();
      }

      writerExecutors.shutdown();
      System.out.println("All writer threads have finished, shutting down readers");

    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  protected abstract String getDatabaseName();

}
