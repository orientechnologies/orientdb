package com.orientechnologies.orient.graph.blueprints;

import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class BlueprintsConcurrentAddEdgeTest {
  private static final int                        THREADS     = 20;
  private static final int                        VERTICES    = 200;
  private static final int                        RETRIES     = 20;
  private static final String                     DBURL       = "local:target/databases/concurrenttestdb";

  private final ConcurrentHashMap<String, String> vertexReg   = new ConcurrentHashMap<String, String>();
  private final ConcurrentSkipListSet<String>     vertexSet   = new ConcurrentSkipListSet<String>();
  private final AtomicInteger                     edgeCounter = new AtomicInteger();

  public static void main(String args[]) {
    new BlueprintsConcurrentAddEdgeTest().test();
  }

  public BlueprintsConcurrentAddEdgeTest() {
  }

  @Test
  public void test() {
    createGraph();
    try {

      insertVertices();
      cacheVerticesIds();
      createEdgesConcurrently();

    } finally {
      getGraph().getRawGraph().drop();
    }
  }

  private void createEdgesConcurrently() {
    Thread myThreads[] = new Thread[THREADS];

    for (int i = 0; i < THREADS; ++i) {
      Thread t = new Thread(new EdgeRunnerNoTx());
      myThreads[i] = t;
      t.start();
    }

    for (Thread t1 : myThreads) {
      try {
        t1.join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (NullPointerException ex) {

      }
    }

    System.out.println("Done adding edges" + new Date().getTime() + " Thread:" + Thread.currentThread().getName());
  }

  private OrientGraphNoTx createGraph() {
    OrientGraphNoTx graph = null;
    try {
      graph = new OrientGraphNoTx(DBURL);
      if (graph.getRawGraph().exists())
        graph.getRawGraph().drop();
      else
        graph.shutdown();

      graph = getGraph();

      graph.createVertexType("CustomVertex");
      graph.createEdgeType("CustomEdge");

    } catch (OSchemaException ex) {

    }
    return graph;
  }

  private void cacheVerticesIds() {
    OrientGraphNoTx graph2 = getGraph();
    try {
      Iterable<OrientVertex> result = graph2.command(new OSQLSynchQuery<ODocument>("select from CustomVertex")).execute();

      for (OrientVertex vRow : result) {
        try {
          vertexReg.put(vRow.getProperty("name").toString(), vRow.getIdentity().toString());
        } catch (NullPointerException ex) {
          // System.err.println("Some problem");
        }
      }

    } finally {
      graph2.shutdown();
    }
    System.out.println("Finished creating registry" + vertexReg);

    vertexSet.addAll(vertexReg.keySet());
  }

  private void insertVertices() {
    System.out.println("Start insert vertices");
    OrientGraphNoTx graph = getGraph();
    // create vertices
    try {

      OrientVertex rootNode = graph.addVertex("class:CustomVertex", "name", "root");
      Random r = new Random();

      for (int i = 0; i < VERTICES; i++) {
        graph.addVertex("class:CustomVertex", "name", "SomeVertex_" + r.nextInt(VERTICES));
      }

      Assert.assertEquals(graph.countVertices("CustomVertex"), VERTICES + 1);

      graph.getRawGraph().getDictionary().put("RootNode", rootNode.getRecord());
    } finally {
      graph.shutdown();
    }
    System.out.println("Finish insert vertices");
  }

  class EdgeRunnerNoTx implements Runnable {

    @Override
    public void run() {
      System.out.println("Start adding edges" + new Date().getTime() + " Thread:" + Thread.currentThread().getName());
      OrientGraphNoTx databaseNoTx = getGraph();

      while (!vertexSet.isEmpty()) {
        String keyFrom = vertexSet.pollFirst();
        Set<String> toKeySet = vertexReg.keySet();

        ODatabaseRecordThreadLocal.INSTANCE.set(databaseNoTx.getRawGraph());
        final int total = toKeySet.size();
        int current = 0;

        for (String keyTo : toKeySet) {
          boolean success = false;
          for (int retry = 0; retry < RETRIES; retry++) {
            try {
              OrientVertex iSourceVertex = databaseNoTx.getVertex(vertexReg.get(keyFrom));
              OrientVertex iDestVertex = databaseNoTx.getVertex(vertexReg.get(keyTo));
              databaseNoTx.addEdge(null, iSourceVertex, iDestVertex, "CustomEdge");

              // OK
              if (retry > 2)
                System.out.println("OK (key #" + current + "/" + total + ") after " + retry + " retries - Thread:"
                    + Thread.currentThread().getName());
              success = true;
              current++;

              final int totalEdges = edgeCounter.incrementAndGet();

              if (totalEdges % 10000 == 0)
                System.out.println("Inserted edges: " + totalEdges + ", currentThread " + current + " Thread:"
                    + Thread.currentThread().getName());

              break;

            } catch (OConcurrentModificationException e) {
              if (retry > 2)
                System.out.println("Managing concurrent exception (key #" + current + "/" + total + ") adding edge " + keyFrom
                    + "->" + keyTo + ", retry " + retry + " Thread:" + Thread.currentThread().getName());
            } catch (Exception e) {
              System.err.println("Exception (key #" + current + "/" + total + ") adding edge " + keyFrom + "->" + keyTo
                  + ", retry " + retry + " Thread:" + Thread.currentThread().getName());
              e.printStackTrace();
            }
          }

          if (!success)
            System.out.println("ERROR on (key #" + current + "/" + total + ") adding edge " + keyFrom + "->" + keyTo + " Thread:"
                + Thread.currentThread().getName());

        }
      }

      System.out.println("Done adding edges" + new Date().getTime() + " Thread:" + Thread.currentThread().getName());

    }
  }

  private OrientGraphNoTx getGraph() {
    OrientGraphNoTx graph = new OrientGraphNoTx(DBURL);
    graph.getRawGraph().getLevel1Cache().setEnable(false);
    graph.getRawGraph().getLevel2Cache().setEnable(false);
    return graph;
  }

}
