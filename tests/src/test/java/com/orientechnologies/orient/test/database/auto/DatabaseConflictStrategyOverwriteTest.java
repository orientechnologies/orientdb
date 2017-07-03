package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.conflict.OOverwriteConflictStrategy;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

public class DatabaseConflictStrategyOverwriteTest {

  private static final String CLIENT_ORIENT_URL_MAIN = "memory:DatabaseConflictStrategyOverwriteTest";

  private volatile Throwable exceptionInThread;

  CountDownLatch initLatch = new CountDownLatch(2);

  CountDownLatch firstFinished = new CountDownLatch(1);

  OrientGraphFactory factory;

  @Test
  public void test() throws Throwable {
    factory = new OrientGraphFactory(CLIENT_ORIENT_URL_MAIN);
    OrientBaseGraph graph = factory.getNoTx();
    graph.setConflictStrategy(OOverwriteConflictStrategy.NAME);
    graph.createVertexType("vertextype");
    final ORID rid = graph.addVertex("class:vertextype").getIdentity();
    graph.shutdown();

    Thread dbClient1 = new Thread() {
      @Override
      public void run() {
        dbClient1(rid);
      }
    };
    dbClient1.start();
    
    Thread dbClient2 = new Thread() {
      @Override
      public void run() {
        dbClient2(rid);
      }
    };
    dbClient2.start();

    dbClient1.join();
    dbClient2.join();

    graph = factory.getNoTx();

    OrientVertex vertex = graph.getVertex(rid);
    vertex.reload();
    Assert.assertEquals(vertex.getProperty("foo"), "woo");
    Assert.assertNull(vertex.getProperty("bar"));

    graph.shutdown();
    factory.close();
    if (exceptionInThread != null) {
      throw exceptionInThread;
    }
  }

  private void dbClient1(ORID rid) {
    OrientGraphNoTx db = factory.getNoTx();
    try {
      OrientVertex vertex = db.getVertex(rid);
      initLatch.countDown();
      vertex.setProperty("foo", "bar");
      vertex.setProperty("bar", "baz");
      vertex.save();

      vertex.setProperty("foo", "baz");
      vertex.save();

      firstFinished.countDown();
    } catch (Exception e) {
      exceptionInThread = e;
    } finally {
      db.shutdown();
    }

  }

  private void dbClient2(ORID rid) {
    OrientGraphNoTx db = factory.getNoTx();
    try {
      OrientVertex vertex = db.getVertex(rid);
      initLatch.countDown();

      firstFinished.await();
      vertex.setProperty("foo", "woo");
      vertex.save();
    } catch (Exception e) {
      exceptionInThread = e;
    } finally {
      db.shutdown();
    }
  }

  private static void sleep(int i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException xcpt) {
      xcpt.printStackTrace();
    }
  }

}
