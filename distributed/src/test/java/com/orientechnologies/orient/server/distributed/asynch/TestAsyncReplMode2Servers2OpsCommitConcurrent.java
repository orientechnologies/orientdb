package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;

public class TestAsyncReplMode2Servers2OpsCommitConcurrent extends BareBoneBase2ServerTest {

  private static final int TOTAL   = 50;
  private ORID             vertex1Id;
  CountDownLatch           counter = new CountDownLatch(2);

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2Servers2OpsCommitConcurrent";
  }

  protected void dbClient1() {
    // OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getLocalURL());
    if(graph.exists()){
      graph.open("admin", "admin");
    }else{
      graph.create();
    }
    OSchema schema = graph.getMetadata().getSchema();
    schema.createClass("vertextype", schema.getClass("V"));
    OVertex vertex1 = graph.newVertex("vertextype");
    vertex1.save();
    graph.commit();
    graph.close();

    vertex1Id = vertex1.getIdentity();

    exec("client1");
  }

  protected void dbClient2() {
    exec("client2");
  }

  protected void exec(final String iClient) {
    counter.countDown();

    try {
      counter.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getLocalURL());
    graph.open("admin", "admin");

    OVertex vertex1 = ((OElement)graph.getRecord(vertex1Id)).asVertex().get();

    try {
      int i = 0;
      for (; i < TOTAL; ++i) {

        for (int retry = 0; retry < 20; ++retry) {
          try {
            OVertex vertex2 = graph.newVertex("vertextype");
            vertex1.addEdge( vertex2,"edgetype");
            vertex1.save();
            graph.commit();

            System.out
                .println(iClient + " - successfully committed version: " + vertex1.getRecord().getVersion() + " retry: " + retry);
            break;

          } catch (ONeedRetryException e) {
            System.out.println(
                iClient + " - caught conflict, reloading vertex. v=" + vertex1.getRecord().getVersion() + " retry: " + retry);
            graph.rollback();
            vertex1.reload();
          }
        }
      }

      // STATISTICALLY HERE AT LEAST ONE CONFLICT HAS BEEN RECEIVED
      vertex1.reload();

      Assert.assertTrue(vertex1.getRecord().getVersion() > TOTAL + 1);
      Assert.assertEquals(TOTAL, i);

    } catch (Throwable e) {
      if (exceptionInThread == null)
        exceptionInThread = e;

    } finally {
      System.out.println("Shutting down");
      graph.close();
    }
  }
}
