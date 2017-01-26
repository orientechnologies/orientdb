package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OVertex;
import junit.framework.Assert;

import java.util.concurrent.CountDownLatch;

public class TestAsyncReplMode2ServersOverflow extends BareBoneBase2ServerTest {

  private static final int TOTAL   = 10000;
  CountDownLatch           counter = new CountDownLatch(2);

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2ServersOverflow";
  }

  protected void dbClient1() {
    // OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");
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
    if(graph.exists()){
      graph.open("admin", "admin");
    }else{
      graph.create();
    }


    try {
      int i = 0;
      for (; i < TOTAL; ++i) {
        final OVertex v = graph.newVertex();
        v.save();
        Assert.assertTrue(v.getIdentity().isPersistent());
      }

    } catch (Throwable e) {
      if (exceptionInThread == null)
        exceptionInThread = e;

    } finally {
      System.out.println("Shutting down");
      graph.close();
    }
  }
}
