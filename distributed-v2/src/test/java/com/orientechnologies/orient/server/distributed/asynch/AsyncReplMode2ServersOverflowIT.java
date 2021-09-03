package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.concurrent.CountDownLatch;
import junit.framework.Assert;

public class AsyncReplMode2ServersOverflowIT extends BareBoneBase2ServerTest {

  private static final int TOTAL = 10000;
  CountDownLatch counter = new CountDownLatch(2);

  @Override
  protected String getDatabaseName() {
    return "AsyncReplMode2ServersOverflowIT";
  }

  protected void dbClient1(BareBonesServer[] servers) {
    // OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");
    exec("client1", servers);
  }

  protected void dbClient2(BareBonesServer[] servers) {
    exec("client2", servers);
  }

  protected void exec(final String iClient, BareBonesServer[] servers) {
    counter.countDown();

    try {
      counter.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    OrientDB orientdb = servers[0].getServer().getContext();
    orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
    ODatabaseDocument graph = orientdb.open(getDatabaseName(), "admin", "admin");

    try {
      int i = 0;
      for (; i < TOTAL; ++i) {
        final OVertex v = graph.newVertex();
        v.save();
        Assert.assertTrue(v.getIdentity().isPersistent());
      }

    } catch (Throwable e) {
      if (exceptionInThread == null) exceptionInThread = e;

    } finally {
      System.out.println("Shutting down");
      graph.close();
    }
  }
}
