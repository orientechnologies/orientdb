package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;
import org.junit.Ignore;

@Ignore
public class AsyncReplMode2Servers2OpsCommitConcurrentIT extends BareBoneBase2ServerTest {

  private static final int TOTAL = 5;
  private ORID vertex1Id;
  CountDownLatch counter = new CountDownLatch(2);

  @Override
  protected String getDatabaseName() {
    return "AsyncReplMode2Servers2OpsCommitConcurrentIT";
  }

  protected void dbClient1(BareBonesServer[] servers) {
    // OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");
    OrientDB orientdb = servers[0].getServer().getContext();
    orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
    ODatabaseDocument graph = orientdb.open(getDatabaseName(), "admin", "admin");
    OVertex vertex1 = graph.newVertex("vertextype");
    vertex1.save();
    graph.commit();
    graph.close();

    vertex1Id = vertex1.getIdentity();

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

    ODatabaseSession graph =
        servers[0].getServer().getContext().open(getDatabaseName(), "admin", "admin");

    OVertex vertex1 = ((OElement) graph.getRecord(vertex1Id)).asVertex().get();

    try {
      int i = 0;
      for (; i < TOTAL; ++i) {

        for (int retry = 0; retry < 20; ++retry) {
          try {
            OVertex vertex2 = graph.newVertex("vertextype");
            vertex1.addEdge(vertex2, "edgetype");
            vertex1.save();
            graph.commit();

            System.out.println(
                iClient
                    + " - successfully committed version: "
                    + vertex1.getRecord().getVersion()
                    + " retry: "
                    + retry);
            break;

          } catch (ONeedRetryException e) {
            System.out.println(
                iClient
                    + " - caught conflict, reloading vertex. v="
                    + vertex1.getRecord().getVersion()
                    + " retry: "
                    + retry);
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
      if (exceptionInThread == null) exceptionInThread = e;

    } finally {
      System.out.println("Shutting down");
      graph.close();
    }
  }
}
