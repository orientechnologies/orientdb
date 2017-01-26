package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;

public class TestAsyncReplMode2ServersAddEdge extends BareBoneBase2ServerTest {

  private static final int NUM_OF_LOOP_ITERATIONS = 100;

  private Object           parentV1Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2ServersAddEdge";
  }

  protected void dbClient1() {
    OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    synchronized (LOCK) {
      ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getLocalURL());
      if(graph.exists()){
        graph.open("admin", "admin");
      }else{
        graph.create();
        graph.createClass("vertextype", "V");
        graph.createClass("edgetype", "E");
      }
      graph.begin();
      try {
        OVertex parentV1 = graph.newVertex("vertextype");
        parentV1.save();
        graph.commit();
        graph.begin();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getIdentity();

        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          OVertex childV = graph.newVertex("vertextype");
          childV.save();
          graph.commit();
          graph.begin();
          assertEquals(1, childV.getVersion());

          parentV1.addEdge(childV, "edgetype").save();

          graph.commit();
          graph.begin();

          OLogManager.instance().error(this, "parentV1 %s v%d should be v%d", parentV1.getIdentity(),
              parentV1.getRecord().getVersion(), i + 2);

          assertEquals(i + 2,  parentV1.getVersion());
          assertEquals(2,  childV.getVersion());
        }

        pause();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Exception", e);
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.close();
        LOCK.notifyAll();
      }
    }
  }

  protected void dbClient2() {
    sleep(500);

    synchronized (LOCK) {
      ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getLocalURL2());
      if(graph.exists()){
        graph.open("admin", "admin");
      }else{
        graph.create();
      }

      try {
        sleep(500);
        OVertex parentV1 = graph.load((ORID) parentV1Id);
        assertEquals(NUM_OF_LOOP_ITERATIONS + 1, parentV1.getRecord().getVersion());
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.close();
        LOCK.notifyAll();
      }
    }
  }

}
