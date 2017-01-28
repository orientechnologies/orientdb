package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;

public class TestAsyncReplMode2Servers extends BareBoneBase2ServerTest {

  private static final int    NUM_OF_LOOP_ITERATIONS = 3;
  private static final int    NUM_OF_RETRIES         = 3;
  private static final String CNT_PROP_NAME          = "cnt";

  private Object              parentV1Id;
  private Object              parentV2Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2Servers";
  }

  protected void dbClient1() {
    OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    synchronized (LOCK) {
      ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getRemoteURL());
      if(graph.exists()){
        graph.open("admin", "admin");
      }else{
        graph.create();
        graph.createClass("vertextype", "V");
      }
      graph.begin();
      try {
        OVertex parentV1 = graph.newVertex("vertextype");
        parentV1.save();
        graph.commit();
        graph.begin();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getIdentity();

        OVertex parentV2 = graph.newVertex("vertextype");
        parentV2.save();
        graph.commit();
        graph.begin();
        assertEquals(1, parentV2.getRecord().getVersion());
        parentV2Id = parentV2.getIdentity();

        int countPropValue = 0;
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV1.setProperty(CNT_PROP_NAME, ++countPropValue);
              parentV1.save();
              graph.commit();
              graph.begin();
              System.out.println("Committing parentV1" + parentV1.getRecord()+"...");
              break;
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              graph.begin();
              parentV1.reload();
            }
          }

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV2.setProperty(CNT_PROP_NAME, countPropValue);
              parentV2.save();
              graph.commit();
              graph.begin();
              System.out.println("Committing parentV2" + parentV2.getRecord() + "...");
              break;
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              graph.begin();
              parentV2.reload();
            }
          }
        }
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

  protected void dbClient2() {
    sleep(1000);

    synchronized (LOCK) {
      ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getRemoteURL2());
      if(graph.exists()){
        graph.open("admin", "admin");
      }else{
        graph.create();
      }
      graph.begin();

      try {
        OVertex parentV1 = graph.load((ORID)parentV1Id);
        assertEquals(1, parentV1.getRecord().getVersion());

        OVertex parentV2 = graph.load((ORID)parentV2Id);
        assertEquals(1, parentV2.getRecord().getVersion());

        int countPropValue = 0;
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;
          sleep(500);

          parentV1.reload();
          parentV2.reload();
          assertEquals("parentV1 (" + parentV1.getRecord() + ")", ++countPropValue, parentV1.<Object>getProperty(CNT_PROP_NAME));
          assertEquals("parentV2 (" + parentV2.getRecord() + ")", countPropValue, parentV2.<Object>getProperty(CNT_PROP_NAME));
        }
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
