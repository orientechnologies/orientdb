package com.orientechnologies.orient.server.distributed.scenariotest;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

public class ConcurrentDistributedUpdateTest extends AbstractScenarioTest {

  @Test
  public void test() throws Exception {
    count = 1;
    writerCount = 1;
    delayWriter = 500;

    className = "Test";
    indexName = null;

    init(2);

    prepare(false);

    execute();
  }

  @Override
  public void executeTest() throws Exception {
    OrientBaseGraph orientGraph = new OrientGraphNoTx(getPlocalDatabaseURL(serverInstance.get(0)));

    OClass clazz = orientGraph.getVertexType("Test");
    if (clazz == null) {
      log("Creating vertex type - " + "Test");
      orientGraph.createVertexType("Test");
    }

    orientGraph.shutdown();

    OrientBaseGraph graph = new OrientGraphNoTx(getPlocalDatabaseURL(serverInstance.get(0)));

    for (int i = 0; i < 2; i++) {
      Vertex vertex = graph.addVertex("class:Test");
      vertex.setProperty("prop1", "v1-" + i);
      vertex.setProperty("prop2", "v2-1");
      vertex.setProperty("prop3", "v3-1");
      graph.commit();
      if ((i % 100) == 0) {
        log("Created " + i + " nodes");
      }
    }
    graph.shutdown();

    executeMultipleTest();
  }

  protected Callable<Void> createWriter(final int serverId, final int threadId, final String databaseURL) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String id = serverId + "." + threadId;

        boolean isRunning = true;

        final OrientBaseGraph graph = new OrientGraph(databaseURL);
        try {
          String query = "select from Test where prop2='v2-1'";
          for (int i = 0; i < 100 && isRunning; i++) {
            if ((i % 25) == 0) {
              log("[" + id + "] Records Processed: [" + i + "]");
            }
            Iterable<Vertex> vtxs = graph.command(new OCommandSQL(query)).execute();
            boolean update = true;
            for (Vertex vtx : vtxs) {
              if (update) {
                update = true;
                for (int k = 0; k < 10 && update; k++) {
                  OrientVertex vtx1 = (OrientVertex) vtx;
                  try {
                    vtx1.setProperty("prop5", "prop55");
                    graph.commit();
                    // log("[" + id + "/" + i + "/" + k + "] OK!\n");
                    break;
                  } catch (OConcurrentModificationException ex) {
                    vtx1.reload();
                  } catch (ODistributedRecordLockedException ex) {
                    log("[" + id + "/" + i + "/" + k + "] Distributed lock Exception " + ex + " for vertex " + vtx1 + " \n");
//                    ex.printStackTrace();
                    update = false;
                    //                    isRunning = false;
                    break;
                  } catch (Exception ex) {
                    log("[" + id + "/" + i + "/" + k + "] Exception " + ex + " for vertex " + vtx1 + "\n\n");
                    ex.printStackTrace();
                    isRunning = false;
                    break;
                  }
                }

                if (!isRunning)
                  break;
              }
            }
          }
        } catch (Exception ex) {
          System.out.println("ID: [" + id + "]********** Exception " + ex + " \n\n");
          ex.printStackTrace();
        } finally {
          log("[" + id + "] Done................>>>>>>>>>>>>>>>>>>");
          graph.shutdown();
          runningWriters.countDown();
        }

        Assert.assertTrue(isRunning);

        return null;
      }
    };
  }
}
