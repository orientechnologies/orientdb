package com.orientechnologies.orient.server.distributed.scenariotest;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentDistributedUpdateIT extends AbstractScenarioTest {

  @Test
  public void test() throws Exception {
    count = 1;
    writerCount = 1;
    delayWriter = 500;

    className = "Test";
    indexName = null;
    OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.setValue(0);

    init(2);

    prepare(false);

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    OrientDB orientDB = serverInstance.get(0).getServerInstance().getContext();

    if (!orientDB.exists(getDatabaseName())) {
      orientDB.execute(
          "create database ? plocal users(admin identified by 'admin' role admin)",
          getDatabaseName());
    }
    ODatabaseDocument orientGraph = orientDB.open(getDatabaseName(), "admin", "admin");
    OClass clazz = orientGraph.getClass("Test");
    if (clazz == null) {
      log("Creating vertex type - " + "Test");
      orientGraph.createClass("Test", "V");
    }

    orientGraph.close();

    if (!orientDB.exists(getDatabaseName())) {
      orientDB.execute(
          "create database ? plocal users(admin identified by 'admin' role admin)",
          getDatabaseName());
    }
    ODatabaseDocument graph = orientDB.open(getDatabaseName(), "admin", "admin");
    for (int i = 0; i < 2; i++) {
      OVertex vertex = graph.newVertex("Test");
      vertex.setProperty("prop1", "v1-" + i);
      vertex.setProperty("prop2", "v2-1");
      vertex.setProperty("prop3", "v3-1");
      vertex.save();
      if ((i % 100) == 0) {
        log("Created " + i + " nodes");
      }
    }
    graph.close();

    executeMultipleTest();
  }

  protected Callable<Void> createWriter(
      final int serverId, final int threadId, final ServerRun server) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String id = serverId + "." + threadId;

        boolean isRunning = true;

        if (!server.getServerInstance().existsDatabase(getDatabaseName())) {
          server
              .getServerInstance()
              .getContext()
              .execute(
                  "create database ? plocal users(admin identified by 'admin' role admin)",
                  getDatabaseName());
        }
        ODatabaseDocument graph =
            server.getServerInstance().openDatabase(getDatabaseName(), "admin", "admin");
        graph.begin();

        try {
          String query = "select from Test where prop2='v2-1'";
          for (int i = 0; i < 100 && isRunning; i++) {
            if ((i % 25) == 0) {
              log("[" + id + "] Records Processed: [" + i + "]");
            }
            Iterable<OElement> vtxs = graph.command(new OCommandSQL(query)).execute();
            boolean update = true;
            for (OElement vtx : vtxs) {
              if (update) {
                update = true;
                for (int k = 0; k < 10 && update; k++) {
                  OElement vtx1 = vtx;
                  try {
                    vtx1.setProperty("prop5", "prop55");
                    vtx1.save();
                    graph.commit();
                    graph.begin();
                    // log("[" + id + "/" + i + "/" + k + "] OK!\n");
                    break;
                  } catch (OConcurrentModificationException ex) {
                    vtx1.reload();
                  } catch (ODistributedRecordLockedException ex) {
                    log(
                        "["
                            + id
                            + "/"
                            + i
                            + "/"
                            + k
                            + "] Distributed lock Exception "
                            + ex
                            + " for vertex "
                            + vtx1
                            + " \n");
                    //                    ex.printStackTrace();
                    update = false;
                    //                    isRunning = false;
                    break;
                  } catch (Exception ex) {
                    log(
                        "["
                            + id
                            + "/"
                            + i
                            + "/"
                            + k
                            + "] Exception "
                            + ex
                            + " for vertex "
                            + vtx1
                            + "\n\n");
                    ex.printStackTrace();
                    isRunning = false;
                    break;
                  }
                }

                if (!isRunning) break;
              }
            }
          }
        } catch (Exception ex) {
          System.out.println("ID: [" + id + "]********** Exception " + ex + " \n\n");
          ex.printStackTrace();
        } finally {
          log("[" + id + "] Done................>>>>>>>>>>>>>>>>>>");
          graph.close();
          runningWriters.countDown();
        }

        Assert.assertTrue(isRunning);

        return null;
      }
    };
  }
}
