package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test case to check the right management of distributed exception while a server is starting. Derived from the test provided by
 * Gino John for issue http://www.prjhub.com/#/issues/6449.
 *
 * 3 nodes, the test is started after the 1st node is up & running. The test is composed by multiple (8) parallel threads that
 * update the same records 20,000 times.
 * 
 * @author Luca Garulli
 */
public class StandAloneDatabaseJavaThreadPoolTest extends AbstractServerClusterTxTest {

  final static int           SERVERS           = 3;
  private static final int   CONCURRENCY_LEVEL = 8;
  private static final int   TOTAL_CYCLES      = 20000;

  private OrientGraphFactory graphReadFactory;
  private ExecutorService    executorService;
  private int                serverStarted     = 0;
  List<Future<?>>            ths               = new ArrayList<Future<?>>();

  @Test
  public void test() throws Exception {
    useTransactions = false;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    if (serverStarted++ == 0) {
      // START THE TEST DURING 2ND NODE STARTUP
      createSchemaAndFirstVertices();
      startTest();
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "remote:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "dbquerytest";
  }

  private void createSchemaAndFirstVertices() {
    OrientBaseGraph orientGraph = new OrientGraphNoTx(getDatabaseURL(serverInstance.get(0)));
    createVertexType(orientGraph, "Test");
    createVertexType(orientGraph, "Test1");
    orientGraph.shutdown();

    OrientBaseGraph graph = getGraphFactory().getTx();

    for (int i = 1; i <= 1; i++) {
      Vertex vertex = graph.addVertex("class:Test");
      vertex.setProperty("prop1", "v1-" + i);
      vertex.setProperty("prop2", "v2-1");
      vertex.setProperty("prop3", "v3-1");
      graph.commit();
      if ((i % 100) == 0) {
        log("Created " + i + " nodes");
      }
    }
    for (int i = 1; i <= 200; i++) {
      Vertex vertex = graph.addVertex("class:Test1");
      vertex.setProperty("prop1", "v1-" + i);
      vertex.setProperty("prop2", "v2-1");
      vertex.setProperty("prop3", "v3-1");
      graph.commit();
      if ((i % 10) == 0) {
        System.out.print("." + i + ".");
      }
      if ((i % 100) == 0) {
        System.out.println();
      }
    }
    graph.shutdown();
  }

  @Override
  public void executeTest() throws Exception {
    waitForEndOfTest();
  }

  private void startTest() {
    for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
      Future<?> future = getExecutorService().submit(startThread(i, getGraphFactory()));
      ths.add(future);
    }
  }

  private void waitForEndOfTest() {
    for (Future<?> th : ths) {
      try {
        th.get();
      } catch (Exception ex) {
        System.out.println("********** Future Exception " + ex);
        ex.printStackTrace();
      }
    }
  }

  private Runnable startThread(final int id, final OrientGraphFactory graphFactory) {

    Runnable th = new Runnable() {
      @Override
      public void run() {
        // OrientBaseGraph graph = new OrientGraph(getDBURL());
        // OrientGraph graph = graphFactory.getTx();
        boolean useSQL = false;
        StringBuilder sb = new StringBuilder(".");
        for (int m = 0; m < id; m++) {
          sb.append(".");
        }
        long st = System.currentTimeMillis();
        try {
          String query = "select from Test where prop2='v2-1'";
          boolean isRunning = true;
          for (int i = 1; i < TOTAL_CYCLES && isRunning; i++) {
            if ((i % 2500) == 0) {
              long et = System.currentTimeMillis();
              log(sb.toString() + " [" + id + "] Total Records Processed: [" + i + "] Current: [2500] Time taken: ["
                  + (et - st) / 1000 + "] seconds");
              st = System.currentTimeMillis();
            }
            OrientGraph graph = graphFactory.getTx();
            try {
              if (useSQL) {
                boolean update = true;
                boolean isException = false;
                String sql = "Update Test set prop5='" + String.valueOf(System.currentTimeMillis()) + "', updateTime='"
                    + new Date().toString() + "' where prop2='v2-1'";
                for (int k = 0; k < 10 && update; k++) {
                  try {
                    graph.command(new OCommandSQL(sql)).execute();
                    if (isException) {
                      log("********** [" + id + "][" + k + "] Update success after distributed lock Exception");
                    }
                    update = false;
                    break;
                  } catch (ONeedRetryException ex) {
                    if (ex instanceof OConcurrentModificationException
                        || ex.getCause() instanceof OConcurrentModificationException) {
                    } else {
                      isException = true;
                      log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: ["
                          + (ex.getCause() != null ? ex.getCause() : "--") + "] ");
                    }
                  } catch (ODistributedException ex) {
                    if (ex.getCause() instanceof OConcurrentModificationException) {
                    } else {
                      isException = true;
                      log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: ["
                          + (ex.getCause() != null ? ex.getCause() : "--") + "] ");
                    }

                  } catch (Exception ex) {
                    log("[" + id + "][" + k + "] Exception " + ex);
                  }
                }
              } else {
                Iterable<Vertex> vtxs = graph.command(new OCommandSQL(query)).execute();
                boolean retry = true;
                for (Vertex vtx : vtxs) {
                  if (retry) {
                    retry = true;
                    boolean isException = false;

                    for (int k = 0; k < 100 && retry; k++) {
                      OrientVertex vtx1 = (OrientVertex) vtx;
                      try {
                        vtx1.setProperty("prop5", "prop55");
                        vtx1.setProperty("updateTime", new Date().toString());
                        graph.commit();
                        if (isException) {
                          // log("********** [" + id + "][" + k + "] Update success after distributed lock Exception for vertex " +
                          // vtx1);
                        }
                        retry = false;
                        break;
                      } catch (OConcurrentModificationException ex) {
                        vtx1.reload();
                      } catch (ONeedRetryException ex) {
                        if (ex instanceof ONeedRetryException || ex.getCause() instanceof ONeedRetryException) {
                          vtx1.reload();
                        } else {
                          if (ex.getCause() instanceof ConcurrentModificationException) {
                            ex.printStackTrace();
                          }
                          log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: ["
                              + (ex.getCause() != null ? ex.getCause() : "--") + "] for vertex " + vtx1);
                        }
                        // log("*** [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (ex.getCause() != null ?
                        // ex.getCause() : "--") + "] for vertex " + vtx1);

                        isException = true;
                      } catch (ODistributedException ex) {
                        if (ex.getCause() instanceof ONeedRetryException) {
                          vtx1.reload();
                        } else {
                          if (ex.getCause() instanceof ConcurrentModificationException) {
                            ex.printStackTrace();
                          }
                          log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: ["
                              + (ex.getCause() != null ? ex.getCause() : "--") + "] for vertex " + vtx1);
                        }
                        // log("*** [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (ex.getCause() != null ?
                        // ex.getCause() : "--") + "] for vertex " + vtx1);

                        isException = true;
                      } catch (Exception ex) {
                        log("[" + id + "][" + k + "] Exception " + ex + " for vertex " + vtx1);
                      }
                    }
                    if (retry) {
                      log("********** [" + id + "] Failed to update after Exception for vertex " + vtx);
                    }
                  }
                }
              }
            } finally {
              graph.shutdown();
            }
          }
        } catch (Exception ex) {
          System.out.println("ID: [" + id + "]********** Exception " + ex + " \n\n");
          ex.printStackTrace();
        } finally {
          log("[" + id + "] Done................>>>>>>>>>>>>>>>>>>");
        }
      }
    };
    return th;
  }

  private OrientGraphFactory getGraphFactory() {
    if (graphReadFactory == null) {
      log("Datastore pool created with size : 10, db location: " + getDatabaseURL(serverInstance.get(0)));
      graphReadFactory = new OrientGraphFactory(getDatabaseURL(serverInstance.get(0)));
      graphReadFactory.setupPool(10, 10);
    }
    return graphReadFactory;
  }

  private void createVertexType(OrientBaseGraph orientGraph, String className) {
    OClass clazz = orientGraph.getVertexType(className);
    if (clazz == null) {
      log("Creating vertex type - " + className);
      orientGraph.createVertexType(className);
    }
  }

  private ExecutorService getExecutorService() {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(10);
    }
    return executorService;
  }
}
