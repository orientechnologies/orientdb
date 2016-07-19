package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;
import org.junit.Test;
import org.testng.Assert;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test case to check the right management of distributed exception while a server is starting. Derived from the test provided by
 * Gino John for issue http://www.prjhub.com/#/issues/6449.
 *
 * 3 nodes, the test is started after the 1st node is up & running. The test is composed by multiple (8) parallel threads that
 * update the same records 20,000 times.
 * 
 * @author Luca Garulli
 */
public class HAGraphTest extends AbstractServerClusterTxTest {

  final static int            SERVERS                 = 3;
  private static final int    CONCURRENCY_LEVEL       = 8;
  private static final int    TOTAL_CYCLES_PER_THREAD = 10000;

  private OrientGraphFactory  graphReadFactory;
  private ExecutorService     executorService;
  private int                 serverStarted           = 0;
  private AtomicLong          operations              = new AtomicLong();

  private final AtomicBoolean serverDown              = new AtomicBoolean(false);
  private final AtomicBoolean serverRestarted         = new AtomicBoolean(false);

  List<Future<?>>             ths                     = new ArrayList<Future<?>>();
  private TimerTask           task;

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

      task = new TimerTask() {
        @Override
        public void run() {
          if (serverDown.get() && !serverRestarted.get()
              && operations.get() >= TOTAL_CYCLES_PER_THREAD * CONCURRENCY_LEVEL * 2 / 3) {

            // RESTART LAST SERVER AT 2/3 OF PROGRESS
            banner("RESTARTING SERVER " + (SERVERS - 1) + "...");
            try {
              serverInstance.get(SERVERS - 1).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
              if (serverInstance.get(SERVERS - 1).server.getPluginByClass(OHazelcastPlugin.class) != null)
                serverInstance.get(SERVERS - 1).server.getPluginByClass(OHazelcastPlugin.class).waitUntilNodeOnline();

              serverRestarted.set(true);

            } catch (Exception e) {
              e.printStackTrace();
            }

          } else if (!serverDown.get() && operations.get() >= TOTAL_CYCLES_PER_THREAD * CONCURRENCY_LEVEL / 3) {

            // SHUTDOWN LASt SERVER AT 1/3 OF PROGRESS
            banner("SIMULATE SOFT SHUTDOWN OF SERVER " + (SERVERS - 1));
            serverInstance.get(SERVERS - 1).shutdownServer();

            serverDown.set(true);
          }
        }
      };
      Orient.instance().scheduleTask(task, 2000, 200);
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "remote:localhost:2424;localhost:2425;localhost:2426/" + getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return "dbquerytest";
  }

  @Override
  public void executeTest() throws Exception {
    waitForEndOfTest();

    if (task != null)
      task.cancel();

    Assert.assertEquals(serverDown.get(), true);
    Assert.assertEquals(serverRestarted.get(), true);
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
          for (int i = 1; i < TOTAL_CYCLES_PER_THREAD && isRunning; i++) {
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
                        vtx1.setProperty("blob", new byte[1000]);

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
                        if (ex instanceof ODistributedRecordLockedException) {
                          if (k > 20)
                            log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] ODistributedRecordLockedException: [" + ex + "] Cause: ["
                                + (ex.getCause() != null ? ex.getCause() : "--") + "] for vertex " + vtx1);
                          vtx1.reload();
                        } else if (ex instanceof ONeedRetryException || ex.getCause() instanceof ONeedRetryException) {
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

            operations.incrementAndGet();
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
}
