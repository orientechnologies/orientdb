/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case to check the right management of distributed exception while a server is starting.
 * Derived from the test provided by Gino John for issue http://www.prjhub.com/#/issues/6449.
 *
 * <p>3 nodes, the test is started after the 1st node is up & running. The test is composed by
 * multiple (8) parallel threads that update the same records 20,000 times.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class HALocalGraphIT extends AbstractServerClusterTxTest {

  protected static final int SERVERS = 3;
  protected static final int CONCURRENCY_LEVEL = 4;
  protected static final int TOTAL_CYCLES_PER_THREAD = 300000;
  protected final AtomicBoolean serverDown = new AtomicBoolean(false);
  protected final AtomicBoolean serverRestarting = new AtomicBoolean(false);
  protected final AtomicBoolean serverRestarted = new AtomicBoolean(false);
  protected ODatabasePool graphReadFactory;
  protected ExecutorService executorService;
  protected int serverStarted = 0;
  protected AtomicLong operations = new AtomicLong();
  List<Future<?>> ths = new ArrayList<Future<?>>();
  private TimerTask task;
  private volatile long sleep = 0;

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
      createSchemaAndFirstVertices(server);
      startTest(server);
      // START THE TEST DURING 2ND NODE STARTUP
      task =
          Orient.instance()
              .scheduleTask(
                  () -> {
                    final OServer server2 = serverInstance.get(SERVERS - 1).getServerInstance();

                    if (server2 != null) {
                      if (serverDown.get()
                          && !serverRestarting.get()
                          && !serverRestarted.get()
                          && !server2.isActive()
                          && operations.get()
                              >= TOTAL_CYCLES_PER_THREAD * CONCURRENCY_LEVEL * 2 / 4) {
                        serverRestarting.set(true);

                        // RESTART LAST SERVER AT 2/3 OF PROGRESS
                        banner("RESTARTING SERVER " + (SERVERS - 1) + "...");
                        try {
                          serverInstance
                              .get(SERVERS - 1)
                              .startServer(
                                  getDistributedServerConfiguration(
                                      serverInstance.get(SERVERS - 1)));

                          if (serverInstance
                                  .get(SERVERS - 1)
                                  .getServerInstance()
                                  .getDistributedManager()
                              != null)
                            serverInstance
                                .get(SERVERS - 1)
                                .getServerInstance()
                                .getDistributedManager()
                                .waitUntilNodeOnline();

                          sleep = 0;

                          serverRestarted.set(true);

                        } catch (Exception e) {
                          e.printStackTrace();
                        }

                      } else if (!serverDown.get()
                          && server2.isActive()
                          && operations.get()
                              >= TOTAL_CYCLES_PER_THREAD * CONCURRENCY_LEVEL * 1 / 4) {

                        // SLOW DOWN A LITTLE BIT
                        sleep = 5;

                        // SHUTDOWN LASt SERVER AT 1/3 OF PROGRESS
                        banner("SIMULATE SOFT SHUTDOWN OF SERVER " + (SERVERS - 1));
                        serverInstance.get(SERVERS - 1).shutdownServer();

                        serverDown.set(true);
                      }
                    }
                  },
                  1000,
                  1000);
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "remote:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "HALocalGraphIT";
  }

  @Override
  public void executeTest() throws Exception {

    waitForEndOfTest();

    if (task != null) task.cancel();

    Assert.assertEquals(serverDown.get(), true);

    Assert.assertEquals(serverRestarting.get(), true);

    waitFor(
        20000,
        new OCallable<Boolean, Void>() {
          @Override
          public Boolean call(Void iArgument) {
            return serverRestarted.get();
          }
        },
        "Server 2 is not active yet");

    Assert.assertEquals(serverRestarted.get(), true);
  }

  private void startTest(final ServerRun server) {
    for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
      Future<?> future = getExecutorService().submit(startThread(i, getGraphFactory(server)));
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

  private Runnable startThread(final int id, final ODatabasePool graphFactory) {

    Runnable th =
        new Runnable() {
          @Override
          public void run() {
            System.out.println("\n\n ----------- startThread()");
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
                  log(
                      sb.toString()
                          + " ["
                          + id
                          + "] Total Records Processed: ["
                          + i
                          + "] Current: [2500] Time taken: ["
                          + (et - st) / 1000
                          + "] seconds");
                  st = System.currentTimeMillis();
                }

                if (sleep > 0) Thread.sleep(sleep);

                ODatabaseDocument graph = graphFactory.acquire();
                graph.begin();

                try {
                  if (useSQL) {
                    boolean update = true;
                    boolean isException = false;
                    String sql =
                        "Update Test set prop5='"
                            + System.currentTimeMillis()
                            + "', updateTime='"
                            + new Date().toString()
                            + "' where prop2='v2-1'";
                    for (int k = 0; k < 10 && update; k++) {
                      try {
                        graph.command(new OCommandSQL(sql)).execute();
                        if (isException) {
                          log(
                              "********** ["
                                  + id
                                  + "]["
                                  + k
                                  + "] Update success after distributed lock Exception");
                        }
                        update = false;
                        break;
                      } catch (ONeedRetryException ex) {
                        if (ex instanceof OConcurrentModificationException
                            || ex.getCause() instanceof OConcurrentModificationException) {
                        } else {
                          isException = true;
                          log(
                              "*$$$$$$$$$$$$$$ ["
                                  + id
                                  + "]["
                                  + k
                                  + "] Distributed Exception: ["
                                  + ex
                                  + "] Cause: ["
                                  + (ex.getCause() != null ? ex.getCause() : "--")
                                  + "] ");
                        }
                      } catch (ODistributedException ex) {
                        if (ex.getCause() instanceof OConcurrentModificationException) {
                        } else {
                          isException = true;
                          log(
                              "*$$$$$$$$$$$$$$ ["
                                  + id
                                  + "]["
                                  + k
                                  + "] Distributed Exception: ["
                                  + ex
                                  + "] Cause: ["
                                  + (ex.getCause() != null ? ex.getCause() : "--")
                                  + "] ");
                        }

                      } catch (Exception ex) {
                        log("[" + id + "][" + k + "] Exception " + ex);
                      }
                    }
                  } else {
                    boolean retry = true;

                    OResultSet vtxs = null;
                    for (int k = 0; k < 100 && retry; k++)
                      try {
                        vtxs = graph.query(query);
                        break;
                      } catch (ONeedRetryException e) {
                        // RETRY
                      }

                    while (vtxs.hasNext()) {
                      OResult vtx = vtxs.next();
                      if (retry) {
                        retry = true;
                        boolean isException = false;

                        for (int k = 0; k < 100 && retry; k++) {
                          OVertex vtx1 = vtx.getVertex().get();
                          try {
                            vtx1.setProperty("prop5", "prop55");
                            vtx1.setProperty("updateTime", new Date().toString());
                            vtx1.setProperty("blob", new byte[1000]);

                            graph.commit();
                            graph.begin();
                            if (isException) {
                              // log("********** [" + id + "][" + k + "] Update success after
                              // distributed lock Exception for vertex " +
                              // vtx1);
                            }
                            retry = false;
                            break;
                          } catch (OConcurrentModificationException ex) {
                            vtx1.reload();
                          } catch (ONeedRetryException ex) {
                            if (ex instanceof ODistributedRecordLockedException) {
                              if (k > 20)
                                log(
                                    "*$$$$$$$$$$$$$$ ["
                                        + id
                                        + "]["
                                        + k
                                        + "] ODistributedRecordLockedException: ["
                                        + ex
                                        + "] Cause: ["
                                        + (ex.getCause() != null ? ex.getCause() : "--")
                                        + "] for vertex "
                                        + vtx1);
                              vtx1.reload();
                            } else if (ex instanceof ONeedRetryException
                                || ex.getCause() instanceof ONeedRetryException) {
                              vtx1.reload();
                            } else {
                              if (ex.getCause() instanceof ConcurrentModificationException) {
                                ex.printStackTrace();
                              }
                              log(
                                  "*$$$$$$$$$$$$$$ ["
                                      + id
                                      + "]["
                                      + k
                                      + "] Distributed Exception: ["
                                      + ex
                                      + "] Cause: ["
                                      + (ex.getCause() != null ? ex.getCause() : "--")
                                      + "] for vertex "
                                      + vtx1);
                            }
                            // log("*** [" + id + "][" + k + "] Distributed Exception: [" + ex + "]
                            // Cause: [" + (ex.getCause() != null ?
                            // ex.getCause() : "--") + "] for vertex " + vtx1);

                            isException = true;
                          } catch (ODistributedException ex) {
                            if (ex.getCause() instanceof ONeedRetryException) {
                              vtx1.reload();
                            } else {
                              if (ex.getCause() instanceof ConcurrentModificationException) {
                                ex.printStackTrace();
                              }
                              log(
                                  "*$$$$$$$$$$$$$$ ["
                                      + id
                                      + "]["
                                      + k
                                      + "] Distributed Exception: ["
                                      + ex
                                      + "] Cause: ["
                                      + (ex.getCause() != null ? ex.getCause() : "--")
                                      + "] for vertex "
                                      + vtx1);
                            }
                            // log("*** [" + id + "][" + k + "] Distributed Exception: [" + ex + "]
                            // Cause: [" + (ex.getCause() != null ?
                            // ex.getCause() : "--") + "] for vertex " + vtx1);

                            isException = true;
                          } catch (Exception ex) {
                            log("[" + id + "][" + k + "] Exception " + ex + " for vertex " + vtx1);
                          }
                        }
                        if (retry) {
                          log(
                              "********** ["
                                  + id
                                  + "] Failed to update after Exception for vertex "
                                  + vtx);
                        }
                      }
                    }
                    vtxs.close();
                  }

                } finally {
                  graph.close();
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

  protected ODatabasePool getGraphFactory(final ServerRun server) {
    if (graphReadFactory == null) {
      //      String dbUrl = getDatabaseURL(serverInstance.get(0));
      //      log("Datastore pool created with size : 10, db location: " + getDatabaseURL(server));
      graphReadFactory =
          new ODatabasePool(
              server.getServerInstance().getContext(),
              getDatabaseName(),
              "admin",
              "admin",
              OrientDBConfig.defaultConfig());
    }
    return graphReadFactory;
  }

  private void createVertexType(ODatabaseDocument orientGraph, String className) {
    OClass clazz = orientGraph.getClass(className);
    if (clazz == null) {
      log("Creating vertex type - " + className);
      orientGraph.createVertexClass(className);
    }
  }

  private ExecutorService getExecutorService() {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(10);
    }
    return executorService;
  }

  private void createSchemaAndFirstVertices(final ServerRun server) {
    //    ODatabaseDocumentTx orientGraph = new
    // ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));

    final ODatabaseDocument orientGraph =
        server.getServerInstance().openDatabase(getDatabaseName(), "admin", "admin");

    /*
    if (orientGraph.exists()) {
      orientGraph.open("admin", "admin");
    } else {
      orientGraph.create();
    } */

    createVertexType(orientGraph, "Test");
    createVertexType(orientGraph, "Test1");
    orientGraph.close();

    ODatabaseDocument graph = getGraphFactory(server).acquire();

    for (int i = 1; i <= 1; i++) {
      OVertex vertex = graph.newVertex("Test");
      vertex.setProperty("prop1", "v1-" + i);
      vertex.setProperty("prop2", "v2-1");
      vertex.setProperty("prop3", "v3-1");
      vertex.save();
      graph.commit();
      graph.begin();
      if ((i % 100) == 0) {
        log("Created " + i + " nodes");
      }
    }
    for (int i = 1; i <= 200; i++) {
      OVertex vertex = graph.newVertex("Test1");
      vertex.setProperty("prop1", "v1-" + i);
      vertex.setProperty("prop2", "v2-1");
      vertex.setProperty("prop3", "v3-1");
      vertex.save();
      graph.commit();
      graph.begin();
      if ((i % 10) == 0) {
        System.out.print("." + i + ".");
      }
      if ((i % 100) == 0) {
        System.out.println();
      }
    }
    graph.close();
  }
}
