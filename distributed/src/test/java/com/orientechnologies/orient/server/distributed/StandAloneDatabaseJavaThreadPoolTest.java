package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class StandAloneDatabaseJavaThreadPoolTest {

  private String          dbName;
  private ODatabasePool   graphReadFactory;
  private ExecutorService executorService;

  public StandAloneDatabaseJavaThreadPoolTest(String dbName) {
    this.dbName = dbName;
    checkAndCreateDatabase(dbName);
  }

  /**
   * @return
   */
  private ExecutorService getExecutorService() {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(10);
    }
    return executorService;
  }

  public void runTest() {
    ODatabaseDocumentTx orientGraph = new ODatabaseDocumentTx(getDBURL());
    if (orientGraph.exists()) {
      orientGraph.open("admin", "admin");
    } else {
      orientGraph.create();
    }
    createVertexType(orientGraph, "Test");
    createVertexType(orientGraph, "Test1");
    orientGraph.close();

    ODatabaseDocument graph = getGraphFactory().acquire();
    graph.begin();
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
    // startPoolInfoThread();
    List<Future<?>> ths = new ArrayList<Future<?>>();
    for (int i = 0; i < 10; i++) {
      Future<?> future = getExecutorService().submit(startThread(i, getGraphFactory()));
      ths.add(future);
    }

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
          for (int i = 1; i < 10000000 && isRunning; i++) {
            if ((i % 2500) == 0) {
              long et = System.currentTimeMillis();
              log(sb.toString() + " [" + id + "] Total Records Processed: [" + i + "] Current: [2500] Time taken: ["
                  + (et - st) / 1000 + "] seconds");
              st = System.currentTimeMillis();
            }
            ODatabaseDocument graph = graphFactory.acquire();
            graph.begin();
            try {
              if (useSQL) {
                boolean update = true;
                boolean isException = false;
                String sql = "Update Test set prop5='" + String.valueOf(System.currentTimeMillis()) + "', updateTime='" + new Date()
                    .toString() + "' where prop2='v2-1'";
                for (int k = 0; k < 10 && update; k++) {
                  try {
                    graph.command(new OCommandSQL(sql)).execute();
                    if (isException) {
                      log("********** [" + id + "][" + k + "] Update success after distributed lock Exception");
                    }
                    update = false;
                    break;
                  } catch (ONeedRetryException ex) {
                    if (ex instanceof OConcurrentModificationException || ex
                        .getCause() instanceof OConcurrentModificationException) {
                    } else {
                      isException = true;
                      log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (
                          ex.getCause() != null ? ex.getCause() : "--") + "] ");
                    }

                  } catch (ODistributedException ex) {
                    if (ex.getCause() instanceof OConcurrentModificationException) {
                    } else {
                      isException = true;
                      log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (
                          ex.getCause() != null ? ex.getCause() : "--") + "] ");
                    }

                  } catch (Exception ex) {
                    log("[" + id + "][" + k + "] Exception " + ex);
                  }

                }
              } else {
                boolean retry = true;

                Iterable<OElement> vtxs = null;
                for (int k = 0; k < 100 && retry; k++)
                  try {
                    vtxs = graph.command(new OCommandSQL(query)).execute();
                    break;
                  } catch (ONeedRetryException e) {
                    // RETRY
                  }

                for (OElement vtx : vtxs) {
                  if (retry) {
                    retry = true;
                    boolean isException = false;

                    for (int k = 0; k < 100 && retry; k++) {
                      OElement vtx1 = vtx;
                      try {
                        vtx1.setProperty("prop5", "prop55");
                        vtx1.setProperty("updateTime", new Date().toString());
                        vtx1.save();
                        graph.commit();
                        graph.begin();
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
                          log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (
                              ex.getCause() != null ? ex.getCause() : "--") + "] for vertex " + vtx1);
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
                          log("*$$$$$$$$$$$$$$ [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (
                              ex.getCause() != null ? ex.getCause() : "--") + "] for vertex " + vtx1);
                        }
                        // log("*** [" + id + "][" + k + "] Distributed Exception: [" + ex + "] Cause: [" + (ex.getCause() != null ?
                        // ex.getCause() : "--") + "] for vertex " + vtx1);

                        isException = true;
                      } catch (Exception ex) {
                        log("[" + id + "][" + k + "] Exception " + ex + " for vertex " + vtx1);
                      }
                    }
                    if (retry) {
                      log("*******#################******* [" + id + "] Failed to update after Exception for vertex " + vtx);
                    }
                  }
                }
              }
            } finally {
              graph.close();
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

  private void startPoolInfoThread() {
    Thread th = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < 10000; i++) {
//          log("[" + i + "] Available insances pool " + getGraphFactory().getAvailableInstancesInPool() + " Created instances: "
//              + getGraphFactory().getCreatedInstancesInPool());//TODO
          try {
            Thread.sleep(20000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

        }
      }
    };
    th.start();
  }

  public void printVertex(String info, OElement vtx) {
    System.out.println("--------" + info + " ----------");
    System.out.println(vtx);
    Set<String> keys = vtx.getPropertyNames();
    for (String key : keys) {
      System.out.println("Key = " + key + " Value = " + vtx.getProperty(key));
    }
  }

  /**
   * @return
   */
  public String getDBURL() {
    return "remote:" + "localhost:2424;localhost:2425;localhost:2426" + "/" + dbName;
  }

  private ODatabasePool getGraphFactory() {
    if (graphReadFactory == null) {
      log("Datastore pool created with size : 10, db location: " + getDBURL());
      graphReadFactory = OrientDB.fromUrl("remote:" + "localhost:2424;localhost:2425;localhost:2426", OrientDBConfig.defaultConfig()).openPool(dbName,"admin", "admin");

//      graphReadFactory.setupPool(10, 10);//TODO
    }
    return graphReadFactory;
  }

  /**
   *
   */
  public void checkAndCreateDatabase(String dbName) {
    try {
      OServerAdmin serverAdmin = new OServerAdmin(getDBURL()).connect("root", "root");
      if (!serverAdmin.existsDatabase("plocal")) {
        log("Database does not exists. New database is created");
        serverAdmin.createDatabase(dbName, "graph", "plocal");
      } else {
        log(dbName + " database already exists");
      }
      serverAdmin.close();
    } catch (Exception ex) {
      log("Failed to create database", ex);
    }
  }

  private void createVertexType(ODatabaseDocument orientGraph, String className) {
    OClass clazz = orientGraph.getClass(className);
    if (clazz == null) {
      log("Creating vertex type - " + className);
      orientGraph.createVertexClass(className);
    }
  }

  private void log(String message) {
    System.out.println(message);
  }

  private void log(String message, Throwable th) {
    System.out.println(th.getMessage());
    th.printStackTrace();
  }

  public static void main(String args[]) {
    StandAloneDatabaseJavaThreadPoolTest test = new StandAloneDatabaseJavaThreadPoolTest("dbquerytest1");
    test.runTest();
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Runtime.getRuntime().halt(0);
  }

}
