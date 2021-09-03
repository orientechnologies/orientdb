package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public final class DistributedDatabaseCRUDIT {

  /**
   * ***************************************************************************************************
   * 1. Create 51 Edge and Vertex. Edge: Test, Test1-Test50, Vertex: TestNode, Test1Node -
   * Test50Node 2. Populate TestNode with 50 records with unique index for property1 and property2
   * 3. Populate Test1Node - Test31Node with 500 records. Each record will have edge to TestNode 4.
   * Start a pool of threads to update one record in TestNode using both Java API and SQL
   * ****************************************************************************************************
   */
  private String dbName;

  private ODatabasePool graphReadFactory;
  private OrientDB orientDB;
  private int totalRecords;

  public DistributedDatabaseCRUDIT(String dbName, int totalRecords) {
    this.dbName = dbName;
    this.totalRecords = totalRecords;
  }

  public void createDBData() {
    log("Creating data for database " + dbName);
    checkAndCreateDatabase(dbName);
    int totalClassCount = 50;
    int mainNodeDataCount = 50;
    OrientDB orientDb = getOrientDB();
    if (!orientDB.exists(dbName)) {
      orientDb.execute(
          "create database ? plocal users(admin identified by 'admin' role admin)", dbName);
    }
    ODatabaseSession orientGraph = orientDb.open(dbName, "admin", "admin");
    createVertexTypeWithUniqueIndex(orientGraph, "Test", "property1", "property2");
    for (int i = 1; i <= totalClassCount; i++) {
      createVertexType(orientGraph, "Test" + i, "property1", "property2");
    }
    orientGraph.close();

    ODatabaseDocument graph = getGraphFactory().acquire();
    graph.begin();

    boolean addTestData = true;
    String query = "select count(*) as dataCount from TestNode";
    Iterable<OVertex> vtxs = graph.command(new OCommandSQL(query)).execute();
    for (OVertex vtx : vtxs) {
      long count = vtx.getProperty("dataCount");
      addTestData = (count == 0);
    }

    if (addTestData) {
      for (int i = 1; i <= mainNodeDataCount; i++) {
        OVertex vertex = graph.newVertex("TestNode");
        vertex.setProperty("property1", "value1-" + i);
        vertex.setProperty("property2", "value2-" + i);
        vertex.setProperty("property3", "value3-" + i);
        vertex.setProperty("property4", "value4-" + i);
        vertex.setProperty("prop-6", "value6-" + i);
        vertex.setProperty("prop-7", "v7-1");
        vertex.setProperty("prop-8", "v7-1");
        vertex.setProperty("prop-9", "v7-1");
        vertex.setProperty("prop-10", "v7-1");
        vertex.setProperty("prop11", "v7-1");
        vertex.setProperty("prop12", "v7-1");
        vertex.setProperty("prop13", "v7-1");
        vertex.setProperty("prop14", "v7-1");
        vertex.setProperty("prop15", System.currentTimeMillis());
        vertex.save();
        graph.commit();
        graph.begin();
        if ((i % 100) == 0) {
          log("Created " + i + " nodes");
        }
      }
    }
    int edgeCounter = 1;
    for (int j = 1; j <= totalClassCount; j++) {
      String edgeName = "Test" + j;
      String className = edgeName + "Node";
      System.out.print("[" + className + "] -> ");
      for (int i = 1; i <= totalRecords; i++) {
        OVertex vertex = graph.newVertex(className);
        vertex.setProperty("property1", "value1-" + i);
        vertex.setProperty("property2", "value2-" + i);
        vertex.setProperty("property3", "value3-" + i);
        vertex.setProperty("property4", "value4-1");
        vertex.setProperty("prop-6", "value6-" + i);
        vertex.setProperty("prop-7", "value7-" + i);
        vertex.setProperty("prop-8", "value7-1");
        vertex.setProperty("prop-9", "value7-1");
        vertex.setProperty("prop-10", "value7-1");
        vertex.setProperty("prop11", "value7-1");
        vertex.setProperty("prop12", "value7-1");
        vertex.setProperty("prop13", "value7-1");
        vertex.setProperty("prop14", System.currentTimeMillis());
        vertex.setProperty("prop15", System.currentTimeMillis());
        vertex.save();
        graph.commit();
        graph.begin();
        if ((i % 200) == 0) {
          System.out.print(".[" + j + "]" + i + ".");
        }
        String edgeSQL =
            "Create EDGE "
                + edgeName
                + " FROM (SELECT FROM TestNode WHERE property1='value1-"
                + edgeCounter
                + "') "
                + "TO (SELECT FROM "
                + className
                + " WHERE property1='value1-"
                + i
                + "') ";
        edgeCounter++;
        if (edgeCounter > mainNodeDataCount) {
          edgeCounter = 1;
        }
        graph.command(new OCommandSQL(edgeSQL)).execute();
      }
      System.out.println();
    }
    graph.close();
    log("Done. Creating data for database " + dbName);
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start Different vertex update with Java API
  // ------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  private void runMultipleVertexUpdateTest() {
    log("Running Java API based multiple vertex update");
    List<Future<?>> ths = new ArrayList<Future<?>>();
    ExecutorService executorService = newFixedThreadPool("testnode-java-api-multi-update", 30);
    for (int i = 1; i <= 30; i++) {
      Future<?> future =
          executorService.submit(startVertexUpdateThread(i, getGraphFactory(), "value4-" + i));
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start Different vertex update with Java API
  // ------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start Different vertex update with Java API
  // ------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  private void runMultipleVertexSQLUpdateTest() {
    log("Running Java API based multiple vertex update using SQL");
    List<Future<?>> ths = new ArrayList<Future<?>>();
    ExecutorService executorService = newFixedThreadPool("testnode-sql-multi-update", 20);
    for (int i = 1; i <= 20; i++) {
      Future<?> future =
          executorService.submit(startSQLUpdateThread(i, getGraphFactory(), "value4-" + i));
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start Different vertex update with Java API
  // ------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start update based on SQL
  // -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  public void runSQLUpdateTest() {
    log("Running SQL based vertex update");
    List<Future<?>> ths = new ArrayList<Future<?>>();
    ExecutorService executorService = newFixedThreadPool("testnode-sql-update", 10);
    for (int i = 0; i < 10; i++) {
      Future<?> future =
          executorService.submit(startSQLUpdateThread(i, getGraphFactory(), "value4-1"));
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

  private Runnable startSQLUpdateThread(
      final int id, final ODatabasePool graphFactory, final String propertyValue) {
    Runnable th =
        new Runnable() {
          @Override
          public void run() {
            log("Starting runnable for sql update thread for property " + propertyValue);
            long st = System.currentTimeMillis();
            try {
              boolean isRunning = true;
              for (int i = 1; i < 10000000 && isRunning; i++) {
                if ((i % 100) == 0) {
                  long et = System.currentTimeMillis();
                  log(
                      " ["
                          + id
                          + "] Total Records Processed: ["
                          + i
                          + "] Time taken for [100] records: ["
                          + (et - st) / 1000
                          + "] seconds");
                  st = System.currentTimeMillis();
                }
                ODatabaseDocument graph = graphFactory.acquire();

                try {
                  boolean update = true;
                  boolean isException = false;
                  Exception tex = null;
                  String sql =
                      "Update TestNode set prop5='"
                          + String.valueOf(System.currentTimeMillis())
                          + "'"
                          + ", prop-7='value7-1', prop-8='value8-1', prop-9='value9-1',prop-10='value10-1', prop11='value11-1'"
                          + ", prop-07='value07-1', prop-08='value08-1', prop-09='value09-1',prop-010='value010-1', prop011='value011-1'"
                          + ", prop12='vaue12-1', prop13='value13-1'"
                          + ", updateTime='"
                          + new Date().toString()
                          + "' where property4='"
                          + propertyValue
                          + "'";
                  int k = 1;
                  for (; k <= 100 && update; k++) {
                    try {
                      graph.command(new OCommandSQL(sql)).execute();
                      if (isException) {
                        // log("********** [" + id + "][" + k + "] Update success after distributed
                        // lock Exception");
                      }
                      update = false;
                      break;
                    } catch (Exception ex) {
                      if (ex instanceof ODatabaseException
                          || ex instanceof ONeedRetryException
                          || ex instanceof ODistributedException) {
                        tex = ex;
                        if (ex instanceof ONeedRetryException
                            || ex.getCause() instanceof ONeedRetryException) {
                          // update is true. retry
                          // log("[" + id + "][" + propertyValue + "][ Retry: " + k + "]
                          // OrientDBInternal Exception [" + ex + "]");
                          try {
                            Thread.sleep(new Random().nextInt(500));
                          } catch (InterruptedException e) {
                            e.printStackTrace();
                          }
                        } else {
                          log(
                              "["
                                  + id
                                  + "][ Retry: "
                                  + k
                                  + "] Failed to update. OrientDB Exception ["
                                  + ex
                                  + "]");
                        }
                        isException = true;
                      } else {
                        tex = ex;
                        log("[" + id + "][" + k + "] Failed non OrientDB Exception [" + ex + "]");
                      }

                      if (update) {
                        log(
                            "*******#################******* ["
                                + id
                                + "][ Retry: "
                                + k
                                + "] Failed to update after Exception ["
                                + ((tex != null) ? tex : "----")
                                + "] for vertex with property4='"
                                + propertyValue
                                + "'");
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- End update based on SQL
  // -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start update based on Java API
  // -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  private void runVertexUpdateTest() {
    log("Running Java API based vertex update");
    List<Future<?>> ths = new ArrayList<Future<?>>();
    ExecutorService executorService = newFixedThreadPool("testnode-java-update", 10);
    for (int i = 0; i < 10; i++) {
      Future<?> future =
          executorService.submit(startVertexUpdateThread(i, getGraphFactory(), "value4-1"));
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

  private Runnable startVertexUpdateThread(
      final int id, final ODatabasePool graphFactory, final String propertyValue) {
    Runnable th =
        new Runnable() {
          @Override
          public void run() {
            log("Starting runnable for vertex update thread for property value " + propertyValue);
            long st = System.currentTimeMillis();
            try {
              // String query = "select from TestNode where property4='value4-1'";
              String query = "select from TestNode where property4='" + propertyValue + "'";
              boolean isRunning = true;
              for (int i = 1; i < 100000000 && isRunning; i++) {
                if ((i % 100) == 0) {
                  long et = System.currentTimeMillis();
                  log(
                      " Total Records Processed: ["
                          + i
                          + "] Time taken for [100] records: ["
                          + (et - st) / 1000
                          + "] seconds");
                  st = System.currentTimeMillis();
                }
                ODatabaseDocument graph = graphFactory.acquire();
                try {
                  Iterable<OElement> vtxs = graph.command(new OCommandSQL(query)).execute();
                  boolean retry = true;
                  for (OElement vtx : vtxs) {
                    if (retry) {
                      retry = true;
                      boolean isException = false;
                      Exception tex = null;
                      int k = 1;
                      for (; k <= 100 && retry; k++) {

                        OVertex vtx1 = vtx.asVertex().get();
                        try {
                          vtx1.setProperty("prop5", "prop55");
                          vtx1.setProperty("updateTime", new Date().toString());
                          graph.commit();
                          if (isException) {
                            // log("********** [" + id + "][" + k + "] Update success after
                            // distributed lock Exception for vertex " +
                            // vtx1);
                          }
                          retry = false;
                          break;
                        } catch (Exception ex) {
                          if (ex instanceof ODatabaseException
                              || ex instanceof ONeedRetryException
                              || ex instanceof ODistributedException) {
                            tex = ex;
                            if (ex instanceof ONeedRetryException
                                || ex.getCause() instanceof ONeedRetryException) {
                              // log("[" + id + "][" + vtx + "][ Retry: " + k + "] OrientDB
                              // Exception [" + ex + "]");
                              try {
                                Thread.sleep(new Random().nextInt(500));
                              } catch (InterruptedException e) {
                                e.printStackTrace();
                              }
                              vtx1.reload();
                            } else {
                              log(
                                  "["
                                      + id
                                      + "]["
                                      + vtx
                                      + "][ Retry: "
                                      + k
                                      + "] Failed to update. OrientDB Exception ["
                                      + ex
                                      + "]");
                            }
                            isException = true;
                          } else {
                            tex = ex;
                            log(
                                "["
                                    + id
                                    + "]["
                                    + k
                                    + "] Failed to update non OrientDB Exception ["
                                    + ex
                                    + "] for vertex ["
                                    + vtx1
                                    + "]");
                          }
                          Thread.sleep(200);
                        }
                      }
                      if (retry) {
                        log(
                            "*******#################******* ["
                                + id
                                + "]["
                                + vtx
                                + "][ Retry: "
                                + k
                                + "] Failed to update after Exception ["
                                + ((tex != null) ? tex : "----")
                                + "]");
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- End update based on Java API
  // -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Database Methods
  // ---------------------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  /** @return */
  public String getDBURL() {
    return "remote:" + "localhost:2424;localhost:2425;localhost:2426" + "/" + dbName;
  }

  private OrientDB getOrientDB() {
    if (orientDB == null) {
      log("Datastore pool created with size : 50, db location: " + getDBURL());
      orientDB =
          new OrientDB(
              "remote:" + "localhost:2424;localhost:2425;localhost:2426",
              "root",
              "root",
              OrientDBConfig.defaultConfig());
    }
    return orientDB;
  }

  private ODatabasePool getGraphFactory() {
    if (graphReadFactory == null) {
      log("Datastore pool created with size : 50, db location: " + getDBURL());
      graphReadFactory =
          new ODatabasePool(getOrientDB(), dbName, "root", "root", OrientDBConfig.defaultConfig());
    }
    return graphReadFactory;
  }

  /** */
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

      ODatabaseDocument orientGraph = getGraphFactory().acquire();

      orientGraph.command(new OCommandSQL("ALTER DATABASE custom strictSQL=false")).execute();
      orientGraph.close();
    } catch (Exception ex) {
      log("Failed to create database", ex);
    }
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- End Database Methods
  // -----------------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // -----------------------------------------------------------------------------------------------------------
  // ----------------------- Utility Methods
  // ------------------------------------------------------------------
  // -----------------------------------------------------------------------------------------------------------

  /**
   * Create vertex and edge type with non unique index.
   *
   * @param orientGraph
   * @param className
   */
  public void createVertexType(
      ODatabaseDocument orientGraph, String className, String property, String keyIndexProperty) {
    String edgeClassName = className;
    String vertexClassName = className + "Node";
    OClass clazz = orientGraph.getClass(edgeClassName);
    if (clazz == null) {
      log("Creating edge type - " + edgeClassName);
      orientGraph.createEdgeClass(edgeClassName);
    } else {
      log("Edge " + edgeClassName + " already exists");
    }
    clazz = orientGraph.getClass(vertexClassName);
    if (clazz == null) {
      log("Creating vertex type - " + vertexClassName);
      clazz = orientGraph.createVertexClass(vertexClassName);
      clazz.createProperty(property, OType.STRING);
      clazz.createProperty(keyIndexProperty, OType.STRING);
      clazz.createIndex(
          vertexClassName + "." + keyIndexProperty, INDEX_TYPE.NOTUNIQUE, keyIndexProperty);
      clazz.createIndex(vertexClassName + "_Index_" + property, INDEX_TYPE.NOTUNIQUE, property);
    } else {
      log("Class " + vertexClassName + " already exists");
    }
  }

  /**
   * Create vertex and edge type with non unique index.
   *
   * @param orientGraph
   * @param className
   */
  public void createVertexTypeWithUniqueIndex(
      ODatabaseDocument orientGraph, String className, String property, String keyIndexProperty) {
    String edgeClassName = className;
    String vertexClassName = className + "Node";
    OClass clazz = orientGraph.getClass(edgeClassName);
    if (clazz == null) {
      log("Creating edge type - " + edgeClassName);
      orientGraph.createEdgeClass(edgeClassName);
    } else {
      log("Edge " + edgeClassName + " already exists");
    }
    clazz = orientGraph.getClass(vertexClassName);
    if (clazz == null) {
      log("Creating vertex type - " + vertexClassName);
      clazz = orientGraph.createVertexClass(vertexClassName);
      clazz.createProperty(property, OType.STRING);
      clazz.createProperty(keyIndexProperty, OType.STRING);
      clazz.createIndex(
          vertexClassName + "." + keyIndexProperty, INDEX_TYPE.UNIQUE, keyIndexProperty);
      clazz.createIndex(vertexClassName + "_Index_" + property, INDEX_TYPE.UNIQUE, property);
    } else {
      log("Class " + vertexClassName + " already exists");
    }
  }

  public static final void log(String message) {
    System.out.println("[" + Thread.currentThread().getName() + "][" + getDate() + "] " + message);
  }

  public static final void log(String message, Throwable th) {
    System.out.println("[" + Thread.currentThread().getName() + "][" + getDate() + "] " + message);
    th.printStackTrace();
  }

  /** @return */
  private static String getDate() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return formatter.format(new Date());
  }

  /**
   * Create thread pool.
   *
   * @param threadName
   * @param poolSize
   * @return
   */
  public static ExecutorService newFixedThreadPool(String threadName, int poolSize) {
    WorkerThreadFactory builder = new WorkerThreadFactory(threadName);
    return Executors.newFixedThreadPool(poolSize, builder);
  }

  public static class WorkerThreadFactory implements ThreadFactory {
    private int counter = 0;
    private String prefix = "worker";

    public WorkerThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, prefix + "-" + counter++);
    }
  }

  private void promptTest() {
    Scanner keyboard = new Scanner(System.in);
    System.out.println("Choose database test");
    System.out.println("0. Create database Records");
    System.out.println("1. Update using Java API - Same vertex from all threads");
    System.out.println("2. Update using SQL - Same vertex from all threads");
    System.out.println("3. Update using Java API - Different vertex");
    System.out.println("4. Update using SQL - Different vertex");

    int choice = keyboard.nextInt();
    if (choice == 0) {
      createDBData();
      promptTest();
    } else if (choice == 1) {
      runVertexUpdateTest();
    } else if (choice == 2) {
      runSQLUpdateTest();
    } else if (choice == 3) {
      runMultipleVertexUpdateTest();
    } else if (choice == 4) {
      runMultipleVertexSQLUpdateTest();
    } else {
      System.out.println("Try again.....");
      choice = -1;
      promptTest();
    }
    keyboard.close();
  }

  // ------------------------------------------------------------------------------------------------------------
  // ---------------------- End of Utility methods
  // -------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------

  public static void main(String args[]) {
    // DistributedDatabaseCRUDIT test = new DistributedDatabaseCRUDIT("testdb1");
    // test.createDBData();

    DistributedDatabaseCRUDIT test1 = new DistributedDatabaseCRUDIT("testdb", 2000);
    test1.promptTest();
    Runtime.getRuntime().halt(0);
  }
}
