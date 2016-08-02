package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public final class DistributedDatabaseCRUDTest {

  /*****************************************************************************************************
   * 1. Create 51 Edge and Vertex. Edge: Test, Test1-Test50, Vertex: TestNode, Test1Node - Test50Node 2. Populate TestNode with 50
   * records with unique index for property1 and property2 3. Populate Test1Node - Test31Node with 500 records. Each record will
   * have edge to TestNode 4. Start a pool of threads to update one record in TestNode using both Java API and SQL
   *
   ******************************************************************************************************/
  private String             dbName;
  private OrientGraphFactory graphReadFactory;

  public DistributedDatabaseCRUDTest(String dbName) {
    this.dbName = dbName;
  }

  public void createDBData() {
    log("Creating data for database " + dbName);
    checkAndCreateDatabase(dbName);
    int totalClassCount = 50;
    int mainNodeDataCount = 50;
    OrientBaseGraph orientGraph = new OrientGraphNoTx(getDBURL());
    createVertexTypeWithUniqueIndex(orientGraph, "Test", "property1", "property2");
    for (int i = 1; i <= totalClassCount; i++) {
      createVertexType(orientGraph, "Test" + i, "property1", "property2");
    }
    orientGraph.shutdown();

    OrientBaseGraph graph = getGraphFactory().getTx();

    boolean addTestData = true;
    String query = "select count(*) as dataCount from TestNode";
    Iterable<Vertex> vtxs = graph.command(new OCommandSQL(query)).execute();
    for (Vertex vtx : vtxs) {
      long count = vtx.getProperty("dataCount");
      addTestData = (count == 0);
    }

    if (addTestData) {
      for (int i = 1; i <= mainNodeDataCount; i++) {
        Vertex vertex = graph.addVertex("class:TestNode");
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
        graph.commit();
        if ((i % 100) == 0) {
          log("Created " + i + " nodes");
        }
      }
    }
    int edgeCounter = 1;
    for (int j = 1; j <= totalClassCount; j++) {
      String edgeName = "Test" + j;
      String className = edgeName + "Node";
      for (int i = 1; i <= 500; i++) {
        Vertex vertex = graph.addVertex("class:" + className);
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
        graph.commit();
        if ((i % 50) == 0) {
          System.out.print(".[" + j + "]" + i + ".");
        }
        String edgeSQL = "Create EDGE " + edgeName + " FROM (SELECT FROM TestNode WHERE property1='value1-" + edgeCounter + "') "
            + "TO (SELECT FROM " + className + " WHERE property1='value1-" + i + "') ";
        edgeCounter++;
        if (edgeCounter > mainNodeDataCount) {
          edgeCounter = 1;
        }
        graph.command(new OCommandSQL(edgeSQL)).execute();
      }
      System.out.println();
    }
    graph.shutdown();
    log("Done. Creating data for database " + dbName);
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start update based on SQL -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  public void runSQLUpdateTest() {
    log("Running SQL based vertex update");
    List<Future<?>> ths = new ArrayList<Future<?>>();
    ExecutorService executorService = newFixedThreadPool("testnode-sql-update", 10);
    for (int i = 0; i < 10; i++) {
      Future<?> future = executorService.submit(startSQLUpdateThread(i, getGraphFactory()));
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

  private Runnable startSQLUpdateThread(final int id, final OrientGraphFactory graphFactory) {
    Runnable th = new Runnable() {
      @Override
      public void run() {
        long st = System.currentTimeMillis();
        try {
          boolean isRunning = true;
          for (int i = 1; i < 10000000 && isRunning; i++) {
            if ((i % 250) == 0) {
              long et = System.currentTimeMillis();
              log(" [" + id + "][" + new Date() + "] Total Records Processed: [" + i + "] Time taken for [250] records: ["
                  + (et - st) / 1000 + "] seconds");
              st = System.currentTimeMillis();
            }
            OrientGraph graph = graphFactory.getTx();
            try {
              boolean update = true;
              boolean isException = false;
              Exception tex = null;
              String sql = "Update TestNode set prop5='" + String.valueOf(System.currentTimeMillis()) + "', updateTime='"
                  + new Date().toString() + "' where property4='value4-1'";
              int k = 1;
              for (; k <= 100 && update; k++) {
                try {
                  graph.command(new OCommandSQL(sql)).execute();
                  if (isException) {
                    // log("********** [" + id + "][" + k + "] Update success after distributed lock Exception");
                  }
                  update = false;
                  break;
                } catch (Exception ex) {
                  tex = ex;

                  if (ex instanceof ODatabaseException || ex instanceof ONeedRetryException
                      || ex instanceof ODistributedException) {
                    if (ex instanceof ONeedRetryException || ex.getCause() instanceof ONeedRetryException) {
                      // update is true. retry
                      Thread.sleep( new Random().nextInt(500));
                    } else {
                      log("[" + id + "][ Retry: " + k + "] Failed to update. OrientDB Exception [" + ex + "]");
                    }
                    isException = true;
                  } else
                    log("[" + id + "][" + k + "] Failed non OrientDB Exception [" + ex + "]");
                }

              }
              if (update) {
                log("*******#################******* [" + id + "][ Retry: " + k + "] Failed to update after Exception ["
                    + ((tex != null) ? tex : "----") + "]for vertex with property4='value4-1'");
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- End update based on SQL -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Start update based on Java API -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  private void runVertexUpdateTest() {
    log("Running Java API based vertex update");
    List<Future<?>> ths = new ArrayList<Future<?>>();
    ExecutorService executorService = newFixedThreadPool("testnode-java-update", 10);
    for (int i = 0; i < 10; i++) {
      Future<?> future = executorService.submit(startVertexUpdateThread(i, getGraphFactory()));
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

  private Runnable startVertexUpdateThread(final int id, final OrientGraphFactory graphFactory) {
    Runnable th = new Runnable() {
      @Override
      public void run() {

        long st = System.currentTimeMillis();
        try {
          String query = "select from TestNode where property4='value4-1'";
          boolean isRunning = true;
          for (int i = 1; i < 100000000 && isRunning; i++) {
            if ((i % 250) == 0) {
              long et = System.currentTimeMillis();
              log(" Total Records Processed: [" + i + "] Time taken for [250] records: [" + (et - st) / 1000 + "] seconds");
              st = System.currentTimeMillis();
            }
            OrientGraph graph = graphFactory.getTx();
            try {
              Iterable<Vertex> vtxs = graph.command(new OCommandSQL(query)).execute();
              boolean retry = true;
              for (Vertex vtx : vtxs) {
                if (retry) {
                  retry = true;
                  boolean isException = false;
                  Exception tex = null;
                  int k = 1;
                  for (; k <= 100 && retry; k++) {
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
                    } catch (Exception ex) {
                      tex = ex;

                      if (ex instanceof ODatabaseException || ex instanceof ONeedRetryException
                          || ex instanceof ODistributedException) {
                        if (ex instanceof ONeedRetryException || ex.getCause() instanceof ONeedRetryException) {
                          Thread.sleep( new Random().nextInt(500));
                        } else {
                          log("[" + id + "][" + vtx + "][ Retry: " + k + "] Failed to update. OrientDB Exception [" + ex + "]");
                        }
                        vtx1.reload();
                        isException = true;
                      } else
                        log("[" + id + "][" + k + "] Failed to update non OrientDB Exception [" + ex + "] for vertex [" + vtx1
                            + "]");
                    }
                  }
                  if (retry) {
                    log("*******#################******* [" + id + "][" + vtx + "][ Retry: " + k
                        + "] Failed to update after Exception [" + ((tex != null) ? tex : "----") + "]");
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- End update based on Java API -------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- Database Methods ---------------------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  /**
   * @return
   */
  public String getDBURL() {
    return "remote:" + "localhost:2424;localhost:2425;localhost:2426" + "/" + dbName;
  }

  private OrientGraphFactory getGraphFactory() {
    if (graphReadFactory == null) {
      log("Datastore pool created with size : 50, db location: " + getDBURL());
      graphReadFactory = new OrientGraphFactory(getDBURL());
      graphReadFactory.setupPool(1, 50);
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

  // ----------------------------------------------------------------------------------------------------------------
  // ---------------------------- End Database Methods -----------------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  // -----------------------------------------------------------------------------------------------------------
  // ----------------------- Utility Methods ------------------------------------------------------------------
  // -----------------------------------------------------------------------------------------------------------

  /**
   * Create vertex and edge type with non unique index.
   *
   * @param orientGraph
   * @param className
   */
  public void createVertexType(OrientBaseGraph orientGraph, String className, String property, String keyIndexProperty) {
    String edgeClassName = className;
    String vertexClassName = className + "Node";
    OClass clazz = orientGraph.getEdgeType(edgeClassName);
    if (clazz == null) {
      log("Creating edge type - " + edgeClassName);
      orientGraph.createEdgeType(edgeClassName);
    } else {
      log("Edge " + edgeClassName + " already exists");
    }
    clazz = orientGraph.getVertexType(vertexClassName);
    if (clazz == null) {
      log("Creating vertex type - " + vertexClassName);
      clazz = orientGraph.createVertexType(vertexClassName);
      clazz.createProperty(property, OType.STRING);
      clazz.createProperty(keyIndexProperty, OType.STRING);
      orientGraph.createKeyIndex(keyIndexProperty, Vertex.class, new Parameter("class", vertexClassName));
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
  public void createVertexTypeWithUniqueIndex(OrientBaseGraph orientGraph, String className, String property,
      String keyIndexProperty) {
    String edgeClassName = className;
    String vertexClassName = className + "Node";
    OClass clazz = orientGraph.getEdgeType(edgeClassName);
    if (clazz == null) {
      log("Creating edge type - " + edgeClassName);
      orientGraph.createEdgeType(edgeClassName);
    } else {
      log("Edge " + edgeClassName + " already exists");
    }
    clazz = orientGraph.getVertexType(vertexClassName);
    if (clazz == null) {
      log("Creating vertex type - " + vertexClassName);
      clazz = orientGraph.createVertexType(vertexClassName);
      clazz.createProperty(property, OType.STRING);
      clazz.createProperty(keyIndexProperty, OType.STRING);
      orientGraph.createKeyIndex(keyIndexProperty, Vertex.class, new Parameter("class", vertexClassName),
          new Parameter("type", "UNIQUE"));
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

  /**
   * @return
   */
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
    private int    counter = 0;
    private String prefix  = "worker";

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
    System.out.println("1. Update using Java API");
    System.out.println("2. Update using SQL");

    int choice = keyboard.nextInt();
    if (choice == 0) {
      createDBData();
      promptTest();
    } else if (choice == 1) {
      runVertexUpdateTest();
    } else if (choice == 2) {
      runSQLUpdateTest();
    } else {
      System.out.println("Try again.....");
      choice = -1;
      promptTest();
    }
    keyboard.close();
  }

  // ------------------------------------------------------------------------------------------------------------
  // ---------------------- End of Utility methods -------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------

  public static void main(String args[]) {
    // DistributedDatabaseCRUDTest test = new DistributedDatabaseCRUDTest("testdb1");
    // test.createDBData();
    DistributedDatabaseCRUDTest test1 = new DistributedDatabaseCRUDTest("testdb");
    test1.promptTest();
    Runtime.getRuntime().halt(0);
  }

}
