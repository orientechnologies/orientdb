package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class OrientCommitMTTestIT {
  public static final String DB_URL = "plocal:./avltreetest";
  public static final String DB_USER = "admin";
  public static final String DB_PASSWORD = "admin";
  private static final String TEST_CLASS = "ORIENT_COMMIT_TEST";
  private static final String THREAD_ID = "ThreadId";
  private static final String ID = "IdField";

  private String failureMessage = "";
  private boolean isValidData;
  private TestExecutor[] threads;

  final int threadCount = 5;
  final int maxSleepTime = 100;
  final int maxOpCount = 6;
  final int initialCacheSize = 10;
  final AtomicInteger idGenerator = new AtomicInteger(1);

  private static Random random = new Random();
  private static OrientGraphFactory factory;

  @Before
  public void setUp() throws IOException {
    if (DB_URL.startsWith("remote:")) {
      OServerAdmin serverAdmin = new OServerAdmin(DB_URL);
      serverAdmin.connect("root", ODatabaseHelper.getServerRootPassword());

      if (serverAdmin.existsDatabase(OrientGraphTest.getStorageType())) {
        serverAdmin.dropDatabase(OrientGraphTest.getStorageType());
      }
      serverAdmin.createDatabase(DB_URL, "graph", OrientGraphTest.getStorageType());
    } else {
      OrientGraph graph = new OrientGraph(DB_URL, DB_USER, DB_PASSWORD);
      graph.drop();
      graph = new OrientGraph(DB_URL, DB_USER, DB_PASSWORD);
      graph.shutdown();
    }
    factory = new OrientGraphFactory(DB_URL).setupPool(5, 10);

    buildSchemaAndSeed();
    this.isValidData = true;
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (DB_URL.startsWith("remote:")) {
      OServerAdmin serverAdmin = new OServerAdmin(DB_URL);
      serverAdmin.connect("root", ODatabaseHelper.getServerRootPassword());

      if (serverAdmin.existsDatabase(OrientGraphTest.getStorageType())) {
        serverAdmin.dropDatabase(OrientGraphTest.getStorageType());
      }
    } else {
      OrientGraph graph = new OrientGraph(DB_URL, DB_USER, DB_PASSWORD);
      graph.drop();
    }
  }

  @Test
  @Ignore
  public void testWithTransactionEmbeddedRidBag() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);

    try {
      System.setOut(
          new PrintStream(new File("target/log/CommitTestTransactionalEmbeddedRidBag.txt")));
    } catch (FileNotFoundException e) {
    }
    // set to run until it fails with the transaction set to true
    executeTest(this.threadCount, this.maxSleepTime, this.maxOpCount, this.initialCacheSize, 2);
  }

  @Test
  public void testSingleThreadWithTransactionEmbeddedRidBag() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);

    try {
      System.setOut(
          new PrintStream(
              new File("target/log/CommitTestTransactionalSingleThreadEmbeddedRidBag.txt")));
    } catch (FileNotFoundException e) {
    }
    // set to run 5 minutes with the transaction set to true
    executeTest(1, this.maxSleepTime, this.maxOpCount, this.initialCacheSize, 2);
  }

  @Test
  @Ignore
  public void testWithTransactionSBTreeRidBag() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);

    try {
      System.setOut(
          new PrintStream(new File("target/log/CommitTestTransactionalSBTreeRidBag.txt")));
    } catch (FileNotFoundException e) {
    }
    // set to run until it fails with the transaction set to true
    executeTest(this.threadCount, this.maxSleepTime, this.maxOpCount, this.initialCacheSize, 2);
  }

  @Test
  public void testSingleThreadWithTransactionSBTreeRidBag() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);

    try {
      System.setOut(
          new PrintStream(
              new File("target/log/CommitTestTransactionalSingleThreadSBTreeRidBag.txt")));
    } catch (FileNotFoundException e) {
    }
    // set to run 5 minutes with the transaction set to true
    executeTest(1, this.maxSleepTime, this.maxOpCount, this.initialCacheSize, 2);
  }

  /** If failure occurs, set its message and kill all other running threads */
  public void setFailureMessage(final String message) {
    this.isValidData = false;
    this.failureMessage = message;
    // exception reproduced - kill all threads
    for (TestExecutor thread : this.threads) {
      if (thread != null) {
        thread.shutdown();
      }
    }
  }

  /** Get failure message */
  public String getFailureMessage() {
    return this.failureMessage;
  }

  private void executeTest(
      final int threadCount,
      final int maxSleepTime,
      final int maxOpCount,
      final int initialCacheSize,
      final int runtimeInMin) {
    CountDownLatch endLatch = new CountDownLatch(threadCount);

    this.threads = new TestExecutor[threadCount];
    for (int i = 0; i < threadCount; i++) {
      this.threads[i] = new TestExecutor(i, endLatch, maxSleepTime, maxOpCount);
      System.out.println("Starting thread id: " + i);
      this.threads[i].seedData(initialCacheSize);
      new Thread(this.threads[i]).start();
    }

    if (runtimeInMin > 0) {
      try {
        Thread.sleep(60000 * runtimeInMin);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      int successfulThreadCount = 0;
      for (TestExecutor thread : this.threads) {
        if (!thread.isShutdown()) {
          ++successfulThreadCount;
          thread.shutdown();
        }
      }
      // verify number or alive threads to number of thread specified to run
      Assert.assertEquals(threadCount, successfulThreadCount);
    }

    // check if failure has occurred and fail test with message
    try {
      endLatch.await();
    } catch (InterruptedException e) {
    } finally {
      if (!this.isValidData) {
        Assert.fail(getFailureMessage());
      }
    }
  }

  class TestExecutor implements Runnable {
    private int maxSleepTime;
    private final CountDownLatch endLatch;
    private boolean shutdown;
    private int maxOpCount;
    private final List<IdPair> cache;
    private final int threadId;

    public TestExecutor(
        final int threadId,
        final CountDownLatch endLatch,
        final int maxSleepTime,
        final int maxOpCount) {
      this.endLatch = endLatch;
      this.maxSleepTime = maxSleepTime;
      this.maxOpCount = maxOpCount;
      this.shutdown = false;
      this.cache = new ArrayList<IdPair>();
      this.threadId = threadId;
    }

    public void seedData(final int initialCacheSize) {
      for (int i = 0; i < initialCacheSize; i++) {
        IdPair newNode = insertNewNode(null);
        ORID recordId = newNode.getOrid();
        Integer id = newNode.getCustomId();
        this.cache.add(new IdPair(recordId, id));
      }
    }

    public void run() {
      try {
        Thread.sleep((long) (Math.random() * this.maxSleepTime));
      } catch (InterruptedException e) {
        // swallow - irrelevant
      }
      try {
        while (!this.shutdown) {
          commitOperations();
        }
      } finally {
        this.endLatch.countDown();
      }
    }

    /**
     * Perform a set of insert or delete operations (picked at random) with variable transaction
     * flag
     */
    private void commitOperations() {
      OrientGraph graph = factory.getTx(); // new OrientGraph(DB_URL, DB_USER, DB_PASSWORD);
      try {
        List<TempCacheObject> tempCache = new ArrayList<TempCacheObject>();
        try {
          // generate random operation list
          List<Operation> operations = generateOperations(this.maxOpCount);
          for (Operation operation : operations) {
            if (Operation.INSERT.equals(operation)) {
              // perform insert operation
              IdPair insertedNode = insertNewNode(graph);
              ORID insertId = insertedNode.getOrid();
              // add inserted id to temp cache
              tempCache.add(new TempCacheObject(operation, insertId, insertedNode.getCustomId()));
            } else if (Operation.DELETE.equals(operation)) {
              // get delete id
              ORID deleteId = getRandomIdForThread(graph);
              if (deleteId != null) {
                // perform delete operation
                Integer customId = deleteExistingNode(deleteId, graph);
                // add deleted id to temp cache
                tempCache.add(new TempCacheObject(operation, deleteId, customId));
              } else {
                System.out.println(
                    "ThreadId: " + this.threadId + " no ids in database for thread to delete.");
              }
            }
          }

          graph.commit();

        } catch (Exception e) {
          graph.rollback();
          tempCache.clear();
          System.out.println(
              "ThreadId: "
                  + this.threadId
                  + " Rolling back transaction due to "
                  + e.getClass().getSimpleName()
                  + " "
                  + e.getMessage());
          e.printStackTrace(System.out);
        }
        // update permanent cache from temp cache
        updateCache(tempCache);

        validateCustomIdsAgainstDatabase(graph);
        validateDatabase(this.cache, graph);
      } catch (Exception e) {
        System.out.println(
            "ThreadId: " + this.threadId + " threw a validation exception: " + e.getMessage());
        e.printStackTrace(System.out);
        // validation failed - set failure message
        setFailureMessage(e.getMessage());
        this.shutdown = true;
      } finally {
        graph.shutdown();
      }
    }

    private void validateCustomIdsAgainstDatabase(OrientGraph graph) throws Exception {
      List<Vertex> recordsInDb = new ArrayList<Vertex>();
      for (Vertex v : graph.getVerticesOfClass(TEST_CLASS)) recordsInDb.add(v);

      for (IdPair cacheInstance : this.cache) {
        Integer customId = cacheInstance.getCustomId();
        boolean found = false;
        for (Vertex vertex : recordsInDb) {
          if (vertex.getProperty(ID).equals(customId)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new Exception(
              "Custom id: " + customId + " exists in cache but was not found in db.");
        }
      }
    }

    public boolean isShutdown() {
      return this.shutdown;
    }

    /**
     * Verify that all ids in the permanent cache are in the db. Verify that all ids (for a given
     * thread) in the db are in the permanent cache.
     */
    private void validateDatabase(final List<IdPair> cache, OrientGraph graph) throws Exception {
      for (IdPair idPair : cache) {
        ORID id = idPair.getOrid();
        if (!isInDatabase(id, graph)) {
          throw new Exception(
              "Insert issue: expected record " + id + " was not found in database.");
        }
      }
      for (Vertex vertex : graph.getVerticesOfClass(TEST_CLASS)) {
        if (Integer.valueOf(this.threadId).equals(vertex.getProperty(THREAD_ID))) {
          ORID dbId = ((OrientVertex) vertex).getIdentity();
          Integer customId = vertex.getProperty(ID);
          if (!cache.contains(new IdPair(dbId, customId))) {
            throw new Exception(
                "Delete issue: record id "
                    + dbId
                    + " for thread id "
                    + this.threadId
                    + " was not found in cache.");
          }
        }
      }
    }

    /** Checks to see if an id for a given thread exist in the db. */
    private boolean isInDatabase(final ORID id, OrientGraph orientGraph) throws Exception {
      final OrientVertex vertex = orientGraph.getVertex(id);
      if (vertex != null) {
        if (!Integer.valueOf(this.threadId).equals(vertex.getProperty(THREAD_ID))) {
          return false;
        }
      }
      return vertex != null;
    }

    /**
     * Add id from the temp cache with insert operation to permanent cache. Remove id from permanent
     * cache that has a delete operation in the temp cache.
     *
     * @param tempCache cached objects
     */
    private void updateCache(final List<TempCacheObject> tempCache) {
      for (TempCacheObject tempCacheObject : tempCache) {
        ORID id = tempCacheObject.getOrientId();
        Operation operation = tempCacheObject.getOperation();
        Integer customId = tempCacheObject.getCustomId();
        if (Operation.INSERT.equals(operation)) {
          this.cache.add(new IdPair(id, customId));
        } else if (Operation.DELETE.equals(operation)) {
          this.cache.remove(new IdPair(id, customId));
        }
      }
    }

    /** Insert new node and create edge with the random node in the db. */
    private IdPair insertNewNode(OrientGraph graph) {
      boolean closeDb = false;
      if (graph == null) {
        graph = factory.getTx();
        closeDb = true;
      }

      try {
        Integer id = OrientCommitMTTestIT.this.idGenerator.getAndIncrement();
        OrientVertex vertex =
            graph.addVertex(
                "class:" + TEST_CLASS, THREAD_ID, Integer.valueOf(this.threadId), ID, id);

        ORID randomId = getRandomIdForThread(graph);
        if (randomId != null) {
          OrientVertex v = graph.getVertex(randomId);
          graph.addEdge(null, vertex, v, "contains");
        }
        ORID newRecordId = vertex.getIdentity();
        return new IdPair(newRecordId, id);
      } finally {
        if (closeDb) graph.shutdown();
      }
    }

    /** Delete all edges connected to given vertex and then delete vertex. */
    private Integer deleteExistingNode(final ORID recordId, OrientGraph graph) {
      OrientVertex vertex = graph.getVertex(recordId);
      Integer customId = vertex.getProperty(ID);

      vertex.remove();
      return customId;
    }

    /**
     * Get all of the ids from the db for that class for a given thread id. Return id from the list
     * at random.
     */
    private ORID getRandomIdForThread(OrientGraph graph) {
      boolean closeDb = false;
      if (graph == null) {
        graph = factory.getTx();
        closeDb = true;
      }

      try {
        List<ORID> idsInDb = new ArrayList<ORID>();

        for (Vertex v : graph.getVerticesOfClass(TEST_CLASS)) {
          if (Integer.valueOf(this.threadId).equals(v.getProperty(THREAD_ID))) {
            idsInDb.add(((OrientVertex) v).getIdentity());
          }
        }
        int size = idsInDb.size();
        if (size == 0) {
          return null;
        }
        int index = random.nextInt(size);
        return idsInDb.get(index);
      } finally {
        if (closeDb) graph.shutdown();
      }
    }

    private List<Operation> generateOperations(final int maxOpCount) {
      List<Operation> operationsList = new ArrayList<Operation>();
      int opCount = (int) (Math.random() * maxOpCount / 2 + maxOpCount / 2);
      for (int index = 0; index < opCount; index++) {
        Operation op = Operation.getRandom();
        operationsList.add(op);
      }
      return operationsList;
    }

    private void shutdown() {
      this.shutdown = true;
    }

    private class TempCacheObject {
      private Operation operation;
      private ORID orientId;
      private Integer customId;

      public TempCacheObject(
          final Operation operation, final ORID orientId, final Integer customId) {
        this.operation = operation;
        this.orientId = orientId;
        this.customId = customId;
      }

      public Operation getOperation() {
        return this.operation;
      }

      public ORID getOrientId() {
        return this.orientId;
      }

      public Integer getCustomId() {
        return this.customId;
      }

      public String toString() {
        return "Operation:"
            + this.operation
            + ", ORID:"
            + this.orientId
            + ", CustomId:"
            + this.customId;
      }
    }
  }

  /** Defines two operations types */
  private static enum Operation {
    INSERT,
    DELETE;

    /** Picks operation at random */
    public static Operation getRandom() {
      if (0.55 > Math.random()) {
        return INSERT;
      } else {
        return DELETE;
      }
    }
  }

  private static class IdPair {
    private ORID orid;
    private Integer customId;

    public IdPair(final ORID orid, final Integer customId) {
      super();
      this.orid = orid;
      this.customId = customId;
    }

    public ORID getOrid() {
      return this.orid;
    }

    public Integer getCustomId() {
      return this.customId;
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof IdPair)) {
        return false;
      }
      IdPair idPair = (IdPair) obj;
      if (!idPair.orid.equals(this.orid)) {
        return false;
      }
      if (!idPair.customId.equals(this.customId)) {
        return false;
      }
      return true;
    }
  }

  /** Create schema that has one class and one field */
  public void buildSchemaAndSeed() {
    OrientGraphNoTx graph = factory.getNoTx();
    try {
      OClass nodeClass = graph.createVertexType(TEST_CLASS);
      nodeClass.createProperty(THREAD_ID, OType.INTEGER).setMandatory(true).setNotNull(true);
      nodeClass.createProperty(ID, OType.INTEGER).setMandatory(true).setNotNull(true);
    } finally {
      graph.shutdown();
    }
  }
}
