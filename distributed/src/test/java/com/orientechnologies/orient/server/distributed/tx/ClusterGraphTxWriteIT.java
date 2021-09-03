package com.orientechnologies.orient.server.distributed.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A basic benchmark that measures latency, throughput and retries in case of conflicting
 * distributed txs.
 *
 * <p>For N servers, we have M >= N groups. In each group there are V vertices. Each group is
 * processed by one thread. Each thread, goes through the vertices and for each vertex i, it tries
 * to connect it to one vertex from the other groups (chosen to have same i in the group, or
 * randomly) by creating a link called 'knows-group-<GroupID>'.
 *
 * <p>The TxWriters could be connected to only one server (single-master) or distributed among the
 * servers (multi-master).
 *
 * <p>If CREATE_EDGES_RANDOMLY is false, txs updating vertex i in each group will conflict. However,
 * contention happens only between txs that are connecting vertices of the same id. Another way to
 * increase contention is to batch together B rounds of connecting vertices in one tx, and slide the
 * range of vertex IDs for next rounds. This way txs across the next B rounds also conflict.
 */
public class ClusterGraphTxWriteIT {
  private TestSetup setup;
  private SetupConfig config;
  private String server0;
  private List<String> edgeClassNames;
  private Map<String, OrientDB> serverRemotes = new HashMap<>();
  // configs
  private static final boolean MULTI_MASTER_WRITE = true;
  // whether to connect vertices with the same id across groups or not.
  private static final boolean CREATE_EDGES_RANDOMLY = false;
  // number of vertices in each group
  private static final int GROUP_SIZE = 100;
  // Number of vertices in the group to connect with the other groups, in each tx.
  // cannot be more than number of vertices in a group.
  private static final int TX_BATCH_SIZE = 1;
  private static final int NO_OF_GROUPS = 3;
  private static final int TX_MAX_RETRIES = 25;

  @Before
  public void before() throws Exception {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    setup = TestSetupUtil.create(config);
    setup.setup();

    edgeClassNames = new ArrayList<>(NO_OF_GROUPS);
    for (int i = 0; i < NO_OF_GROUPS; i++) {
      edgeClassNames.add(String.format("knows-group-%d", i));
    }

    try (OrientDB remote =
        setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig())) {
      System.out.println("Creating database and schema...");
      remote.execute(
          "create database ? plocal users(admin identified by 'admin' role admin)", "test");
      try (ODatabaseSession session = remote.open("test", "root", "test")) {
        /* Create schema */
        OClass personClass = session.createVertexClass("Person");
        personClass.createProperty("id", OType.STRING);

        OClass person = session.getClass("Person");
        person.createIndex("Person.id", OClass.INDEX_TYPE.UNIQUE, "id");

        for (String edgeClassName : edgeClassNames) {
          session.createEdgeClass(edgeClassName);
        }
      }
    }

    OrientDBConfig orientDBConfig = OrientDBConfig.defaultConfig();
    // Do reties in the code, so we can count the conflicts/rollbacks.
    orientDBConfig
        .getConfigurations()
        .setValue(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY, 1);
    // make sure we always use embedded edges, otherwise connecting edges doesn't necessarily
    // conflict with each other.
    orientDBConfig
        .getConfigurations()
        .setValue(
            OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, Integer.MAX_VALUE);

    for (String server : config.getServerIds()) {
      serverRemotes.put(server, setup.createRemote(server, "root", "test", orientDBConfig));
    }
  }

  @Test
  public void test() throws InterruptedException {
    List<TxWriter> txWriters = new LinkedList<>();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(NO_OF_GROUPS);
    CountDownLatch verticesCreated = new CountDownLatch(NO_OF_GROUPS);

    List<String> serverNameList = new ArrayList<>(config.getServerIds());
    for (int groupId = 0; groupId < NO_OF_GROUPS; groupId++) {
      // assign which server will receive writes/updates for the group
      final String server = getServerForGroup(groupId, serverNameList);
      final String workerId = "Writer" + groupId;
      TxWriterConfig writerConfig =
          new TxWriterConfig(
              workerId,
              groupId,
              GROUP_SIZE,
              TX_MAX_RETRIES,
              TX_BATCH_SIZE,
              NO_OF_GROUPS,
              CREATE_EDGES_RANDOMLY);
      TxWriter txWriter = new TxWriter(server, serverRemotes.get(server), writerConfig);
      txWriters.add(txWriter);
      new Thread(() -> txWriter.start(start, verticesCreated, finished)).start();
    }

    start.countDown();

    if (!verticesCreated.await(180, TimeUnit.SECONDS)) {
      fail("Timed out waiting for vertices to be created!");
    }
    long startTimeSec = System.currentTimeMillis() / 1000;
    System.out.println("Waiting for writers...");

    finished.await();
    long finishTimeSec = System.currentTimeMillis() / 1000;

    for (String server : serverRemotes.keySet()) {
      OrientDB remote = serverRemotes.get(server);
      try (ODatabaseSession session = remote.open("test", "root", "test")) {
        verifyAndPrintDBStats(session, server);
      }
    }

    verifyAndPrintTxStats(txWriters, startTimeSec, finishTimeSec);
  }

  private void verifyAndPrintDBStats(final ODatabaseSession session, String server) {
    List<Integer> outEdgePerVertex = null;
    try (OResultSet rset = session.query("select outE().size() as outCount from Person")) {
      outEdgePerVertex =
          rset.stream()
              .map(res -> (Integer) res.getProperty("outCount"))
              .collect(Collectors.toList());
    }
    int noOfVertices = outEdgePerVertex.size();
    assertEquals(GROUP_SIZE * NO_OF_GROUPS, noOfVertices);

    int totalNoOfEdges = outEdgePerVertex.stream().mapToInt(Integer::intValue).sum();
    assertEquals(GROUP_SIZE * NO_OF_GROUPS * (NO_OF_GROUPS - 1) * TX_BATCH_SIZE, totalNoOfEdges);

    if (!CREATE_EDGES_RANDOMLY) {
      outEdgePerVertex.forEach(
          count -> assertEquals((NO_OF_GROUPS - 1) * TX_BATCH_SIZE, count.intValue()));
    }

    System.out.printf(
        "* On server %s: vertex count: %d, edge count: %d.\n",
        server, noOfVertices, totalNoOfEdges);
  }

  private void verifyAndPrintTxStats(List<TxWriter> txWriters, long startSec, long finishSec) {
    List<List<TxStat>> txStatsList = new ArrayList<>();
    assertEquals(NO_OF_GROUPS, txWriters.size());
    for (TxWriter txWriter : txWriters) {
      List<TxStat> txStats = txWriter.getTxStats();
      assertEquals(GROUP_SIZE, txStats.size());
      txStatsList.add(txStats);
    }

    long committed =
        txStatsList.stream().flatMap(Collection::stream).filter(TxStat::isCommitted).count();

    double commitRate = (double) committed / (GROUP_SIZE * NO_OF_GROUPS);

    int retries =
        txStatsList.stream()
            .flatMap(Collection::stream)
            .filter(TxStat::isCommitted)
            .map(txStat -> txStat.retries)
            .mapToInt(Integer::intValue)
            .sum();

    double averageLatency =
        txStatsList.stream()
            .flatMap(Collection::stream)
            .filter(TxStat::isCommitted)
            .map(TxStat::getLatency)
            .mapToLong(Long::longValue)
            .average()
            .getAsDouble();

    int percentile99Index = (int) (committed * 0.99) - 1;
    long percentile99 =
        txStatsList.stream()
            .flatMap(Collection::stream)
            .filter(TxStat::isCommitted)
            .map(TxStat::getLatency)
            .sorted()
            .skip(percentile99Index - 1)
            .findFirst()
            .get();

    int size = (int) (finishSec - startSec);
    int[] throughput = new int[size + 1];
    txStatsList.stream()
        .flatMap(Collection::stream)
        .filter(TxStat::isCommitted)
        .forEach(
            txStat -> {
              int idx = (int) ((txStat.commitTimestamp / 1000) - startSec);
              if (idx >= throughput.length) {
                System.out.println("WARNING: commit timestamp is greater than throughput array!");
              } else {
                throughput[idx]++;
              }
            });
    System.out.printf("Throughput per sec.: %s\n", Arrays.toString(throughput));

    System.out.printf(
        "* Total time: %d s, \n  committed txs: %d, \n  commit rate: %f, \n  "
            + "retries: %d, \n  average latency: %f ms, \n  99%% latency: %d ms.\n",
        finishSec - startSec, committed, commitRate, retries, averageLatency, percentile99);
  }

  private String getServerForGroup(int groupId, List<String> servers) {
    if (!MULTI_MASTER_WRITE) {
      return servers.get(0);
    }
    return servers.get(groupId % servers.size());
  }

  @After
  public void after() {
    try {
      OrientDB remote = serverRemotes.get(server0);
      if (remote != null) {
        remote.drop("test");
      }
      for (OrientDB r : serverRemotes.values()) {
        r.close();
      }
    } finally {
      setup.teardown();
      ODatabaseDocumentTx.closeAll();
    }
  }
}
