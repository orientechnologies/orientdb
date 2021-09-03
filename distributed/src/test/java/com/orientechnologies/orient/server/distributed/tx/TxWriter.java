package com.orientechnologies.orient.server.distributed.tx;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** TxWriter issues transactions to only one server. */
public class TxWriter {
  private final TxWriterConfig config;
  private final List<TxStat> txStats = new LinkedList<>();
  private final Random random;
  private final String writerId;
  private final OrientDB remote;
  private final String serverName;

  public TxWriter(String serverName, OrientDB remote, TxWriterConfig config) {
    this.config = config;
    writerId = config.writerId;
    random = new Random();
    this.remote = remote;
    this.serverName = serverName;
  }

  private void createVertices(ODatabaseSession session) {
    for (int i = 0; i < config.groupSize; i++) {
      final int vertexId = i;
      session.executeWithRetry(
          10,
          (Function<ODatabaseSession, Void>)
              oDatabaseSession -> {
                createVertex(oDatabaseSession, createId(config.vertexGroupId, vertexId));
                return null;
              });
    }
  }

  public void start(CountDownLatch start, CountDownLatch verticesCreated, CountDownLatch finished) {
    try {
      start.await();

      try (ODatabaseSession session = remote.open("test", "root", "test")) {
        System.out.printf("%s: creating vertices...\n", config.writerId);
        createVertices(session);
        verticesCreated.countDown();
        verticesCreated.await();

        System.out.printf("%s: connecting vertices.\n", config.writerId);
        connectVertices(session);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      System.out.printf("%s (%s) finished.\n", config.writerId, serverName);
      finished.countDown();
    }
  }

  private Map<Integer, OVertex> chooseDestVertexPerGroup(
      ODatabaseSession session, int fromVertexId, List<Integer> allGroupIds) {
    // find a vertex in each group to connect to the vertex in this group
    return allGroupIds.stream()
        .map(
            targetGroupId -> {
              int vertexToConnect =
                  config.createEdgesRandomly ? random.nextInt(config.groupSize) : fromVertexId;
              OResultSet result =
                  session.query(
                      "select from person where id = ?", createId(targetGroupId, vertexToConnect));
              OVertex vertex = result.next().getVertex().get();
              result.close();
              return new OPair<>(targetGroupId, vertex);
            })
        .collect(Collectors.toMap(OPair::getKey, OPair::getValue));
  }

  private void connectVertices(ODatabaseSession session) {
    // connect vertices of the same id/worker across servers
    List<String> edgeClassNames = generateEdgeClassNames(config.noOfGroups);
    List<Integer> allGroupIds =
        IntStream.range(0, config.noOfGroups).boxed().collect(Collectors.toList());

    for (int startVertex = 0; startVertex < config.groupSize; startVertex++) {
      int retry;
      // consider latency with all retries together.
      long startTimestamp = System.currentTimeMillis();
      for (retry = 0; retry < config.txMaxRetries; retry++) {
        try {
          session.begin();
          // Each tx creates links between vertices with the same id (or random ids)
          // across groups. Depending on tx batch size, this is done for more than one id at a time.
          for (int batch = 0; batch < config.txBatchSize; batch++) {
            int currentVertex = (startVertex + batch) % config.groupSize;
            // connect it with vertices (of the same ID or randomly chosen) in the other groups
            Map<Integer, OVertex> targetPerGroup =
                chooseDestVertexPerGroup(session, currentVertex, allGroupIds);

            OVertex myVertex = targetPerGroup.get(config.vertexGroupId);
            for (Integer gId : allGroupIds) {
              if (!gId.equals(config.vertexGroupId)) {
                createEdge(myVertex, targetPerGroup.get(gId), edgeClassNames.get(gId));
              }
            }
          }
          session.commit();
          long finishTimestamp = System.currentTimeMillis();
          txStats.add(new TxStat(startTimestamp, finishTimestamp, retry));
          break;
        } catch (ONeedRetryException ex) {
          session.rollback();
          System.out.printf("Exception while connecting vertices: %s.\n", ex.getClass().toString());
        }
      }
      if (retry >= config.txMaxRetries) {
        txStats.add(new TxStat(startTimestamp, -1, retry - 1));
      }
    }
  }

  public List<TxStat> getTxStats() {
    return txStats;
  }

  public String getWriterId() {
    return writerId;
  }

  private List<String> generateEdgeClassNames(int noOfGroups) {
    List<String> edgeClassNames = new ArrayList<>(noOfGroups);
    for (int i = 0; i < noOfGroups; i++) {
      edgeClassNames.add(String.format("knows-group-%d", i));
    }
    return edgeClassNames;
  }

  private String createId(int groupId, int vertexId) {
    return String.format("v-%d-%d", groupId, vertexId);
  }

  private static OVertex createVertex(ODatabaseSession graph, String id) {
    OVertex v = graph.newVertex("Person");
    v.setProperty("id", id);
    v.save();
    return v;
  }

  private static OEdge createEdge(OVertex v1, OVertex v2, String edgeClassname) {
    OEdge e = v1.addEdge(v2, edgeClassname);
    e.save();
    return e;
  }
}
