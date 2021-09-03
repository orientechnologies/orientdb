package com.orientechnologies.orient.graph.batch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Simple Multi-threads graph importer. Source file downloaded from
 * http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Books.csv.
 *
 * @author Luca Garulli
 */
public class OGraphImporterMTAPITest {
  static long row = 0;
  static long lastVertexCount = 0;
  static long lastEdgeCount = 0;
  static int parallel = 8;

  public static void main(String[] args) throws IOException, InterruptedException {
    OGlobalConfiguration.ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL.setValue(64);

    // String dbUrl = "memory:amazonReviews";
    final String dbUrl = "plocal:/temp/databases/amazonReviews";

    final File f = new File("/temp/databases/amazonReviews");
    if (f.exists()) OFileUtils.deleteRecursively(f);

    final OrientGraph roGraph = new OrientGraph(dbUrl, "admin", "admin");

    final OrientGraphNoTx graph = new OrientGraphNoTx(dbUrl, "admin", "admin");

    OrientVertexType user = graph.createVertexType("User", 64);
    user.createProperty("uid", OType.STRING);

    user.createIndex(
        "User.uid",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        null,
        "AUTOSHARDING",
        new String[] {"uid"});

    OrientVertexType product = graph.createVertexType("Product", 64);
    product.createProperty("uid", OType.STRING);

    product.createIndex(
        "Product.uid",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        null,
        "AUTOSHARDING",
        new String[] {"uid"});

    graph.createEdgeType("Reviewed");

    final File file = new File("/Users/luca/Downloads/ratings_Books.csv");
    final BufferedReader br = new BufferedReader(new FileReader(file));

    final AtomicLong retry = new AtomicLong();

    Orient.instance()
        .scheduleTask(
            () -> {
              roGraph.makeActive();
              final long vertexCount = roGraph.countVertices();
              final long edgeCount = roGraph.countEdges();

              System.out.println(
                  String.format(
                      "%d vertices=%d %d/sec edges=%d %d/sec retry=%d",
                      row,
                      vertexCount,
                      ((vertexCount - lastVertexCount) * 1000 / 2000),
                      edgeCount,
                      ((edgeCount - lastEdgeCount) * 1000 / 2000),
                      retry.get()));

              lastVertexCount = vertexCount;
              lastEdgeCount = edgeCount;
            },
            2000,
            2000);

    final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(10000);

    final Thread[] threads = new Thread[parallel];
    for (int i = 0; i < parallel; ++i) {
      threads[i] =
          new Thread() {
            @Override
            public void run() {
              final OrientGraph localGraph = new OrientGraph(dbUrl, "admin", "admin", false);

              final ODatabaseDocumentInternal db = localGraph.getRawGraph();
              final OIndex userIndex =
                  db.getMetadata().getIndexManagerInternal().getIndex(db, "User.uid");
              final OIndex productIndex =
                  db.getMetadata().getIndexManagerInternal().getIndex(db, "Product.uid");

              localGraph.begin();
              for (int i = 0; ; ++i) {
                final String line;

                try {
                  line = queue.take();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  return;
                }

                final String[] parts = line.split(",");

                if (parts.length != 4) {
                  // SKIP IT
                  System.out.print("Skipped invalid line " + i + ": " + line);
                  continue;
                }

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put("score", new Float(parts[2]).intValue());
                properties.put("date", Long.parseLong(parts[3]));

                for (int localRetry = 0; localRetry < 100; ++localRetry) {
                  try {
                    final OrientVertex v1;
                    try (Stream<ORID> stream = userIndex.getInternal().getRids(parts[0])) {
                      v1 =
                          stream
                              .findAny()
                              .map(localGraph::getVertex)
                              .orElseGet(() -> localGraph.addVertex("class:User", "uid", parts[0]));
                    }

                    final OrientVertex v2;
                    try (Stream<ORID> stream = productIndex.getInternal().getRids(parts[1])) {
                      v2 =
                          stream
                              .findAny()
                              .map(localGraph::getVertex)
                              .orElseGet(
                                  () -> localGraph.addVertex("class:Product", "uid", parts[1]));
                    }

                    final OrientEdge edge = localGraph.addEdge(null, v1, v2, "Reviewed");
                    edge.setProperties(properties);

                    if (i % 2 == 0) {
                      localGraph.commit();
                      localGraph.begin();
                    }

                    break;

                  } catch (ONeedRetryException e) {
                    // RETRY
                    retry.incrementAndGet();
                  } catch (ORecordDuplicatedException e) {
                    // RETRY
                    retry.incrementAndGet();
                  }
                }
              }
            }
          };
    }

    for (int i = 0; i < parallel; ++i) threads[i].start();

    try {
      for (String line; (line = br.readLine()) != null; ) {
        row++;
        queue.put(line);
      }
    } finally {
      br.close();
    }

    for (int i = 0; i < parallel; ++i) threads[i].join();

    graph.shutdown();
    roGraph.shutdown();
  }
}
