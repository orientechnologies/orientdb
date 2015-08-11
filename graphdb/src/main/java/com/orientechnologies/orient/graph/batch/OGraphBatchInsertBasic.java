package com.orientechnologies.orient.graph.batch;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * this is and API for fast batch import of simple graphs, starting from an empty (or non existing) DB. This class allows import of
 * graphs with
 * <ul>
 * <li>no properties on edges</li>
 * <li>no properties on vertices</li>
 * <li>Long values for vertex ids</li>
 * </ul>
 * These limitations are intended to have best performance on a very simple use case. If there limitations don't fit your
 * requirements you can rely on other implementations (at the time of writing this they are planned, but not implemented yet)
 *
 * Typical usage: <code>
 *   OGraphBatchInsertSimple batch = new OGraphBatchInsertSimple("plocal:your/db", "admin", "admin");
 *   batch.begin();
 *   batch.createEdge(0L, 1L);
 *   batch.createEdge(0L, 2L);
 *   ...
 *   batch.end();
 * </code>
 *
 * There is no need to create vertices before connecting them: <code>
 *   batch.createVertex(0L);
 *   batch.createVertex(1L);
 *   batch.createEdge(0L, 1L);
 * </code>
 *
 * is equivalent to (but less performing than) <code>
 *   batch.createEdge(0L, 1L);
 * </code>
 *
 * batch.createVertex() is needed only if you want to create unconnected vertices.
 *
 * @since 2.0 M3
 * @author Luigi Dell'Aquila (l.dellaquila-at-orientechnologies.com)
 */
public class OGraphBatchInsertBasic {

  private final String        userName;
  private final String        dbUrl;
  private final String        password;
  Map<Long, List<Long>>       out                      = new HashMap<Long, List<Long>>();
  Map<Long, List<Long>>       in                       = new HashMap<Long, List<Long>>();
  private String              idPropertyName           = "uid";
  private String              edgeClass                = OrientEdgeType.CLASS_NAME;
  private String              vertexClass              = OrientVertexType.CLASS_NAME;
  private ODatabaseDocumentTx db;
  private int                 averageEdgeNumberPerNode = -1;
  private int                 estimatedEntries         = -1;
  private int                 bonsaiThreshold          = 1000;
  private int[]               clusterIds;
  private long[]              lastClusterPositions;
  private long                last                     = 0;
  private boolean             walActive;

  private int                 parallel                 = 4;
  private AtomicInteger       runningThreads;

  class BatchImporterJob extends Thread {

    private final int mod;
    OClass            vClass;

    BatchImporterJob(int mod, OClass vertexClass) {
      this.mod = mod;
      this.vClass = vertexClass;
    }

    @Override
    public void run() {
      try {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
        db.open(userName, password);
        db.declareIntent(new OIntentMassiveInsert());
        int clusterId = clusterIds[mod];

        final String outField = OrientEdgeType.CLASS_NAME.equals(edgeClass) ? "out_" : ("out_" + edgeClass);
        final String inField = OrientEdgeType.CLASS_NAME.equals(edgeClass) ? "in_" : ("in_" + edgeClass);

        String clusterName = db.getStorage().getClusterById(clusterId).getName();
        // long firstAvailableClusterPosition = lastClusterPositions[mod] + 1;

        for (long i = mod; i <= last; i += parallel) {
          final List<Long> outIds = out.get(i);
          final List<Long> inIds = in.get(i);
          final ODocument doc = new ODocument(vClass);
          if (outIds == null && inIds == null) {
            db.save(doc, clusterName).delete();
          } else {
            doc.field(idPropertyName, i);
            if (outIds != null) {
              final ORidBag outBag = new ORidBag();
              for (Long l : outIds) {
                final ORecordId rid = new ORecordId(getClusterId(l), getClusterPosition(l));
                outBag.add(rid);
              }
              doc.field(outField, outBag);
            }
            if (inIds != null) {
              final ORidBag inBag = new ORidBag();
              for (Long l : inIds) {
                final ORecordId rid = new ORecordId(getClusterId(l), getClusterPosition(l));
                inBag.add(rid);
              }
              doc.field(inField, inBag);
            }
            db.save(doc, clusterName);
          }
        }
      } finally {
        runningThreads.decrementAndGet();
        synchronized (runningThreads) {
          runningThreads.notifyAll();
        }
        db.activateOnCurrentThread();
        db.declareIntent(null);
        db.close();
      }
    }
  }

  /**
   * Creates a new batch insert procedure by using admin user. It's intended to be used only for a single batch cycle (begin,
   * create..., end)
   *
   * @param iDbURL
   *          db connection URL (plocal:/your/db/path)
   */
  public OGraphBatchInsertBasic(String iDbURL) {
    this.dbUrl = iDbURL;
    this.userName = "admin";
    this.password = "admin";
  }

  /**
   * Creates a new batch insert procedure. It's intended to be used only for a single batch cycle (begin, create..., end)
   *
   * @param iDbURL
   *          db connection URL (plocal:/your/db/path)
   * @param iUserName
   *          db user name (use admin for new db)
   * @param iPassword
   *          db password (use admin for new db)
   */
  public OGraphBatchInsertBasic(String iDbURL, String iUserName, String iPassword) {
    this.dbUrl = iDbURL;
    this.userName = iUserName;
    this.password = iPassword;
  }

  /**
   * Creates the database (if it does not exist) and initializes batch operations. Call this once, before starting to create
   * vertices and edges.
   *
   */
  public void begin() {
    walActive = OGlobalConfiguration.USE_WAL.getValueAsBoolean();
    if (walActive)
      OGlobalConfiguration.USE_WAL.setValue(false);
    if (averageEdgeNumberPerNode > 0) {
      OGlobalConfiguration.RID_BAG_EMBEDDED_DEFAULT_SIZE.setValue(averageEdgeNumberPerNode);
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(bonsaiThreshold);
    }

    db = new ODatabaseDocumentTx(dbUrl);
    if (db.exists()) {
      db.open(userName, password);
    } else {
      db.create();
    }
    createBaseSchema();

    out = estimatedEntries > 0 ? new HashMap<Long, List<Long>>(estimatedEntries) : new HashMap<Long, List<Long>>();
    in = estimatedEntries > 0 ? new HashMap<Long, List<Long>>(estimatedEntries) : new HashMap<Long, List<Long>>();

    OClass vClass = db.getMetadata().getSchema().getClass(this.vertexClass);
    int[] existingClusters = vClass.getClusterIds();
    for (int c = existingClusters.length; c <= parallel; c++) {
      vClass.addCluster(vClass.getName() + "_" + c);
    }

    clusterIds = vClass.getClusterIds();

    lastClusterPositions = new long[clusterIds.length];
    for (int i = 0; i < clusterIds.length; i++) {
      int clusterId = clusterIds[i];
      try {
        lastClusterPositions[i] = db.getStorage().getClusterById(clusterId).getLastPosition();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Flushes data to db and closes the db. Call this once, after vertices and edges creation.
   */
  public void end() {
    final OClass vClass = db.getMetadata().getSchema().getClass(vertexClass);

    try {

      runningThreads = new AtomicInteger(parallel);
      for (int i = 0; i < parallel - 1; i++) {
        Thread t = new BatchImporterJob(i, vClass);
        t.start();
      }
      Thread t = new BatchImporterJob(parallel - 1, vClass);
      t.run();

      if (runningThreads.get() > 0) {
        synchronized (runningThreads) {
          while (runningThreads.get() > 0) {
            try {
              runningThreads.wait();
            } catch (InterruptedException e) {
            }
          }
        }
      }

    } finally {
      db.activateOnCurrentThread();
      db.declareIntent(null);
      db.close();
      if (walActive)
        OGlobalConfiguration.USE_WAL.setValue(true);
    }
  }

  /**
   * Creates a new vertex
   * 
   * @param v
   *          the vertex ID
   */
  public void createVertex(final Long v) {
    last = last < v ? v : last;
    final List<Long> outList = out.get(v);
    if (outList == null) {
      out.put(v, new ArrayList<Long>(averageEdgeNumberPerNode <= 0 ? 4 : averageEdgeNumberPerNode));
    }
  }

  /**
   * Creates a new edge between two vertices. If vertices do not exist, they will be created
   * 
   * @param from
   *          id of the vertex that is starting point of the edge
   * @param to
   *          id of the vertex that is end point of the edge
   */
  public void createEdge(final Long from, final Long to) {
    if (from < 0) {
      throw new IllegalArgumentException(" Invalid vertex id: " + from);
    }
    if (to < 0) {
      throw new IllegalArgumentException(" Invalid vertex id: " + to);
    }
    last = last < from ? from : last;
    last = last < to ? to : last;
    putInList(from, out, to);
    putInList(to, in, from);
  }

  /**
   *
   * @return the configured average number of edges per node (optimization parameter, not calculated)
   */
  public int getAverageEdgeNumberPerNode() {
    return averageEdgeNumberPerNode;
  }

  /**
   * configures the average number of edges per node (for optimization). Use it before calling begin()
   * 
   * @param averageEdgeNumberPerNode
   */
  public void setAverageEdgeNumberPerNode(final int averageEdgeNumberPerNode) {
    this.averageEdgeNumberPerNode = averageEdgeNumberPerNode;
  }

  /**
   * @return the property name where ids are written on vertices
   */
  public String getIdPropertyName() {
    return idPropertyName;
  }

  /**
   * @param idPropertyName
   *          the property name where ids are written on vertices
   */
  public void setIdPropertyName(final String idPropertyName) {
    this.idPropertyName = idPropertyName;
  }

  /**
   *
   * @return the edge class name (E by default)
   */
  public String getEdgeClass() {
    return edgeClass;
  }

  /**
   *
   * @param edgeClass
   *          the edge class name
   */
  public void setEdgeClass(final String edgeClass) {
    this.edgeClass = edgeClass;
  }

  /**
   *
   * @return the vertex class name (V by default)
   */
  public String getVertexClass() {
    return vertexClass;
  }

  /**
   *
   * @param vertexClass
   *          the vertex class name
   */
  public void setVertexClass(final String vertexClass) {
    this.vertexClass = vertexClass;
  }

  /**
   * @return the threshold for passing from emdedded RidBag to SBTreeBonsai (low level optimization).
   */
  public int getBonsaiThreshold() {
    return bonsaiThreshold;
  }

  /**
   * Sets the threshold for passing from emdedded RidBag to SBTreeBonsai implementation, in number of edges (low level
   * optimization). High values speed up writes but slow down reads later. Set -1 (default) to use default database configuration.
   *
   * See OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD}
   *
   */
  public void setBonsaiThreshold(final int bonsaiThreshold) {
    this.bonsaiThreshold = bonsaiThreshold;
  }

  /**
   * Returns the estimated number of entries. 0 for auto-resize.
   */
  public int getEstimatedEntries() {
    return estimatedEntries;
  }

  /**
   * Sets the estimated number of entries, 0 for auto-resize (default). This pre-allocate in memory structure avoiding resizing of
   * them at run-time.
   * 
   */
  public void setEstimatedEntries(final int estimatedEntries) {
    this.estimatedEntries = estimatedEntries;
  }

  /**
   *
   * @return number of parallel threads used for batch import
   */
  public int getParallel() {
    return parallel;
  }

  /**
   * sets the number of parallel threads to be used for batch insert
   * 
   * @param parallel
   *          number of threads (default 4)
   */
  public void setParallel(int parallel) {
    this.parallel = parallel;
  }

  private void putInList(final Long key, final Map<Long, List<Long>> out, final Long value) {
    List<Long> list = out.get(key);
    if (list == null) {
      list = new ArrayList<Long>(averageEdgeNumberPerNode <= 0 ? 4 : averageEdgeNumberPerNode);
      out.put(key, list);
    }
    list.add(value);
  }

  private void createBaseSchema() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass v;
    OClass e;
    if (!schema.existsClass(OrientVertexType.CLASS_NAME)) {
      v = schema.createClass(OrientVertexType.CLASS_NAME);
    } else {
      v = schema.getClass(OrientVertexType.CLASS_NAME);
    }
    if (!schema.existsClass(OrientEdgeType.CLASS_NAME)) {
      e = schema.createClass(OrientEdgeType.CLASS_NAME);
    } else {
      e = schema.getClass(OrientEdgeType.CLASS_NAME);
    }
    if (!OrientVertexType.CLASS_NAME.equals(this.vertexClass)) {
      if (!schema.existsClass(this.vertexClass)) {
        schema.createClass(this.vertexClass, v);
      }
    }
    if (!schema.existsClass(this.edgeClass)) {
      if (!schema.existsClass(this.edgeClass)) {
        schema.createClass(this.edgeClass, e);
      }
    }
  }

  private long getClusterPosition(final long uid) {
    return lastClusterPositions[(int) (uid % parallel)] + (uid / parallel) + 1;
  }

  private int getClusterId(final long left) {
    return clusterIds[(int) (left % parallel)];
  }

}
