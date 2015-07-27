package com.orientechnologies.orient.graph.batch;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
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
 * this is and API for fast batch import of graphs with only one class for edges and one class for vertices, starting from an empty
 * (or non existing) DB. This class allows import of graphs with
 * <ul>
 * <li>properties on edges</li>
 * <li>properties on vertices</li>
 * <li>Long values for vertex ids</li>
 * </ul>
 *
 * This batch insert procedure is made of four phases, that have to be executed in the correct order:
 * <ul>
 * <li>begin(): initializes the database</li>
 * <li>create edges (with or without properties) and vertices</li>
 * <li>set properties on vertices</li>
 * <li>end(): flushes data to db</li>
 * </ul>
 *
 * Typical usage: <code>
 *   OGraphBatchInsert batch = new OGraphBatchInsert("plocal:your/db", "admin", "admin");
 * 
 *   //phase 1: begin
 *   batch.begin();
 * 
 *   //phase 2: create edges
 *   Map&lt;String, Object&gt; edgeProps = new HashMap&lt;String, Object&gt;
 *   edgeProps.put("foo", "bar");
 *   batch.createEdge(0L, 1L, edgeProps);
 *   batch.createVertex(2L);
 *   batch.createEdge(3L, 4L, null);
 *   ...
 * 
 *   //phase 3: set properties on vertices, THIS CAN BE DONE ONLY AFTER EDGE AND VERTEX CREATION
 *   Map&lt;String, Object&gt; vertexProps = new HashMap&lt;String, Object&gt;
 *   vertexProps.put("foo", "bar");
 *   batch.setVertexProperties(0L, vertexProps)
 *   ...
 * 
 *   //phase 4: end
 *   batch.end();
 * </code>
 *
 * There is no need to create vertices before connecting them: <code>
 *   batch.createVertex(0L);
 *   batch.createVertex(1L);
 *   batch.createEdge(0L, 1L, props);
 * </code>
 *
 * is equivalent to (but less performing than) <code>
 *   batch.createEdge(0L, 1L, props);
 * </code>
 *
 * batch.createVertex(Long) is needed only if you want to create unconnected vertices
 *
 * @since 2.0 M3
 * @author Luigi Dell'Aquila (l.dellaquila-at-orientechnologies.com)
 */
public class OGraphBatchInsert {

  private final String        userName;
  private final String        dbUrl;
  private final String        password;
  Map<Long, List<Object>>     out                      = new HashMap<Long, List<Object>>();
  Map<Long, List<Object>>     in                       = new HashMap<Long, List<Object>>();
  private String              idPropertyName           = "uid";
  private String              edgeClass                = OrientEdgeType.CLASS_NAME;
  private String              vertexClass              = OrientVertexType.CLASS_NAME;
  private OClass              oVertexClass;
  private ODatabaseDocumentTx db;
  private int                 averageEdgeNumberPerNode = -1;
  private int                 estimatedEntries         = -1;
  private int                 bonsaiThreshold          = 1000;
  private int[]               clusterIds;
  private long[]              lastClusterPositions;
  private long[]              nextVerticesToCreate;                                        // absolute value
  private long                last                     = 0;
  private boolean             walActive;

  private int                 parallel                 = 4;
  private final AtomicInteger runningThreads           = new AtomicInteger(0);

  boolean                     settingProperties        = false;
  private Boolean             useLightWeigthEdges      = null;

  class BatchImporterJob extends Thread {

    private final int mod;
    private OClass    vClass;
    private long      last;

    BatchImporterJob(int mod, OClass vertexClass, long last) {
      this.mod = mod;
      this.vClass = vertexClass;
      this.last = last;
    }

    @Override
    public void run() {
      try {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
        db.open(userName, password);
        run(db);
      } finally {
        runningThreads.decrementAndGet();
        synchronized (runningThreads) {
          runningThreads.notifyAll();
        }
        db.activateOnCurrentThread();
        db.close();
      }
    }

    private void run(ODatabaseDocumentTx db) {
      db.declareIntent(new OIntentMassiveInsert());
      int clusterId = clusterIds[mod];

      final String outField = OrientEdgeType.CLASS_NAME.equals(edgeClass) ? "out_" : ("out_" + edgeClass);
      final String inField = OrientEdgeType.CLASS_NAME.equals(edgeClass) ? "in_" : ("in_" + edgeClass);

      String clusterName = db.getStorage().getClusterById(clusterId).getName();
      // long firstAvailableClusterPosition = lastClusterPositions[mod] + 1;

      for (long i = nextVerticesToCreate[mod]; i <= last; i += parallel) {
        createVertex(db, i, inField, outField, clusterName, null);
      }
      db.declareIntent(null);
    }

    public void createVertex(ODatabaseDocumentTx db, long i, Map<String, Object> properties) {
      int clusterId = clusterIds[mod];

      final String outField = OrientEdgeType.CLASS_NAME.equals(edgeClass) ? "out_" : ("out_" + edgeClass);
      final String inField = OrientEdgeType.CLASS_NAME.equals(edgeClass) ? "in_" : ("in_" + edgeClass);
      String clusterName = db.getStorage().getClusterById(clusterId).getName();

      createVertex(db, i, inField, outField, clusterName, properties);
    }

    private void createVertex(ODatabaseDocumentTx db, long i, String inField, String outField, String clusterName,
        Map<String, Object> properties) {
      final List<Object> outIds = out.get(i);
      final List<Object> inIds = in.get(i);
      final ODocument doc = new ODocument(vClass);
      if (outIds == null && inIds == null) {
        db.save(doc, clusterName).delete();
      } else {
        doc.field(idPropertyName, i);
        if (outIds != null) {
          final ORidBag outBag = new ORidBag();
          for (Object l : outIds) {

            ORecordId rid;
            if (l instanceof ORecordId) {
              rid = (ORecordId) l;
            } else {
              rid = new ORecordId(getClusterId((Long) l), getClusterPosition((Long) l));
            }
            outBag.add(rid);
          }
          doc.field(outField, outBag);
        }
        if (inIds != null) {
          final ORidBag inBag = new ORidBag();
          for (Object l : inIds) {
            ORecordId rid;
            if (l instanceof ORecordId) {
              rid = (ORecordId) l;
            } else {
              rid = new ORecordId(getClusterId((Long) l), getClusterPosition((Long) l));
            }
            inBag.add(rid);
          }
          doc.field(inField, inBag);
        }

        doc.fromMap(properties);
        db.save(doc, clusterName);
      }
      nextVerticesToCreate[mod] += parallel;
    }
  }

  /**
   * Creates a new batch insert procedure by using admin user. It's intended to be used only for a single batch cycle (begin,
   * create..., end)
   *
   * @param iDbURL
   *          db connection URL (plocal:/your/db/path)
   */
  public OGraphBatchInsert(String iDbURL) {
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
  public OGraphBatchInsert(String iDbURL, String iUserName, String iPassword) {
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
    if (this.useLightWeigthEdges == null) {
      final List<OStorageEntryConfiguration> custom = (List<OStorageEntryConfiguration>) db.get(ODatabase.ATTRIBUTES.CUSTOM);
      for (OStorageEntryConfiguration c : custom) {
        if (c.name.equals("useLightweightEdges")) {
          this.useLightWeigthEdges = Boolean.parseBoolean(c.value);
          break;
        }
      }
      if (this.useLightWeigthEdges == null) {
        this.useLightWeigthEdges = true;
      }
    }
    createBaseSchema();

    out = estimatedEntries > 0 ? new HashMap<Long, List<Object>>(estimatedEntries) : new HashMap<Long, List<Object>>();
    in = estimatedEntries > 0 ? new HashMap<Long, List<Object>>(estimatedEntries) : new HashMap<Long, List<Object>>();

    OClass vClass = db.getMetadata().getSchema().getClass(this.vertexClass);
    int[] existingClusters = vClass.getClusterIds();
    for (int c = existingClusters.length; c <= parallel; c++) {
      vClass.addCluster(vClass.getName() + "_" + c);
    }

    clusterIds = vClass.getClusterIds();

    lastClusterPositions = new long[clusterIds.length];
    nextVerticesToCreate = new long[clusterIds.length];
    for (int i = 0; i < clusterIds.length; i++) {
      int clusterId = clusterIds[i];
      try {
        nextVerticesToCreate[i] = i;
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
      runningThreads.set(parallel);
      for (int i = 0; i < parallel - 1; i++) {
        Thread t = new BatchImporterJob(i, vClass, last);
        t.start();
      }
      Thread t = new BatchImporterJob(parallel - 1, vClass, last);
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
    if (settingProperties) {
      throw new IllegalStateException("Cannot create new edges when already set properties on vertices");
    }

    last = last < v ? v : last;
    final List<Object> outList = out.get(v);
    if (outList == null) {
      out.put(v, new ArrayList<Object>(averageEdgeNumberPerNode <= 0 ? 4 : averageEdgeNumberPerNode));
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
  public void createEdge(final Long from, final Long to, Map<String, Object> properties) {
    if (settingProperties) {
      throw new IllegalStateException("Cannot create new edges when already set properties on vertices");
    }
    if (from < 0) {
      throw new IllegalArgumentException(" Invalid vertex id: " + from);
    }
    if (to < 0) {
      throw new IllegalArgumentException(" Invalid vertex id: " + to);
    }
    if (useLightWeigthEdges && (properties == null || properties.size() == 0)) {
      last = last < from ? from : last;
      last = last < to ? to : last;
      putInList(from, out, to);
      putInList(to, in, from);
    } else {
      ODocument edgeDoc = new ODocument(edgeClass);

      edgeDoc.fromMap(properties);
      edgeDoc.field("out", new ORecordId(getClusterId(from), getClusterPosition(from)));
      edgeDoc.field("in", new ORecordId(getClusterId(to), getClusterPosition(to)));
      db.save(edgeDoc);
      ORecordId rid = (ORecordId) edgeDoc.getIdentity();
      putInList(from, out, rid);
      putInList(to, in, rid);
    }
  }

  public void setVertexProperties(Long id, Map<String, Object> properties) {
    if (properties == null || properties.size() == 0) {
      return;
    }
    settingProperties = true;
    final int cluster = (int) (id % parallel);
    if (nextVerticesToCreate[cluster] <= id) {
      if (oVertexClass == null) {
        oVertexClass = db.getMetadata().getSchema().getClass(vertexClass);
      }
      if (nextVerticesToCreate[cluster] < id) {
        new BatchImporterJob(cluster, oVertexClass, id - 1).run(db);
      }
      new BatchImporterJob(cluster, oVertexClass, id).createVertex(db, id, properties);
    } else {
      final ODocument doc = db.load(new ORecordId(getClusterId(id), getClusterPosition(id)));
      if (doc == null) {
        throw new RuntimeException("trying to insert properties on non existing document");
      }
      doc.fromMap(properties);
      db.save(doc);
    }
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

  private void putInList(final Long key, final Map<Long, List<Object>> out, final Object value) {
    List<Object> list = out.get(key);
    if (list == null) {
      list = new ArrayList<Object>(averageEdgeNumberPerNode <= 0 ? 4 : averageEdgeNumberPerNode);
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
