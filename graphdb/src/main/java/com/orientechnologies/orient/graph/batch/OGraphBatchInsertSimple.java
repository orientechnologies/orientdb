package com.orientechnologies.orient.graph.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;

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
 */
public class OGraphBatchInsertSimple {

  private final String        userName;
  private final String        dbUrl;
  private final String        password;

  private String              idPropertyName           = "uid";
  private String              edgeClass                = "E";
  private String              vertexClass              = "V";

  private ODatabaseDocumentTx db;

  private int                 averageEdgeNumberPerNode = -1;
  private int                 bonsaiThreshold          = 1000;

  private int[]               clusterIds;
  private long[]              lastClusterIds;

  Map<Long, List<Long>>       out                      = new HashMap<Long, List<Long>>();
  Map<Long, List<Long>>       in                       = new HashMap<Long, List<Long>>();

  private long                last                     = 0;

  /**
   * creates a new batch insert procedure. It's intended to be used only for a single batch cycle (begin, create..., end)
   * 
   * @param iDbURL
   *          db connection URL (plocal:/your/db/path)
   * @param iUserName
   *          db user name (use admin for new db)
   * @param iPassword
   *          db password (use admin for new db)
   */
  public OGraphBatchInsertSimple(String iDbURL, String iUserName, String iPassword) {
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
  }

  /**
   * Flushes data to db and closes the db. Call this once, after vertices and edges creation.
   */
  public void end() {
    OClass vClass = db.getMetadata().getSchema().getClass(vertexClass);
    int clusterId = vClass.getDefaultClusterId();

    try {
      OCluster cluster = db.getStorage().getClusterById(clusterId);
      long firstAvailableClusterPosition = cluster.getLastPosition() + 1;
      String clusterName = cluster.getName();

      String outField = "E".equals(this.edgeClass) ? "out_" : ("out_" + this.edgeClass);
      String inField = "E".equals(this.edgeClass) ? "in_" : ("in_" + this.edgeClass);

      db.declareIntent(new OIntentMassiveInsert());

      for (long i = 0; i <= last; i++) {
        List<Long> outIds = this.out.get(i);
        List<Long> inIds = this.in.get(i);
        ODocument doc = new ODocument(vClass);
        if (outIds == null && inIds == null) {
          db.save(doc, clusterName).delete();
        } else {
          doc.field(idPropertyName, i);
          if (outIds != null) {
            ORidBag outBag = new ORidBag();
            for (Long l : outIds) {
              ORecordId rid = new ORecordId(clusterId, firstAvailableClusterPosition + l);
              outBag.add(rid);
            }
            doc.field(outField, outBag);
          }
          if (inIds != null) {
            ORidBag inBag = new ORidBag();
            for (Long l : inIds) {
              ORecordId rid = new ORecordId(clusterId, firstAvailableClusterPosition + l);
              inBag.add(rid);
            }
            doc.field(inField, inBag);
          }
          db.save(doc, clusterName);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      db.declareIntent(null);
      db.close();
    }
  }

  /**
   * creates a new vertex
   * 
   * @param v
   *          the vertex ID
   */
  public void createVertex(Long v) {
    last = last < v ? v : last;
    List<Long> outList = out.get(v);
    if (outList == null) {
      out.put(v, new ArrayList<Long>(averageEdgeNumberPerNode <= 0 ? 4 : averageEdgeNumberPerNode));
    }
  }

  /**
   * creates a new edge between two vertices. If vertices do not exist, they will be created
   * 
   * @param from
   *          id of the vertex that is starting point of the edge
   * @param to
   *          id of the vertex that is end point of the edge
   */
  public void createEdge(Long from, Long to) {
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

  private void putInList(Long key, Map<Long, List<Long>> out, Long value) {
    List<Long> list = out.get(key);
    if (list == null) {
      list = new ArrayList<Long>(averageEdgeNumberPerNode <= 0 ? 4 : averageEdgeNumberPerNode);
      out.put(key, list);
    }
    list.add(value);
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
  public void setAverageEdgeNumberPerNode(int averageEdgeNumberPerNode) {
    this.averageEdgeNumberPerNode = averageEdgeNumberPerNode;
  }

  private void createBaseSchema() {
    OSchema schema = db.getMetadata().getSchema();
    OClass v;
    OClass e;
    if (!schema.existsClass("V")) {
      v = schema.createClass("V");
    } else {
      v = schema.getClass("V");
    }
    if (!schema.existsClass("E")) {
      e = schema.createClass("E");
    } else {
      e = schema.getClass("E");
    }
    if (!"V".equals(this.vertexClass)) {
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

  private long getClusterPosition(long uid) {
    return lastClusterIds[(int) (uid % clusterIds.length)] + (uid / clusterIds.length) + 1;
  }

  private int getClusterId(long left) {
    return clusterIds[(int) (left % clusterIds.length)];
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
  public void setIdPropertyName(String idPropertyName) {
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
  public void setEdgeClass(String edgeClass) {
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
  public void setVertexClass(String vertexClass) {
    this.vertexClass = vertexClass;
  }

  /**
   * @return the threshold for passing from emdedded RidBag to SBTreeBonsai (low level optimization).
   */
  public int getBonsaiThreshold() {
    return bonsaiThreshold;
  }

  /**
   * sets the threshold for passing from emdedded RidBag to SBTreeBonsai implementation, in number of edges (low level
   * optimization). High values speed up writes but slow down reads later. Set -1 (default) to use default database configuration.
   *
   * See OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD}
   *
   */
  public void setBonsaiThreshold(int bonsaiThreshold) {
    this.bonsaiThreshold = bonsaiThreshold;
  }
}
