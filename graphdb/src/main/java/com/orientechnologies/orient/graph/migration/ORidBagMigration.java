package com.orientechnologies.orient.graph.migration;

import java.util.List;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * In OrientDb 1.7-rc1 was introduced RidBag a new data structure for link management. Since OrientDB 1.7 RidBag is default data
 * structure for management relationships in graph. Use this class to upgrade graphs created under older versions of OrientDB.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class ORidBagMigration {
  private final ODatabaseDocument database;

  public ORidBagMigration(ODatabaseDocument database) {
    this.database = database;
  }

  public void execute() {
    List<ODocument> vertexes = database.query(new OSQLSynchQuery<ODocument>("select from V"));

    for (ODocument vertex : vertexes) {
      for (String fieldName : vertex.fieldNames()) {
        if (fieldName.startsWith(OrientVertex.CONNECTION_IN_PREFIX) || fieldName.startsWith(OrientVertex.CONNECTION_OUT_PREFIX)) {
          Object oldValue = vertex.field(fieldName);
          if (oldValue instanceof OMVRBTreeRIDSet) {
            OMVRBTreeRIDSet oldTree = (OMVRBTreeRIDSet) oldValue;
            ORidBag bag = new ORidBag();
            bag.addAll(oldTree);

            vertex.field(fieldName, bag);
          }
        }
      }
      vertex.save();
    }
  }
}
