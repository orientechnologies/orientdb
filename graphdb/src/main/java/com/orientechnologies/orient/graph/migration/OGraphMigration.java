package com.orientechnologies.orient.graph.migration;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.tinkerpop.blueprints.impls.orient.OrientIndex;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * In OrientDb 1.7-rc1 was introduced RidBag a new data structure for link management. Since OrientDB 1.7 RidBag is default data
 * structure for management relationships in graph. Use this class to upgrade graphs created under older versions of OrientDB.
 * 
 * Structure of manual indexes was changed too. This class is used to upgrade structure of manual indexes.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OGraphMigration {
  private final ODatabaseDocument database;

  public OGraphMigration(ODatabaseDocument database) {
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

    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    boolean indexWasMigrated = false;
    for (OIndex index : indexManager.getIndexes()) {
      final ODocument config = index.getConfiguration();

      ODocument metadata = index.getMetadata();

      if (config.field(OrientIndex.CONFIG_CLASSNAME) != null && metadata == null) {
        final OIndex<OIdentifiable> recordKeyValueIndex = (OIndex<OIdentifiable>) database
            .getMetadata()
            .getIndexManager()
            .createIndex("__@recordmap@___" + index.getName(), OClass.INDEX_TYPE.DICTIONARY.toString(),
                new OSimpleKeyIndexDefinition(OType.LINK, OType.STRING), null, null, null);

        final Iterator<Map.Entry<Object, Collection<OIdentifiable>>> iterator = index.iterator();
        while (iterator.hasNext()) {
          final Map.Entry<Object, Collection<OIdentifiable>> entry = iterator.next();
          final String keyTemp = entry.getKey().toString();

          for (OIdentifiable identifiable : entry.getValue())
            recordKeyValueIndex.put(new OCompositeKey(identifiable.getIdentity(), keyTemp), identifiable.getIdentity());
        }

        metadata = new ODocument();
        metadata.field(OrientIndex.CONFIG_CLASSNAME, config.field(OrientIndex.CONFIG_CLASSNAME));
        metadata.field(OrientIndex.CONFIG_RECORD_MAP_NAME, recordKeyValueIndex.getName());

        config.field("metadata", metadata);

        indexWasMigrated = true;
      }
    }

    if (indexWasMigrated)
      indexManager.save();
  }
}
