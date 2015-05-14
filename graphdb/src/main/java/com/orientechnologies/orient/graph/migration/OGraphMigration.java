/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.graph.migration;

import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
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
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OGraphMigration {
  private final ODatabaseDocument      database;
  private final OCommandOutputListener commandOutputListener;

  public OGraphMigration(ODatabaseDocument database, OCommandOutputListener commandOutputListener) {
    this.database = database;
    this.commandOutputListener = commandOutputListener;
  }

  public void execute() {
    List<ODocument> vertexes = database.query(new OSQLSynchQuery<ODocument>("select from V"));

    if (commandOutputListener != null)
      commandOutputListener.onMessage(vertexes.size() + " vertexes were fetched to process.");
    for (int i = 0; i < vertexes.size(); i++) {
      final ODocument vertex = vertexes.get(i);

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

      if (commandOutputListener != null && i % 10000 == 0)
        commandOutputListener.onMessage(i + " vertexes were processed.");
    }

    if (commandOutputListener != null)
      commandOutputListener.onMessage("All vertexes were processed, looking for manual indexes to update.");

    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    boolean indexWasMigrated = false;
    for (OIndex index : indexManager.getIndexes()) {
      final ODocument config = index.getConfiguration();

      ODocument metadata = index.getMetadata();

      if (config.field(OrientIndex.CONFIG_CLASSNAME) != null && metadata == null) {
        if (commandOutputListener != null)
          commandOutputListener.onMessage("Index " + index.getName() + " uses out of dated index format and will be updated.");

        OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.DICTIONARY.toString(), null);

        final OIndex<OIdentifiable> recordKeyValueIndex = (OIndex<OIdentifiable>) database
            .getMetadata()
            .getIndexManager()
            .createIndex("__@recordmap@___" + index.getName(), OClass.INDEX_TYPE.DICTIONARY.toString(),
                new OSimpleKeyIndexDefinition(factory.getLastVersion(), OType.LINK, OType.STRING), null, null, null);

        OIndexCursor cursor = index.cursor();
        Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();

        while (entry != null) {
          final String keyTemp = entry.getKey().toString();
          final OIdentifiable identifiable = entry.getValue();
          recordKeyValueIndex.put(new OCompositeKey(identifiable.getIdentity(), keyTemp), identifiable.getIdentity());

          entry = cursor.nextEntry();
        }

        metadata = new ODocument();
        metadata.field(OrientIndex.CONFIG_CLASSNAME, config.field(OrientIndex.CONFIG_CLASSNAME));
        metadata.field(OrientIndex.CONFIG_RECORD_MAP_NAME, recordKeyValueIndex.getName());

        config.field("metadata", metadata);

        indexWasMigrated = true;

        if (commandOutputListener != null)
          commandOutputListener.onMessage("Index " + index.getName() + " structure was updated.");
      }
    }

    if (indexWasMigrated)
      indexManager.save();

    if (commandOutputListener != null)
      commandOutputListener.onMessage("Graph database update is completed");

  }
}
