/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.index.ODefaultIndexFactory;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import java.util.Map;
import java.util.Set;

public class OLuceneSpatialIndex extends OLuceneIndexNotUnique {

  public OLuceneSpatialIndex(String typeId, String algorithm, OLuceneIndexEngine engine, String valueContainerAlgorithm,
      ODocument metadata) {
    super(typeId, algorithm, engine, valueContainerAlgorithm, metadata);
  }

  @Override
  protected void populateIndex(ODocument doc, Object fieldValue) {
    put(fieldValue, doc);
  }

  @Override
  protected Object getCollatingValue(Object key) {
    return key;
  }

  @Override
  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    Object snapshotValue = snapshot.get(key);

    Set<OIdentifiable> values;
    if (snapshotValue == null)
      values = null;
    else if (snapshotValue.equals(RemovedValue.INSTANCE))
      values = null;
    else
      values = (Set<OIdentifiable>) snapshotValue;

    if (values == null) {
      if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
        values = new OIndexRIDContainer(getName(), true);
      } else {
        values = new OMVRBTreeRIDSet(OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger());
        ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
      }

      snapshot.put(key, values);
    }

    values.add(value.getIdentity());
    snapshot.put(key, values);
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {
    long size = 0;
    OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
    try {
      engine.setRebuilding(true);
      super.rebuild(iProgressListener);
    } finally {
      engine.setRebuilding(false);

    }
    engine.flush();
    return ((OLuceneIndexEngine) indexEngine).size(null);
  }
}
