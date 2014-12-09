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
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.index.ODefaultIndexFactory;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexMultiValues;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OLuceneFullTextIndex extends OIndexMultiValues implements OLuceneIndex {

  public OLuceneFullTextIndex(String typeId, String algorithm, OLuceneIndexEngine indexEngine, String valueContainerAlgorithm,
      ODocument metadata) {
    super(typeId, algorithm, indexEngine, valueContainerAlgorithm, metadata);
    indexEngine.setIndexMetadata(metadata);
  }

  @Override
  public OIndexMultiValues create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
    engine.setManagedIndex(this);
    return super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  @Override
  public OIndexMultiValues put(Object key, OIdentifiable iSingleValue) {
    if (key == null)
      return this;

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();

      try {
        Set<OIdentifiable> refs = new HashSet<OIdentifiable>();

        // ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
        refs.add(iSingleValue);

        // SAVE THE INDEX ENTRY
        indexEngine.put(key, refs);

      } finally {
        releaseExclusiveLock();
      }
      return this;
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public Set<OIdentifiable> get(Object key, OCommandContext context) {
    checkForRebuild();

    key = getCollatingValue(key);

    acquireSharedLock();
    try {

      final Set<OIdentifiable> values = indexEngine.get(key);

      if (values == null)
        return Collections.emptySet();

      return values;

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Set<OIdentifiable> get(Object key) {
    checkForRebuild();

    acquireSharedLock();
    try {

      final Set<OIdentifiable> values = indexEngine.get(key);

      if (values == null)
        return Collections.emptySet();

      return values;

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();

    try {

      acquireExclusiveLock();
      try {

        if (indexEngine instanceof OLuceneIndexEngine) {
          return ((OLuceneIndexEngine) indexEngine).remove(key, value);
        }
      } finally {
        releaseExclusiveLock();
      }

    } finally {
      modificationLock.releaseModificationLock();
    }
    return false;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return false;
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
    protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
        super.removeFromSnapshot(key, value, snapshot);
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
