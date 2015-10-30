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
import com.orientechnologies.lucene.LuceneTxOperations;
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexMultiValues;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OLuceneIndexNotUnique extends OIndexNotUnique implements OLuceneIndex {

  public OLuceneIndexNotUnique(String name, String typeId, String algorithm, OLuceneIndexEngine engine,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, engine, valueContainerAlgorithm, metadata);

    engine.setIndexMetadata(metadata);
  }

  @Override
  public OIndexMultiValues create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
    engine.setManagedIndex(this);
    engine.setIndexName(name);
    return super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  @Override
  public OIndexMultiValues put(Object key, OIdentifiable iSingleValue) {
    checkForRebuild();

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        checkForKeyType(key);
        Set<OIdentifiable> values = new HashSet<OIdentifiable>();
        values.add(iSingleValue);
        indexEngine.put(key, values);
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  protected void commitSnapshot(Map<Object, Object> snapshot) {
    for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
      Object key = snapshotEntry.getKey();
      LuceneTxOperations operations = (LuceneTxOperations) snapshotEntry.getValue();
      checkForKeyType(key);

      for (OIdentifiable oIdentifiable : operations.removed) {
        ((OLuceneIndexEngine) indexEngine).remove(key, oIdentifiable);
      }

    }
    for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
      Object key = snapshotEntry.getKey();
      LuceneTxOperations operations = (LuceneTxOperations) snapshotEntry.getValue();
      checkForKeyType(key);

      indexEngine.put(key, operations.added);

    }

  }

  @Override
  public Set<OIdentifiable> get(Object key) {
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
  protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    LuceneTxOperations operations = (LuceneTxOperations) snapshot.get(key);
    if (operations == null) {
      operations = new LuceneTxOperations();
      snapshot.put(key, operations);
    }
    operations.removed.add(value.getIdentity());
    snapshot.put(key, operations);
  }

  @Override
  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    LuceneTxOperations operations = (LuceneTxOperations) snapshot.get(key);

    if (operations == null) {
      operations = new LuceneTxOperations();
      snapshot.put(key, operations);
    }
    operations.added.add(value.getIdentity());
    snapshot.put(key, operations);
  }

  @Override
  protected void clearSnapshot(IndexTxSnapshot indexTxSnapshot) {
    indexTxSnapshot.clear = true;
    indexTxSnapshot.indexSnapshot.clear();
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    checkForRebuild();

    key = getCollatingValue(key);
    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {

        if (indexEngine instanceof OLuceneIndexEngine) {
          return ((OLuceneIndexEngine) indexEngine).remove(key, value);
        } else {
          return false;
        }

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {

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

  @Override
  public Object getCollatingValue(Object key) {
    return key;
  }

  @Override
  public IndexSearcher searcher() throws IOException {
    return ((OLuceneIndexEngine) indexEngine).searcher();


  }


  protected OLuceneIndexEngine getIndexEngine(){
    return (OLuceneIndexEngine) indexEngine;
  }
  @Override
  public boolean canBeUsedInEqualityOperators() {
    return false;
  }
}


