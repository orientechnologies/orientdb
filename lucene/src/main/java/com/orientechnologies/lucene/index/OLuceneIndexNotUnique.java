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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.LuceneTxOperations;
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OIndexEngineCallback;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class OLuceneIndexNotUnique extends OIndexAbstract<Set<OIdentifiable>> implements OLuceneIndex {

  public OLuceneIndexNotUnique(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, valueContainerAlgorithm, metadata, version, storage);
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {
    return super.rebuild(iProgressListener);
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    return storage.iterateIndexEntriesBetween(indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null);
  }

  @Override
  public OIndexCursor cursor() {
    return storage.getIndexCursor(indexId, null);
  }

  @Override
  public OIndexCursor descCursor() {
    return storage.getIndexCursor(indexId, null);
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  protected void onIndexEngineChange(int indexId) {

    storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Object>() {
      @Override
      public Object callEngine(OIndexEngine engine) {
        OLuceneIndexEngine oIndexEngine = (OLuceneIndexEngine) engine;
        oIndexEngine.init(getName(), getType(), getDefinition(), isAutomatic(), getMetadata());
        return null;
      }
    });
  }

  protected Object encodeKey(Object key) {
    return key;
  }

  protected Object decodeKey(Object key) {
    return key;
  }

  @Override
  public OLuceneIndexNotUnique put(final Object key, final OIdentifiable singleValue) {

    OTransaction transaction = getDatabase().getTransaction();
    if (transaction.isActive()) {
      OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transaction.addIndexEntry(this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, encodeKey(key), singleValue);

      Document luceneDoc = storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Document>() {
        @Override
        public Document callEngine(OIndexEngine engine) {
          OLuceneIndexEngine oIndexEngine = (OLuceneIndexEngine) engine;
          return oIndexEngine.buildDocument(key, singleValue);
        }
      });

      try {
        transactionChanges.put(key, singleValue, luceneDoc);
      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {

      storage.updateIndexEntry(indexId, key, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return Arrays.asList(singleValue);
        }
      });
    }

    return this;

  }

  @Override
  public boolean remove(final Object key, final OIdentifiable value) {

    OTransaction transaction = getDatabase().getTransaction();
    if (transaction.isActive()) {

      transaction.addIndexEntry(this, super.getName(), OTransactionIndexChanges.OPERATION.REMOVE, encodeKey(key), value);
      OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      try {
        transactionChanges.remove(key, value);
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error while removing", e);
      }
      return true;
    } else {
      return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Boolean>() {
        @Override
        public Boolean callEngine(OIndexEngine engine) {
          OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
          return indexEngine.remove(key, value);
        }
      });
    }
  }

  @Override
  public long getSize() {
    return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Long>() {
      // TODO apply current TX
      @Override
      public Long callEngine(OIndexEngine engine) {
        OTransaction transaction = getDatabase().getTransaction();
        OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
        return indexEngine.sizeInTx(getTransactionChanges(transaction));
      }
    });
  }

  @Override
  public long getKeySize() {
    return 0;
  }

  @Override
  public OLuceneIndexNotUnique create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    return (OLuceneIndexNotUnique) super
        .create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, determineValueSerializer());
  }

  @Override
  public Set<OIdentifiable> get(final Object key) {

    final OTransaction transaction = getDatabase().getTransaction();
    if (transaction.isActive()) {
      return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Set<OIdentifiable>>() {
        @Override
        public Set<OIdentifiable> callEngine(OIndexEngine engine) {
          OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
          return (Set<OIdentifiable>) indexEngine.getInTx(key, getTransactionChanges(transaction));
        }
      });
    } else {
      return (Set<OIdentifiable>) storage.getIndexValue(indexId, key);
    }
  }

  // @Override
  // public OIndexMultiValues put(Object key, OIdentifiable iSingleValue) {
  // checkForRebuild();
  //
  // key = getCollatingValue(key);
  //
  // modificationLock.requestModificationLock();
  // try {
  // acquireExclusiveLock();
  // try {
  // checkForKeyType(key);
  // Set<OIdentifiable> values = new HashSet<OIdentifiable>();
  // values.add(iSingleValue);
  // indexEngine.put(key, values);
  // return this;
  //
  // } finally {
  // releaseExclusiveLock();
  // }
  // } finally {
  // modificationLock.releaseModificationLock();
  // }
  // }

  @Override
  protected void commitSnapshot(final Map<Object, Object> snapshot) {

    storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Object>() {
      @Override
      public Boolean callEngine(OIndexEngine engine) {
        OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;

        for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
          Object key = snapshotEntry.getKey();
          LuceneTxOperations operations = (LuceneTxOperations) snapshotEntry.getValue();

          for (OIdentifiable oIdentifiable : operations.removed) {
            indexEngine.remove(decodeKey(key), oIdentifiable);
          }

        }
        for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
          Object key = snapshotEntry.getKey();
          LuceneTxOperations operations = (LuceneTxOperations) snapshotEntry.getValue();

          indexEngine.put(decodeKey(key), operations.added);

        }
        OTransaction transaction = getDatabase().getTransaction();
        resetTransactionChanges(transaction);
        return null;
      }
    });

  }

  @Override
  public boolean remove(Object key) {
    return super.remove(key);
  }

  @Override
  protected OBinarySerializer determineValueSerializer() {
    return storage.getComponentsFactory().binarySerializerFactory.getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
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

  //
  // @Override
  // public boolean remove(Object key, OIdentifiable value) {
  // checkForRebuild();
  //
  // key = getCollatingValue(key);
  // modificationLock.requestModificationLock();
  // try {
  // acquireExclusiveLock();
  // try {
  //
  // if (indexEngine instanceof OLuceneIndexEngine) {
  // return ((OLuceneIndexEngine) indexEngine).remove(key, value);
  // } else {
  // return false;
  // }
  //
  // } finally {
  // releaseExclusiveLock();
  // }
  // } finally {
  // modificationLock.releaseModificationLock();
  // }
  // }

  // @Override
  // public long rebuild(OProgressListener iProgressListener) {
  //
  // OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
  // try {
  // engine.setRebuilding(true);
  // super.rebuild(iProgressListener);
  // } finally {
  // engine.setRebuilding(false);
  //
  // }
  // engine.flush();
  // return ((OLuceneIndexEngine) indexEngine).size(null);
  //
  // }

  @Override
  public Object getCollatingValue(Object key) {
    return key;
  }

  @Override
  public IndexSearcher searcher() throws IOException {
    return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<IndexSearcher>() {
      @Override
      public IndexSearcher callEngine(OIndexEngine engine) {
        OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
        try {
          return indexEngine.searcher();
        } catch (IOException e) {
          throw OException.wrapException(new OIndexException("Cannot get searcher from index " + getName()), e);
        }
      }
    });

  }

  public OLuceneTxChanges getTransactionChanges(OTransaction transaction) {

    OLuceneTxChanges changes = (OLuceneTxChanges) transaction.getCustomData(getName());
    if (changes == null) {

      changes = storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<OLuceneTxChanges>() {
        @Override
        public OLuceneTxChanges callEngine(OIndexEngine engine) {
          OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
          try {
            return indexEngine.buildTxChanges();
          } catch (IOException e) {
            throw OException.wrapException(new OIndexException("Cannot get searcher from index " + getName()), e);
          }
        }
      });
      transaction.setCustomData(getName(), changes);
    }
    return changes;
  }

  private void resetTransactionChanges(OTransaction transaction) {
    transaction.setCustomData(getName(), null);
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return false;
  }

}
