/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.OLuceneTxOperations;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.storage.OBasicTransaction;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OLuceneIndexNotUnique extends OIndexAbstract implements OLuceneIndex {

  public OLuceneIndexNotUnique(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata, final int binaryFormatVersion) {
    super(name, typeId, algorithm, valueContainerAlgorithm, metadata, version, storage, binaryFormatVersion);
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {
    return super.rebuild(iProgressListener);
  }

  @Override
  public boolean remove(final Object key, final OIdentifiable value) {

    if (key != null) {
      OBasicTransaction transaction = getDatabase().getMicroOrRegularTransaction();
      if (transaction.isActive()) {

        transaction.addIndexEntry(this, super.getName(), OTransactionIndexChanges.OPERATION.REMOVE, encodeKey(key), value);
        OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
        transactionChanges.remove(key, value);
        return true;
      } else {
        while (true) {
          try {
            return storage.callIndexEngine(false, false, indexId, engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.remove(key, value);
            });
          } catch (OInvalidIndexEngineIdException e) {
            doReloadIndexEngine();
          }
        }

      }
    }
    return true;
  }

  @Override
  public boolean remove(Object key) {
    return super.remove(key);
  }

  @Override
  public OIndexAbstract removeCluster(String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.remove(iClusterName)) {
        updateConfiguration();
        remove("_CLUSTER:" + storage.getClusterByName(iClusterName).getId());
      }

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected OBinarySerializer determineValueSerializer() {
    return storage.getComponentsFactory().binarySerializerFactory.getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
  }

  @Override
  protected Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.NonUnique);
  }

  @Override
  public Object getCollatingValue(Object key) {
    return key;
  }

  @Override
  protected void commitSnapshot(final Map<Object, Object> snapshot) {
    while (true)
      try {
        storage.callIndexEngine(false, false, indexId, engine -> {
          OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;

          for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
            Object key = snapshotEntry.getKey();
            OLuceneTxOperations operations = (OLuceneTxOperations) snapshotEntry.getValue();

            for (OIdentifiable oIdentifiable : operations.removed) {
              indexEngine.remove(decodeKey(key), oIdentifiable);
            }

          }
          try {
            for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
              Object key = snapshotEntry.getKey();
              OLuceneTxOperations operations = (OLuceneTxOperations) snapshotEntry.getValue();

              indexEngine.put(decodeKey(key), operations.added);

            }
            OBasicTransaction transaction = getDatabase().getMicroOrRegularTransaction();
            resetTransactionChanges(transaction);
            return null;
          } catch (IOException e) {
            throw OException.wrapException(new OIndexException("Error during commit of index changes"), e);
          }
        });
        break;
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }

  }

  public OLuceneIndexNotUnique delete() {
    acquireExclusiveLock();

    try {
      while (true)
        try {
          storage.deleteIndexEngine(indexId);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }

      // REMOVE THE INDEX ALSO FROM CLASS MAP
      if (getDatabase().getMetadata() != null)
        getDatabase().getMetadata().getIndexManagerInternal().removeClassPropertyIndex(this);
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  protected Object decodeKey(Object key) {
    return key;
  }

  private void resetTransactionChanges(OBasicTransaction transaction) {
    transaction.setCustomData(getName(), null);
  }

  @Override
  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    OLuceneTxOperations operations = (OLuceneTxOperations) snapshot.get(key);

    if (operations == null) {
      operations = new OLuceneTxOperations();
      snapshot.put(key, operations);
    }
    operations.added.add(value.getIdentity());
    snapshot.put(key, operations);
  }

  @Override
  protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    OLuceneTxOperations operations = (OLuceneTxOperations) snapshot.get(key);
    if (operations == null) {
      operations = new OLuceneTxOperations();
      snapshot.put(key, operations);
    }
    operations.removed.add(value.getIdentity());
    snapshot.put(key, operations);
  }

  @Override
  protected void clearSnapshot(IndexTxSnapshot indexTxSnapshot) {
    indexTxSnapshot.clear = true;
    indexTxSnapshot.indexSnapshot.clear();
  }

  @Override
  protected void onIndexEngineChange(int indexId) {
    while (true)
      try {
        storage.callIndexEngine(false, false, indexId, engine -> {
          OLuceneIndexEngine oIndexEngine = (OLuceneIndexEngine) engine;
          oIndexEngine.init(getName(), getType(), getDefinition(), isAutomatic(), getMetadata());
          return null;
        });
        break;
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  protected Object encodeKey(Object key) {
    return key;
  }

  private OLuceneTxChanges getTransactionChanges(OBasicTransaction transaction) {

    OLuceneTxChanges changes = (OLuceneTxChanges) transaction.getCustomData(getName());
    if (changes == null) {
      while (true)
        try {
          changes = storage.callIndexEngine(false, false, indexId, engine -> {
            OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
            try {
              return indexEngine.buildTxChanges();
            } catch (IOException e) {
              throw OException.wrapException(new OIndexException("Cannot get searcher from index " + getName()), e);
            }
          });
          break;
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }

      transaction.setCustomData(getName(), changes);
    }
    return changes;
  }

  @Override
  public OLuceneIndexNotUnique create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    return (OLuceneIndexNotUnique) super
        .create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, determineValueSerializer());
  }

  @Deprecated
  @Override
  public Collection<OIdentifiable> get(final Object key) {
    try (Stream<ORID> stream = getRids(key)) {
      return stream.collect(Collectors.toList());
    }
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    final OBasicTransaction transaction = getDatabase().getMicroOrRegularTransaction();
    if (transaction.isActive()) {
      while (true) {
        try {
          //noinspection resource
          return storage.callIndexEngine(false, false, indexId, engine -> {
            OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
            return indexEngine.getInTx(key, getTransactionChanges(transaction));
          }).stream().map(OIdentifiable::getIdentity);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }

    } else {
      while (true) {
        try {
          @SuppressWarnings("unchecked")
          Set<OIdentifiable> result = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);
          //noinspection resource
          return result.stream().map(OIdentifiable::getIdentity);
          // TODO filter these results based on security
//          return new HashSet(OIndexInternal.securityFilterOnRead(this, result));
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    }
  }

  @Override
  public OLuceneIndexNotUnique put(final Object key, final OIdentifiable singleValue) {

    if (key != null) {
      OBasicTransaction transaction = getDatabase().getMicroOrRegularTransaction();

      if (transaction.isActive()) {
        OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
        transaction.addIndexEntry(this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, encodeKey(key), singleValue);

        Document luceneDoc;
        while (true) {
          try {
            luceneDoc = storage.callIndexEngine(false, false, indexId, engine -> {
              OLuceneIndexEngine oIndexEngine = (OLuceneIndexEngine) engine;
              return oIndexEngine.buildDocument(key, singleValue);
            });
            break;
          } catch (OInvalidIndexEngineIdException e) {
            doReloadIndexEngine();
          }
        }

        transactionChanges.put(key, singleValue, luceneDoc);

      } else {
        while (true) {
          try {
            storage.putIndexValue(indexId, key, Collections.singletonList(singleValue));
            break;
          } catch (OInvalidIndexEngineIdException e) {
            doReloadIndexEngine();
          }
        }
      }
    }
    return this;

  }

  @Override
  public long size() {
    while (true) {
      try {
        // TODO apply current TX
        return storage.callIndexEngine(false, false, indexId, engine -> {
          OBasicTransaction transaction = getDatabase().getMicroOrRegularTransaction();
          OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
          return indexEngine.sizeInTx(getTransactionChanges(transaction));
        });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }

  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {

    @SuppressWarnings("resource")
    String query = (String) keys.stream().findFirst().map(k -> (OCompositeKey) k).map(OCompositeKey::getKeys)
        .orElse(Collections.singletonList("q=*:*")).get(0);
    return IndexStreamSecurityDecorator.decorateStream(this, getRids(query).map((rid) -> new ORawPair<>(query, rid)));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(this,
            storage.iterateIndexEntriesBetween(indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }

  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator
            .decorateStream(this, storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator
            .decorateStream(this, storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(this, storage.getIndexStream(indexId, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }

  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(this, storage.getIndexStream(indexId, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public IndexSearcher searcher() {
    while (true) {
      try {
        return storage.callIndexEngine(false, false, indexId, engine -> {
          OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
          return indexEngine.searcher();
        });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return false;
  }

}
