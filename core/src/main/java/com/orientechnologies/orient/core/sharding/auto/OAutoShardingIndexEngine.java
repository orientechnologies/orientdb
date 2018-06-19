/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sharding.auto;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTableBucket;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OSHA256HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.OLocalHashTableV2;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v3.OLocalHashTableV3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Index engine implementation that relies on multiple hash indexes partitioned by key.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public final class OAutoShardingIndexEngine implements OIndexEngine {
  public static final  int    VERSION                             = 2;
  private static final String SUBINDEX_METADATA_FILE_EXTENSION    = ".asm";
  private static final String SUBINDEX_TREE_FILE_EXTENSION        = ".ast";
  private static final String SUBINDEX_BUCKET_FILE_EXTENSION      = ".asb";
  private static final String SUBINDEX_NULL_BUCKET_FILE_EXTENSION = ".asn";

  private final OAbstractPaginatedStorage        storage;
  private       List<OHashTable<Object, Object>> partitions;
  private       OAutoShardingStrategy            strategy;
  private final int                              version;
  private final String                           name;
  private       int                              partitionSize;
  private final AtomicLong                       bonsayFileId = new AtomicLong(0);

  OAutoShardingIndexEngine(final String iName, final OAbstractPaginatedStorage iStorage, final int iVersion) {
    this.name = iName;
    this.storage = iStorage;

    this.version = iVersion;
  }

  @Override
  public String getName() {
    return name;
  }

  public OAutoShardingStrategy getStrategy() {
    return strategy;
  }

  @Override
  public void create(final OBinarySerializer valueSerializer, final boolean isAutomatic, final OType[] keyTypes,
      final boolean nullPointerSupport, final OBinarySerializer keySerializer, final int keySize, final Set<String> clustersToIndex,
      final Map<String, String> engineProperties, final ODocument metadata, OEncryption encryption) {

    this.strategy = new OAutoShardingMurmurStrategy(keySerializer);
    this.partitionSize = clustersToIndex.size();
    if (metadata != null && metadata.containsField("partitions"))
      this.partitionSize = metadata.field("partitions");

    engineProperties.put("partitions", "" + partitionSize);

    init();

    final OHashFunction<Object> hashFunction;
    if (encryption != null) {
      hashFunction = new OSHA256HashFunction<>(keySerializer);
    } else {
      hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
    }
    for (OHashTable<Object, Object> p : partitions) {
      p.create(keySerializer, valueSerializer, encryption, nullPointerSupport, keyTypes, hashFunction);
    }
  }

  @Override
  public void load(final String indexName, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final OBinarySerializer keySerializer, final OType[] keyTypes, final boolean nullPointerSupport, final int keySize,
      final Map<String, String> engineProperties, OEncryption encryption) {

    this.strategy = new OAutoShardingMurmurStrategy(keySerializer);

    if (storage instanceof OAbstractPaginatedStorage) {
      final String partitionsAsString = engineProperties.get("partitions");
      if (partitionsAsString == null || partitionsAsString.isEmpty())
        throw new OIndexException(
            "Cannot load autosharding index '" + indexName + "' because there is no metadata about the number of partitions");

      partitionSize = Integer.parseInt(partitionsAsString);
      init();

      OHashFunction<Object> hashFunction;
      if (encryption != null) {
        hashFunction = new OSHA256HashFunction<>(keySerializer);
      } else {
        hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
      }

      int i = 0;
      for (OHashTable<Object, Object> p : partitions)
        p.load(indexName + "_" + (i++), keyTypes, nullPointerSupport, keySerializer, valueSerializer, encryption, hashFunction);
    }
  }

  @Override
  public <I> I getComponent(String name) {
    for (OHashTable<Object, Object> partition : partitions) {
      if (partition.getName().equals(name)) {
        return (I) partition;
      }
    }

    throw new IllegalStateException("Component with name " + name + " was not found");
  }

  @Override
  public void flush() {
    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions)
        p.flush();
  }

  @Override
  public void deleteWithoutLoad(final String indexName) {
    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions)
        p.deleteWithoutLoad(indexName);
  }

  @Override
  public void delete() {
    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions)
        p.delete();
  }

  @Override
  public void init(final String indexName, final String indexType, final OIndexDefinition indexDefinition,
      final boolean isAutomatic, final ODocument metadata) {
  }

  private void init() {
    if (partitions != null)
      return;

    partitions = new ArrayList<>(partitionSize);

    if (version == 1) {
      for (int i = 0; i < partitionSize; ++i) {
        partitions.add(new OLocalHashTableV2<>(name + "_" + i, SUBINDEX_METADATA_FILE_EXTENSION, SUBINDEX_TREE_FILE_EXTENSION,
            SUBINDEX_BUCKET_FILE_EXTENSION, SUBINDEX_NULL_BUCKET_FILE_EXTENSION, storage));
      }
    } else if (version == 2) {
      for (int i = 0; i < partitionSize; ++i) {
        partitions.add(new OLocalHashTableV3<>(name + "_" + i, SUBINDEX_METADATA_FILE_EXTENSION, SUBINDEX_TREE_FILE_EXTENSION,
            SUBINDEX_BUCKET_FILE_EXTENSION, SUBINDEX_NULL_BUCKET_FILE_EXTENSION, storage));
      }
    } else {
      throw new IllegalStateException("Illegal index version " + version);
    }
  }

  @Override
  public boolean contains(final Object key) {
    return getPartition(key).get(key) != null;
  }

  @Override
  public boolean remove(final Object key) {
    return getPartition(key).remove(key) != null;
  }

  @Override
  public void clear() {
    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions)
        p.clear();
  }

  @Override
  public void close() {
    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions)
        p.close();
  }

  @Override
  public Object get(final Object key) {
    return getPartition(key).get(key);
  }

  @Override
  public void put(final Object key, final Object value) {
    getPartition(key).put(key, value);
  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {
    Object value = get(key);
    OIndexUpdateAction<Object> updated = updater.update(value, bonsayFileId);
    if (updated.isChange())
      put(key, updated.getValue());
    else if (updated.isRemove()) {
      remove(key);
    } else if (updated.isNothing()) {
      //Do Nothing
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(Object key, OIdentifiable value, Validator<Object, OIdentifiable> validator) {
    return getPartition(key).validatedPut(key, value, (Validator) validator);
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    long counter = 0;

    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions) {
        if (transformer == null)
          counter += p.size();
        else {
          final OHashTableBucket.Entry<Object, Object> firstEntry = p.firstEntry();
          if (firstEntry == null)
            continue;

          OHashTableBucket.Entry<Object, Object>[] entries = p.ceilingEntries(firstEntry.key);

          while (entries.length > 0) {
            for (OHashTableBucket.Entry<Object, Object> entry : entries)
              counter += transformer.transformFromValue(entry.value).size();

            entries = p.higherEntries(entries[entries.length - 1].key);
          }
        }
      }
    return counter;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public OIndexCursor cursor(final ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("cursor");
  }

  @Override
  public OIndexCursor descCursor(final ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("descCursor");
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private int nextPartition = 1;
      private OHashTable<Object, Object> hashTable;
      private int nextEntriesIndex;
      private OHashTableBucket.Entry<Object, Object>[] entries;

      {
        if (partitions == null || partitions.isEmpty())
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
        else {
          hashTable = partitions.get(0);
          OHashTableBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
          if (firstEntry == null)
            entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
          else
            entries = hashTable.ceilingEntries(firstEntry.key);
        }
      }

      @Override
      public Object next(final int prefetchSize) {
        if (entries.length == 0) {
          return null;
        }

        final OHashTableBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];
        nextEntriesIndex++;
        if (nextEntriesIndex >= entries.length) {
          entries = hashTable.higherEntries(entries[entries.length - 1].key);
          nextEntriesIndex = 0;

          if (entries.length == 0 && nextPartition < partitions.size()) {
            // GET NEXT PARTITION
            hashTable = partitions.get(nextPartition++);
            OHashTableBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
            if (firstEntry == null)
              entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            else
              entries = hashTable.ceilingEntries(firstEntry.key);
          }
        }

        return bucketEntry.key;
      }
    };
  }

  @Override
  public OIndexCursor iterateEntriesBetween(final Object rangeFrom, final boolean fromInclusive, final Object rangeTo,
      final boolean toInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(final Object fromKey, final boolean isInclusive, final boolean ascSortOrder,
      ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("firstKey");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("lastKey");
  }

  @Override
  public boolean acquireAtomicExclusiveLock(final Object key) {
    getPartition(key).acquireAtomicExclusiveLock();
    return false;
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return getPartition(key).getName();
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private OHashTable<Object, Object> getPartition(final Object iKey) {
    final int partitionId = iKey != null ? strategy.getPartitionsId(iKey, partitionSize) : 0;
    return partitions.get(partitionId);
  }
}
