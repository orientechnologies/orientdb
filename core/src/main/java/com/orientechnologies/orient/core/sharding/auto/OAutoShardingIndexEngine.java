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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OSHA256HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.OHashIndexBucket;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.OLocalHashTableV2;

import java.io.IOException;
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
  public static final  int    VERSION                             = 1;
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

    final OHashFunction<Object> hashFunction;

    if (encryption != null) {
      //noinspection unchecked
      hashFunction = new OSHA256HashFunction<>(keySerializer);
    } else {
      //noinspection unchecked
      hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
    }

    this.partitionSize = clustersToIndex.size();
    if (metadata != null && metadata.containsField("partitions"))
      this.partitionSize = metadata.field("partitions");

    engineProperties.put("partitions", "" + partitionSize);

    init();

    try {
      for (OHashTable<Object, Object> p : partitions) {
        //noinspection unchecked
        p.create(keySerializer, valueSerializer, keyTypes, encryption, hashFunction, nullPointerSupport);
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during creation of index with name " + name), e);
    }
  }

  @Override
  public void load(final String indexName, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final OBinarySerializer keySerializer, final OType[] keyTypes, final boolean nullPointerSupport, final int keySize,
      final Map<String, String> engineProperties, OEncryption encryption) {

    this.strategy = new OAutoShardingMurmurStrategy(keySerializer);

    if (storage != null) {
      final String partitionsAsString = engineProperties.get("partitions");
      if (partitionsAsString == null || partitionsAsString.isEmpty())
        throw new OIndexException(
            "Cannot load autosharding index '" + indexName + "' because there is no metadata about the number of partitions");

      partitionSize = Integer.parseInt(partitionsAsString);
      init();

      int i = 0;

      final OHashFunction<Object> hashFunction;

      if (encryption != null) {
        //noinspection unchecked
        hashFunction = new OSHA256HashFunction<>(keySerializer);
      } else {
        //noinspection unchecked
        hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
      }

      for (OHashTable<Object, Object> p : partitions)
        //noinspection unchecked
        p.load(indexName + "_" + (i++), keyTypes, nullPointerSupport, encryption, hashFunction, keySerializer, valueSerializer);
    }
  }

  @Override
  public void flush() {
    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions)
        p.flush();
  }

  @Override
  public void deleteWithoutLoad(final String indexName) {
    try {
      if (partitions != null)
        for (OHashTable<Object, Object> p : partitions)
          p.deleteWithoutLoad(indexName);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index with name " + name), e);
    }
  }

  @Override
  public void delete() {
    try {
      if (partitions != null)
        for (OHashTable<Object, Object> p : partitions)
          p.delete();
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index with name " + name), e);
    }
  }

  @Override
  public void init(final String indexName, final String indexType, final OIndexDefinition indexDefinition,
      final boolean isAutomatic, final ODocument metadata) {
  }

  private void init() {
    if (partitions != null)
      return;

    partitions = new ArrayList<>(partitionSize);
    for (int i = 0; i < partitionSize; ++i) {
      partitions.add(new OLocalHashTableV2<>(name + "_" + i, SUBINDEX_METADATA_FILE_EXTENSION, SUBINDEX_TREE_FILE_EXTENSION,
          SUBINDEX_BUCKET_FILE_EXTENSION, SUBINDEX_NULL_BUCKET_FILE_EXTENSION, storage));
    }
  }

  @Override
  public boolean contains(final Object key) {
    return getPartition(key).get(key) != null;
  }

  @Override
  public boolean remove(final Object key) {
    try {
      return getPartition(key).remove(key) != null;
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of key " + key + " of index with name " + name), e);
    }
  }

  @Override
  public void clear() {
    try {
      if (partitions != null)
        for (OHashTable<Object, Object> p : partitions)
          p.clear();
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during clear of index with name " + name), e);
    }
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
    try {
      getPartition(key).put(key, value);
    } catch (IOException e) {
      throw OException
          .wrapException(new OIndexException("Error during insertion of key " + key + " of index with name " + name), e);
    }
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
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    try {
      return getPartition(key).validatedPut(key, value, (Validator) validator);
    } catch (IOException e) {
      throw OException
          .wrapException(new OIndexException("Error during insertion of key " + key + " of index with name " + name), e);
    }
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    long counter = 0;

    if (partitions != null)
      for (OHashTable<Object, Object> p : partitions) {
        if (transformer == null)
          counter += p.size();
        else {
          final OHashIndexBucket.Entry<Object, Object> firstEntry = p.firstEntry();
          if (firstEntry == null)
            continue;

          OHashIndexBucket.Entry<Object, Object>[] entries = p.ceilingEntries(firstEntry.key);

          while (entries.length > 0) {
            for (OHashIndexBucket.Entry<Object, Object> entry : entries)
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
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      {
        if (partitions == null || partitions.isEmpty())
          //noinspection unchecked
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
        else {
          hashTable = partitions.get(0);
          OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
          if (firstEntry == null)
            //noinspection unchecked
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

        final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];
        nextEntriesIndex++;
        if (nextEntriesIndex >= entries.length) {
          entries = hashTable.higherEntries(entries[entries.length - 1].key);
          nextEntriesIndex = 0;

          if (entries.length == 0 && nextPartition < partitions.size()) {
            // GET NEXT PARTITION
            hashTable = partitions.get(nextPartition++);
            OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
            if (firstEntry == null)
              //noinspection unchecked
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

  private OHashTable<Object, Object> getPartition(final Object iKey) {
    final int partitionId = iKey != null ? strategy.getPartitionsId(iKey, partitionSize) : 0;
    return partitions.get(partitionId);
  }
}
