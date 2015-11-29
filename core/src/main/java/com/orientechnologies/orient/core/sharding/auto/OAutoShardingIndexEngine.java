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
package com.orientechnologies.orient.core.sharding.auto;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.index.hashindex.local.OHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Index engine implementation that relies on multiple hash indexes partitioned by key.
 * 
 * @author Luca Garulli
 */
public final class OAutoShardingIndexEngine implements OIndexEngine {
  public static final int    VERSION                             = 1;
  public static final String METADATA_FILE_EXTENSION             = ".asmm";
  public static final String SUBINDEX_METADATA_FILE_EXTENSION    = ".asm";
  public static final String SUBINDEX_TREE_FILE_EXTENSION        = ".ast";
  public static final String SUBINDEX_BUCKET_FILE_EXTENSION      = ".asb";
  public static final String SUBINDEX_NULL_BUCKET_FILE_EXTENSION = ".asn";

  private final OAbstractPaginatedStorage        storage;
  private final boolean                          durableInNonTx;
  private final OMurmurHash3HashFunction<Object> hashFunction;
  private List<OHashTable<Object, Object>>       partitions;
  private OAutoShardingStrategy                  strategy;
  private int                                    version;
  private final String                           name;
  private int                                    partitionSize;

  public OAutoShardingIndexEngine(final String iName, final Boolean iDurableInNonTxMode, final OAbstractPaginatedStorage iStorage,
      final int iVersion) {
    this.name = iName;
    this.storage = iStorage;
    this.hashFunction = new OMurmurHash3HashFunction<Object>();

    if (iDurableInNonTxMode == null)
      durableInNonTx = OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean();
    else
      durableInNonTx = iDurableInNonTxMode;

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
      ODocument metadata) {

    this.strategy = new OAutoShardingMurmurStrategy(keySerializer);
    this.hashFunction.setValueSerializer(keySerializer);
    this.partitionSize = clustersToIndex.size();

    final OStorage storage = getDatabase().getStorage().getUnderlying();
    if (storage instanceof OLocalPaginatedStorage) {
      // WRITE INDEX METADATA INFORMATION
      final String path = ((OLocalPaginatedStorage) storage).getStoragePath();

      final File fileMetadata = new File(path + "/" + name + METADATA_FILE_EXTENSION);
      if (fileMetadata.exists())
        fileMetadata.delete();
      try {
        if (metadata == null)
          metadata = new ODocument();
        metadata.field("partitions", partitionSize);
        OIOUtils.writeFile(fileMetadata, metadata.toJSON());
      } catch (IOException e1) {
        throw new OConfigurationException("Cannot create sharded index metadata file '" + fileMetadata + "'");
      }
    }

    init();
    for (OHashTable<Object, Object> p : partitions)
      p.create(keySerializer, valueSerializer, keyTypes, nullPointerSupport);
  }

  @Override
  public void load(final String indexName, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final OBinarySerializer keySerializer, final OType[] keyTypes, final boolean nullPointerSupport, final int keySize) {

    this.strategy = new OAutoShardingMurmurStrategy(keySerializer);

    final OStorage storage = getDatabase().getStorage().getUnderlying();
    if (storage instanceof OLocalPaginatedStorage) {
      // LOAD INDEX METADATA INFORMATION
      final String path = ((OLocalPaginatedStorage) storage).getStoragePath();

      final File fileMetadata = new File(path + "/" + name + METADATA_FILE_EXTENSION);

      if (!fileMetadata.exists())
        throw new OConfigurationException("Cannot find sharded index metadata file '" + fileMetadata + "'");
      try {
        final ODocument metadata = new ODocument();
        metadata.fromJSON(OIOUtils.readFileAsString(fileMetadata));
        partitionSize = metadata.field("partitions");
        init();

      } catch (IOException e1) {
        throw new OConfigurationException("Cannot load sharded index metadata file '" + fileMetadata + "'");
      }
    }

    int i = 0;
    for (OHashTable<Object, Object> p : partitions)
      p.load(indexName + "_" + (i++), keyTypes, nullPointerSupport);

    hashFunction.setValueSerializer(keySerializer);
  }

  @Override
  public void flush() {
    for (OHashTable<Object, Object> p : partitions)
      p.flush();
  }

  @Override
  public void deleteWithoutLoad(final String indexName) {
    for (OHashTable<Object, Object> p : partitions)
      p.deleteWithoutLoad(indexName, (OAbstractPaginatedStorage) getDatabase().getStorage().getUnderlying());
  }

  @Override
  public void delete() {
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

    partitions = new ArrayList<OHashTable<Object, Object>>();
    for (int i = 0; i < partitionSize; ++i) {
      partitions
          .add(new OLocalHashTable<Object, Object>(name + "_" + i, SUBINDEX_METADATA_FILE_EXTENSION, SUBINDEX_TREE_FILE_EXTENSION,
              SUBINDEX_BUCKET_FILE_EXTENSION, SUBINDEX_NULL_BUCKET_FILE_EXTENSION, hashFunction, durableInNonTx, storage));
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
    for (OHashTable<Object, Object> p : partitions)
      p.clear();
  }

  @Override
  public void close() {
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
  public long size(final ValuesTransformer transformer) {
    long counter = 0;

    for (OHashTable<Object, Object> p : partitions) {
      if (transformer == null)
        counter += p.size();
      else {
        OHashIndexBucket.Entry<Object, Object> firstEntry = p.firstEntry();
        if (firstEntry == null)
          return 0;

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
    throw new UnsupportedOperationException("keyCursor");
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
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

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private OHashTable<Object, Object> getPartition(final Object iKey) {
    final int partitionId = strategy.getPartitionsId(iKey, partitionSize);
    return partitions.get(partitionId);
  }
}
