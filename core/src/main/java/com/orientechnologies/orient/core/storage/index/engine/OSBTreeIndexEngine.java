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

package com.orientechnologies.orient.core.storage.index.engine;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeV2;

import java.io.IOException;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/30/13
 */
public class OSBTreeIndexEngine implements OIndexEngine {
  public static final int VERSION = 2;

  public static final String DATA_FILE_EXTENSION        = ".sbt";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";

  private final OSBTree<Object, Object> sbTree;
  private final String                  name;
  private final int                     id;

  public OSBTreeIndexEngine(final int id, String name, OAbstractPaginatedStorage storage, int version) {
    this.id = id;
    this.name = name;

    if (version == 1) {
      sbTree = new OSBTreeV1<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else if (version == 2) {
      sbTree = new OSBTreeV2<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else {
      throw new IllegalStateException("Invalid version of index, version = " + version);
    }
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void flush() {
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
    try {
      //noinspection unchecked
      sbTree.create(keySerializer, valueSerializer, keyTypes, keySize, nullPointerSupport, encryption);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during creation of index " + name), e);
    }
  }

  @Override
  public void delete() {
    try {
      doClearTree();

      sbTree.delete();
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  private void doClearTree() throws IOException {
    try (final Stream<Object> stream = sbTree.keyStream()) {
      stream.forEach((key) -> {
        try {
          sbTree.remove(key);
        } catch (final IOException e) {
          throw OException.wrapException(new OIndexException("Error during clearing a tree" + name), e);
        }
      });
    }

    if (sbTree.isNullPointerSupport()) {
      sbTree.remove(null);
    }
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
    //noinspection unchecked
    sbTree.load(indexName, keySerializer, valueSerializer, keyTypes, keySize, nullPointerSupport, encryption);
  }

  @Override
  public boolean remove(Object key) {
    try {
      return sbTree.remove(key) != null;
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during removal of key " + key + " from index " + name), e);
    }
  }

  @Override
  public void clear() {
    try {
      doClearTree();
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during clear index " + name), e);
    }
  }

  @Override
  public void close() {
    sbTree.close();
  }

  @Override
  public Object get(Object key) {
    return sbTree.get(key);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return StreamSupport.stream(Spliterators.emptySpliterator(), false);
    }
    return convertTreeStreamToIndexStream(valuesTransformer, sbTree.iterateEntriesMajor(firstKey, true, true));
  }

  private static Stream<ORawPair<Object, ORID>> convertTreeStreamToIndexStream(ValuesTransformer valuesTransformer,
      Stream<ORawPair<Object, Object>> treeStream) {
    if (valuesTransformer == null) {
      return treeStream.map((entry) -> new ORawPair<>(entry.first, (ORID) entry.second));
    } else {
      //noinspection resource
      return treeStream.flatMap(
          (entry) -> valuesTransformer.transformFromValue(entry.second).stream().map((rid) -> new ORawPair<>(entry.first, rid)));
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return StreamSupport.stream(Spliterators.emptySpliterator(), false);
    }

    return convertTreeStreamToIndexStream(valuesTransformer, sbTree.iterateEntriesMinor(lastKey, true, false));
  }

  @Override
  public Stream<Object> keyStream() {
    return sbTree.keyStream();
  }

  @Override
  public void put(Object key, Object value) {
    try {
      sbTree.put(key, value);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during insertion of key " + key + " in index " + name), e);
    }
  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {
    try {
      sbTree.update(key, updater, null);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during update of key " + key + " in index " + name), e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    try {
      return sbTree.validatedPut(key, value, (Validator) validator);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during insertion of key " + key + " in index " + name), e);
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo,
      boolean toInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return convertTreeStreamToIndexStream(transformer,
        sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return convertTreeStreamToIndexStream(transformer, sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return convertTreeStreamToIndexStream(transformer, sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder));
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (transformer == null) {
      return sbTree.size();
    } else {
      int counter = 0;

      if (sbTree.isNullPointerSupport()) {
        final Object nullValue = sbTree.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      final Object firstKey = sbTree.firstKey();
      final Object lastKey = sbTree.lastKey();

      if (firstKey != null && lastKey != null) {
        try (final Stream<ORawPair<Object, Object>> stream = sbTree.iterateEntriesBetween(firstKey, true, lastKey, true, true)) {
          counter += stream.mapToInt((pair) -> transformer.transformFromValue(pair.second).size()).sum();
        }
        return counter;
      }

      return counter;
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    sbTree.acquireAtomicExclusiveLock();
    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

}
