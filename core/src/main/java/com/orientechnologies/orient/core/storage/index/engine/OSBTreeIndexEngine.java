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
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeV2;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
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
  private final int                     version;
  private final String                  name;
  private final int                     id;

  public OSBTreeIndexEngine(final int id, String name, OAbstractPaginatedStorage storage, int version) {
    this.id = id;
    this.name = name;
    this.version = version;

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
    final Spliterator<Object> keySpliterator = sbTree.keySpliterator();
    StreamSupport.stream(keySpliterator, false).forEach((key) -> {
      try {
        sbTree.remove(key);
      } catch (final IOException e) {
        throw OException.wrapException(new OIndexException("Error during clearing a tree" + name), e);
      }
    });

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
  public boolean contains(Object key) {
    return sbTree.get(key) != null;
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
  public int getVersion() {
    return version;
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
  public IndexCursor cursor(ValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return new NullCursor();
    }

    return new OSBTreeIndexCursor(sbTree.iterateEntriesMajor(firstKey, true, true), valuesTransformer);
  }

  @Override
  public IndexCursor descCursor(ValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return new NullCursor();
    }

    return new OSBTreeIndexCursor(sbTree.iterateEntriesMinor(lastKey, true, false), valuesTransformer);
  }

  @Override
  public IndexKeySpliterator keyCursor() {
    return new IndexKeySpliterator() {
      private final Spliterator<Object> sbTreeKeySpliterator = sbTree.keySpliterator();

      @Override
      public boolean tryAdvance(Consumer<? super Object> action) {
        return sbTreeKeySpliterator.tryAdvance(action);
      }

      @Override
      public Spliterator<Object> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return NONNULL | ORDERED;
      }
    };
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
  public Object getFirstKey() {
    return sbTree.firstKey();
  }

  @Override
  public Object getLastKey() {
    return sbTree.lastKey();
  }

  @Override
  public IndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder),
        transformer);
  }

  @Override
  public IndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder), transformer);
  }

  @Override
  public IndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder), transformer);
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
        final Spliterator<ORawPair<Object, Object>> spliterator = sbTree.iterateEntriesBetween(firstKey, true, lastKey, true, true);
        counter += StreamSupport.stream(spliterator, false).mapToInt((pair) -> transformer.transformFromValue(pair.second).size())
            .sum();
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

  private static final class OSBTreeIndexCursor implements IndexCursor {
    private final Iterator<ORawPair<Object, ORID>> entriesIterator;

    private OSBTreeIndexCursor(Spliterator<ORawPair<Object, Object>> treeSpliterator, ValuesTransformer valuesTransformer) {

      if (valuesTransformer == null) {
        entriesIterator = StreamSupport.stream(treeSpliterator, false).map((pair) -> new ORawPair<>(pair.first, (ORID) pair.second))
            .iterator();
      } else {
        entriesIterator = StreamSupport.stream(treeSpliterator, false).flatMap((pair) -> {
          final Collection<ORID> values = valuesTransformer.transformFromValue(pair.second);
          return values.stream().map((rid) -> new ORawPair<>(pair.first, rid));
        }).iterator();
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      if (entriesIterator.hasNext()) {
        action.accept(entriesIterator.next());
        return true;
      }

      return false;
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return NONNULL | ORDERED;
    }
}

  private static class NullCursor implements IndexCursor {

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return false;
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }
}
