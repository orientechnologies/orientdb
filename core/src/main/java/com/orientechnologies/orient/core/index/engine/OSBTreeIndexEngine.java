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

package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Andrey Lomakin
 * @since 8/30/13
 */
public class OSBTreeIndexEngine extends OSharedResourceAdaptiveExternal implements OIndexEngine {
  public static final int               VERSION                    = 1;

  public static final String            DATA_FILE_EXTENSION        = ".sbt";
  public static final String            NULL_BUCKET_FILE_EXTENSION = ".nbt";

  private final OSBTree<Object, Object> sbTree;
  private int                           version;
  private final String                  name;

  public OSBTreeIndexEngine(String name, Boolean durableInNonTxMode, OAbstractPaginatedStorage storage, int version) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);

    this.name = name;
    boolean durableInNonTx;

    if (durableInNonTxMode == null)
      durableInNonTx = OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean();
    else
      durableInNonTx = durableInNonTxMode;

    this.version = version;

    sbTree = new OSBTree<Object, Object>(name, DATA_FILE_EXTENSION, durableInNonTx, NULL_BUCKET_FILE_EXTENSION, storage);
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
    acquireSharedLock();
    try {
      sbTree.flush();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
                        OBinarySerializer keySerializer, int keySize, ODocument metadata) {
    acquireExclusiveLock();
    try {
      sbTree.create(keySerializer, valueSerializer, keyTypes, keySize, nullPointerSupport);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() {
    acquireSharedLock();
    try {
      sbTree.delete();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    acquireExclusiveLock();
    try {
      sbTree.deleteWithoutLoad(indexName);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize) {
    acquireExclusiveLock();
    try {
      sbTree.load(indexName, keySerializer, valueSerializer, keyTypes, keySize, nullPointerSupport);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean contains(Object key) {
    acquireSharedLock();
    try {
      return sbTree.get(key) != null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean remove(Object key) {
    acquireSharedLock();
    try {
      return sbTree.remove(key) != null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public void clear() {
    acquireSharedLock();
    try {
      sbTree.clear();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void close() {
    acquireSharedLock();
    try {
      sbTree.close();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object get(Object key) {
    acquireSharedLock();
    try {
      return sbTree.get(key);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    acquireSharedLock();
    try {
      final Object firstKey = sbTree.firstKey();
      if (firstKey == null)
        return new NullCursor();

      return new OSBTreeIndexCursor(sbTree.iterateEntriesMajor(firstKey, true, true), valuesTransformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    acquireSharedLock();
    try {
      final Object lastKey = sbTree.lastKey();
      if (lastKey == null)
        return new NullCursor();

      return new OSBTreeIndexCursor(sbTree.iterateEntriesMinor(lastKey, true, false), valuesTransformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    acquireSharedLock();
    try {
      return new OIndexKeyCursor() {
        private final OSBTree.OSBTreeKeyCursor<Object> sbTreeKeyCursor = sbTree.keyCursor();

        @Override
        public Object next(int prefetchSize) {
          return sbTreeKeyCursor.next(prefetchSize);
        }
      };
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void put(Object key, Object value) {
    acquireSharedLock();
    try {
      sbTree.put(key, value);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object getFirstKey() {
    acquireSharedLock();
    try {
      return sbTree.firstKey();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object getLastKey() {
    acquireSharedLock();
    try {
      return sbTree.lastKey();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    acquireSharedLock();
    try {
      return new OSBTreeIndexCursor(sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder),
          transformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    acquireSharedLock();
    try {
      return new OSBTreeIndexCursor(sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder), transformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    acquireSharedLock();
    try {
      return new OSBTreeIndexCursor(sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder), transformer);

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    acquireSharedLock();
    try {
      if (transformer == null)
        return sbTree.size();
      else {
        final Object firstKey = sbTree.firstKey();
        final Object lastKey = sbTree.lastKey();

        if (firstKey != null && lastKey != null) {
          int counter = 0;

          final OSBTree.OSBTreeCursor<Object, Object> cursor = sbTree.iterateEntriesBetween(firstKey, true, lastKey, true, true);
          Map.Entry<Object, Object> entry = cursor.next(-1);
          while (entry != null) {
            counter += transformer.transformFromValue(entry.getValue()).size();
            entry = cursor.next(-1);
          }

          return counter;
        }

        return 0;
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  private static final class OSBTreeIndexCursor extends OIndexAbstractCursor {
    private final OSBTree.OSBTreeCursor<Object, Object> treeCursor;
    private final ValuesTransformer                     valuesTransformer;

    private Iterator<OIdentifiable>                     currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
    private Object                                      currentKey      = null;

    private OSBTreeIndexCursor(OSBTree.OSBTreeCursor<Object, Object> treeCursor, ValuesTransformer valuesTransformer) {
      this.treeCursor = treeCursor;
      this.valuesTransformer = valuesTransformer;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (valuesTransformer == null) {
        final Object entry = treeCursor.next(getPrefetchSize());
        return (Map.Entry<Object, OIdentifiable>) entry;
      }

      if (currentIterator == null)
        return null;

      while (!currentIterator.hasNext()) {
        final Object p = treeCursor.next(getPrefetchSize());
        final Map.Entry<Object, OIdentifiable> entry = (Map.Entry<Object, OIdentifiable>) p;

        if (entry == null) {
          currentIterator = null;
          return null;
        }

        currentKey = entry.getKey();
        currentIterator = valuesTransformer.transformFromValue(entry.getValue()).iterator();
      }

      final OIdentifiable value = currentIterator.next();

      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return currentKey;
        }

        @Override
        public OIdentifiable getValue() {
          return value;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }

  private static class NullCursor extends OIndexAbstractCursor {
    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      return null;
    }
  }
}
