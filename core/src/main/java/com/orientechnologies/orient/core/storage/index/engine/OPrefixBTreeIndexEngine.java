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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OPrefixBTree;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/30/13
 */
public class OPrefixBTreeIndexEngine implements OIndexEngine {
  public static final int VERSION = 1;

  public static final String DATA_FILE_EXTENSION        = ".pbt";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".npt";

  private final OPrefixBTree<Object> prefixTree;
  private final int                  version;
  private final String               name;

  public OPrefixBTreeIndexEngine(String name, OAbstractPaginatedStorage storage, int version) {
    this.name = name;
    this.version = version;

    prefixTree = new OPrefixBTree<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
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
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata, OEncryption encryption) {

    prefixTree.create(keySerializer, valueSerializer, nullPointerSupport, encryption);
  }

  @Override
  public void delete() {
    prefixTree.delete();
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    prefixTree.deleteWithoutLoad();
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
    prefixTree.load(indexName, keySerializer, valueSerializer, nullPointerSupport, encryption);
  }

  @Override
  public boolean contains(Object key) {
    if (key == null) {
      return prefixTree.get(null) != null;
    }

    return prefixTree.get(key.toString()) != null;
  }

  @Override
  public boolean remove(Object key) {
    if (key == null) {
      return prefixTree.remove(null) != null;
    }

    return prefixTree.remove(key.toString()) != null;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public void clear() {
    prefixTree.clear();
  }

  @Override
  public void close() {
    prefixTree.close();
  }

  @Override
  public Object get(Object key) {
    if (key != null) {
      return prefixTree.get(key.toString());
    }

    return prefixTree.get(null);
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    final String firstKey = prefixTree.firstKey();
    if (firstKey == null) {
      return new NullCursor();
    }

    return new OSBTreeIndexCursor(prefixTree.iterateEntriesMajor(firstKey, true, true), valuesTransformer);
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    final String lastKey = prefixTree.lastKey();
    if (lastKey == null) {
      return new NullCursor();
    }

    return new OSBTreeIndexCursor(prefixTree.iterateEntriesMinor(lastKey, true, false), valuesTransformer);
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private final OPrefixBTree.OSBTreeKeyCursor<String> sbTreeKeyCursor = prefixTree.keyCursor();

      @Override
      public Object next(int prefetchSize) {
        return sbTreeKeyCursor.next(prefetchSize);
      }
    };
  }

  @Override
  public void put(Object key, Object value) {
    if (key == null) {
      prefixTree.put(null, value);
    } else {
      prefixTree.put(key.toString(), value);
    }
  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {
    if (key == null) {
      prefixTree.update(null, updater, null);
    } else {
      prefixTree.update(key.toString(), updater, null);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(Object key, OIdentifiable value, Validator<Object, OIdentifiable> validator) {
    if (key == null) {
      return prefixTree.validatedPut(null, value, (Validator) validator);
    }

    return prefixTree.validatedPut(key.toString(), value, (Validator) validator);
  }

  @Override
  public Object getFirstKey() {
    return prefixTree.firstKey();
  }

  @Override
  public Object getLastKey() {
    return prefixTree.lastKey();
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(
        prefixTree.iterateEntriesBetween(rangeFrom.toString(), fromInclusive, rangeTo.toString(), toInclusive, ascSortOrder),
        transformer);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(
        prefixTree.iterateEntriesMajor(fromKey == null ? null : fromKey.toString(), isInclusive, ascSortOrder), transformer);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(
        prefixTree.iterateEntriesMinor(toKey == null ? null : toKey.toString(), isInclusive, ascSortOrder), transformer);
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (transformer == null)
      return prefixTree.size();
    else {
      int counter = 0;

      if (prefixTree.isNullPointerSupport()) {
        final Object nullValue = prefixTree.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      final Object firstKey = prefixTree.firstKey();
      final Object lastKey = prefixTree.lastKey();

      if (firstKey != null && lastKey != null) {
        final OPrefixBTree.OSBTreeCursor<String, Object> cursor = prefixTree
            .iterateEntriesBetween(firstKey.toString(), true, lastKey.toString(), true, true);
        Map.Entry<String, Object> entry = cursor.next(-1);
        while (entry != null) {
          counter += transformer.transformFromValue(entry.getValue()).size();
          entry = cursor.next(-1);
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
    prefixTree.acquireAtomicExclusiveLock();
    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  private static final class OSBTreeIndexCursor extends OIndexAbstractCursor {
    private final OPrefixBTree.OSBTreeCursor<String, Object> treeCursor;
    private final ValuesTransformer                          valuesTransformer;

    private Iterator<OIdentifiable> currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
    private Object                  currentKey      = null;

    private OSBTreeIndexCursor(OPrefixBTree.OSBTreeCursor<String, Object> treeCursor, ValuesTransformer valuesTransformer) {
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
