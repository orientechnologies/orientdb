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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract index implementation that supports multi-values for the same key.
 *
 * @author Luca Garulli
 */
public abstract class OIndexMultiValues extends OIndexAbstract<Set<OIdentifiable>> {
  public OIndexMultiValues(String name, final String type, String algorithm, OIndexEngine<Set<OIdentifiable>> indexEngine,
      String valueContainerAlgorithm, final ODocument metadata, OStorage storage) {
    super(name, type, algorithm, indexEngine, valueContainerAlgorithm, metadata, storage);
  }

  public Set<OIdentifiable> get(Object key) {
    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireSharedLock(key);
    try {

      acquireSharedLock();
      try {

        final Set<OIdentifiable> values = indexEngine.get(key);

        if (values == null)
          return Collections.emptySet();

        return new HashSet<OIdentifiable>(values);

      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(key);
    }
  }

  public long count(Object key) {
    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();
    if (!txIsActive)
      keyLockManager.acquireSharedLock(key);
    try {
      acquireSharedLock();
      try {

        final Set<OIdentifiable> values = indexEngine.get(key);

        if (values == null)
          return 0;

        return values.size();

      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(key);
    }

  }

  public OIndexMultiValues put(Object key, final OIdentifiable iSingleValue) {
    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);
    try {
      if (modificationLock != null)
        modificationLock.requestModificationLock();
      try {
        checkForKeyType(key);
        acquireSharedLock();
        startStorageAtomicOperation();
        try {
          Set<OIdentifiable> values = indexEngine.get(key);

          if (values == null) {
            if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
              boolean durable = false;
              if (metadata != null && Boolean.TRUE.equals(metadata.field("durableInNonTxMode")))
                durable = true;

              values = new OIndexRIDContainer(getName(), durable);
            } else {
              values = new OMVRBTreeRIDSet(OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger());
              ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
            }
          }

          if (!iSingleValue.getIdentity().isValid())
            ((ORecord) iSingleValue).save();

          values.add(iSingleValue.getIdentity());
          indexEngine.put(key, values);

          commitStorageAtomicOperation();
          return this;

        } catch (RuntimeException e) {
          rollbackStorageAtomicOperation();
          throw new OIndexException("Error during insertion of key in index", e);
        } finally {
          releaseSharedLock();
        }
      } finally {
        if (modificationLock != null)
          modificationLock.releaseModificationLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);

    try {
      if (modificationLock != null)
        modificationLock.requestModificationLock();
      try {
        acquireSharedLock();
        startStorageAtomicOperation();
        try {

          Set<OIdentifiable> values = indexEngine.get(key);

          if (values == null) {
            commitStorageAtomicOperation();
            return false;
          }

          if (value == null) {
            indexEngine.remove(key);
          } else if (values.remove(value)) {
            if (values.isEmpty())
              indexEngine.remove(key);
            else
              indexEngine.put(key, values);

            commitStorageAtomicOperation();
            return true;
          }

          commitStorageAtomicOperation();
          return false;

        } catch (RuntimeException e) {
          rollbackStorageAtomicOperation();
          throw new OIndexException("Error during removal of entry by key", e);
        } finally {
          releaseSharedLock();
        }
      } finally {
        if (modificationLock != null)
          modificationLock.releaseModificationLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }

  }

  public OIndexMultiValues create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {

    return (OIndexMultiValues) super
        .create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, determineValueSerializer());
  }

  protected OStreamSerializer determineValueSerializer() {
    if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm))
      return (OStreamSerializer) getDatabase().getSerializerFactory()
          .getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
    else
      return OStreamSerializerListRID.INSTANCE;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      return indexEngine
          .iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);

    acquireSharedLock();
    try {
      return indexEngine.iterateEntriesMajor(fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE);

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      return indexEngine.iterateEntriesMinor(toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder)
      comparator = ODefaultComparator.INSTANCE;
    else
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    Collections.sort(sortedKeys, comparator);

    return new OIndexAbstractCursor() {
      private Iterator<?> keysIterator = sortedKeys.iterator();

      private Iterator<OIdentifiable> currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
      private Object currentKey;

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (currentIterator == null)
          return null;

        Object key = null;
        if (!currentIterator.hasNext()) {
          Collection<OIdentifiable> result = null;
          while (keysIterator.hasNext() && (result == null || result.isEmpty())) {
            key = keysIterator.next();
            key = getCollatingValue(key);

            acquireSharedLock();
            try {
              result = indexEngine.get(key);
            } finally {
              releaseSharedLock();
            }
          }

          if (result == null) {
            currentIterator = null;
            return null;
          }

          currentKey = key;
          currentIterator = result.iterator();
        }

        final OIdentifiable resultValue = currentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return currentKey;
          }

          @Override
          public OIdentifiable getValue() {
            return resultValue;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  public long getSize() {
    acquireSharedLock();
    try {
      return indexEngine.size(MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }

  }

  public long getKeySize() {
    acquireSharedLock();
    try {
      return indexEngine.size(null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    acquireSharedLock();
    try {
      return indexEngine.cursor(MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor() {
    acquireSharedLock();
    try {
      return indexEngine.descCursor(MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  private static final class MultiValuesTransformer implements OIndexEngine.ValuesTransformer<Set<OIdentifiable>> {
    private static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

    @Override
    public Collection<OIdentifiable> transformFromValue(Set<OIdentifiable> value) {
      return value;
    }
  }
}
