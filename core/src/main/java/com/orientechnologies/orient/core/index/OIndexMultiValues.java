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
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
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
import java.util.concurrent.Callable;

/**
 * Abstract index implementation that supports multi-values for the same key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexMultiValues extends OIndexAbstract<Set<OIdentifiable>> {
  public OIndexMultiValues(String name, final String type, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, final ODocument metadata) {
    super(name, type, algorithm, valueContainerAlgorithm, metadata, version, storage);
  }

  public Set<OIdentifiable> get(Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireSharedLock(key);
    try {

      acquireSharedLock();
      try {

        final Set<OIdentifiable> values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);

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
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();
    if (!txIsActive)
      keyLockManager.acquireSharedLock(key);
    try {
      acquireSharedLock();
      try {

        final Set<OIdentifiable> values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);

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

  public OIndexMultiValues put(Object key, final OIdentifiable singleValue) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);
    try {
      checkForKeyType(key);
      acquireSharedLock();

      try {
        if (!singleValue.getIdentity().isValid())
          (singleValue.getRecord()).save();

        final ORID identity = singleValue.getIdentity();

        final boolean durable;

        if (metadata != null && Boolean.TRUE.equals(metadata.field("durableInNonTxMode")))
          durable = true;
        else
          durable = false;

        final Set<OIdentifiable> values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);

        final Callable<Object> creator = new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            Set<OIdentifiable> result = values;

            if (result == null) {
              if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
                result = new OIndexRIDContainer(getName(), durable);
              } else {
                throw new IllegalStateException("MVRBTree is not supported any more");
              }
            }

            result.add(identity);

            return result;
          }
        };

        storage.updateIndexEntry(indexId, key, creator);
        return this;
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);

    try {
      acquireSharedLock();
      try {
        final Set<OIdentifiable> values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);

        if (values == null) {
          return false;
        }

        final OModifiableBoolean removed = new OModifiableBoolean(false);

        final Callable<Object> creator = new EntityRemover(value, removed, values);

        storage.updateIndexEntry(indexId, key, creator);

        return removed.getValue();

      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }

  }

  public OIndexMultiValues create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {

    return (OIndexMultiValues) super.create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener,
        determineValueSerializer());
  }

  protected OBinarySerializer determineValueSerializer() {
    if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm))
      return storage.getComponentsFactory().binarySerializerFactory
          .getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
    else
      return OStreamSerializerListRID.INSTANCE;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    checkForRebuild();

    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      return storage.iterateIndexEntriesBetween(indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder,
          MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    checkForRebuild();

    fromKey = getCollatingValue(fromKey);

    acquireSharedLock();
    try {
      return storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE);

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    checkForRebuild();

    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      return storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder)
      comparator = ODefaultComparator.INSTANCE;
    else
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    Collections.sort(sortedKeys, comparator);

    return new OIndexAbstractCursor() {
      private Iterator<?>             keysIterator    = sortedKeys.iterator();

      private Iterator<OIdentifiable> currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
      private Object                  currentKey;

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
              result = (Collection<OIdentifiable>) storage.getIndexValue(indexId, key);
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
    checkForRebuild();
    acquireSharedLock();
    try {
      return storage.getIndexSize(indexId, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }

  }

  public long getKeySize() {
    checkForRebuild();
    acquireSharedLock();
    try {
      return storage.getIndexSize(indexId, null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return storage.getIndexCursor(indexId, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return storage.getIndexDescCursor(indexId, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  private static final class MultiValuesTransformer implements OIndexEngine.ValuesTransformer {
    private static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

    @Override
    public Collection<OIdentifiable> transformFromValue(Object value) {
      return (Collection<OIdentifiable>) value;
    }
  }

  private static class EntityRemover implements Callable<Object> {
    private final OIdentifiable      value;
    private final OModifiableBoolean removed;
    private final Set<OIdentifiable> values;

    public EntityRemover(OIdentifiable value, OModifiableBoolean removed, Set<OIdentifiable> values) {
      this.value = value;
      this.removed = removed;
      this.values = values;
    }

    @Override
    public Object call() throws Exception {
      if (value == null) {
        removed.setValue(true);

        return null;
      } else if (values.remove(value)) {
        removed.setValue(true);

        if (values.isEmpty())
          return null;
        else
          return values;
      }

      return values;
    }
  }
}
