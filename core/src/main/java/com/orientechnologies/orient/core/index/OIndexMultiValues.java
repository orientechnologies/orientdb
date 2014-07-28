/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Abstract index implementation that supports multi-values for the same key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexMultiValues extends OIndexAbstract<Set<OIdentifiable>> {
  public OIndexMultiValues(final String type, String algorithm, OIndexEngine<Set<OIdentifiable>> indexEngine,
      String valueContainerAlgorithm) {
    super(type, algorithm, indexEngine, valueContainerAlgorithm);
  }

  public Set<OIdentifiable> get(Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    acquireSharedLock();
    try {

      final Set<OIdentifiable> values = indexEngine.get(key);

      if (values == null)
        return Collections.emptySet();

      return new HashSet<OIdentifiable>(values);

    } finally {
      releaseSharedLock();
    }
  }

  public long count(Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    acquireSharedLock();
    try {

      final Set<OIdentifiable> values = indexEngine.get(key);

      if (values == null)
        return 0;

      return values.size();

    } finally {
      releaseSharedLock();
    }
  }

  public OIndexMultiValues put(Object key, final OIdentifiable iSingleValue) {
    checkForRebuild();

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();
    try {
      checkForKeyType(key);
      acquireExclusiveLock();
      try {
        Set<OIdentifiable> values = indexEngine.get(key);

        if (values == null) {
          if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
            values = new OIndexRIDContainer(getName());
          } else {
            values = new OMVRBTreeRIDSet(OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger());
            ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
          }
        }

        if (!iSingleValue.getIdentity().isValid())
          ((ORecord<?>) iSingleValue).save();

        values.add(iSingleValue.getIdentity());
        indexEngine.put(key, values);

        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  protected void putInSnapshot(Object key, OIdentifiable value, final Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    Object snapshotValue = snapshot.get(key);

    Set<OIdentifiable> values;
    if (snapshotValue == null)
      values = indexEngine.get(key);
    else if (snapshotValue.equals(RemovedValue.INSTANCE))
      values = null;
    else
      values = (Set<OIdentifiable>) snapshotValue;

    if (values == null) {
      if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
        values = new OIndexRIDContainer(getName());
      } else {
        values = new OMVRBTreeRIDSet(OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger());
        ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
      }

      snapshot.put(key, values);
    }

    values.add(value.getIdentity());
    snapshot.put(key, values);
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    checkForRebuild();

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        Set<OIdentifiable> values = indexEngine.get(key);

        if (values == null)
          return false;

        if (values.remove(value)) {
          if (values.isEmpty())
            indexEngine.remove(key);
          else
            indexEngine.put(key, values);
          return true;
        }

        return false;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  protected void removeFromSnapshot(Object key, final OIdentifiable value, final Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    final Object snapshotValue = snapshot.get(key);

    Set<OIdentifiable> values;
    if (snapshotValue == null)
      values = indexEngine.get(key);
    else if (snapshotValue.equals(RemovedValue.INSTANCE))
      values = null;
    else
      values = (Set<OIdentifiable>) snapshotValue;

    if (values == null)
      return;

    if (values.remove(value)) {
      if (values.isEmpty())
        snapshot.put(key, RemovedValue.INSTANCE);
      else
        snapshot.put(key, values);
    }
  }

  @Override
  protected void commitSnapshot(Map<Object, Object> snapshot) {
    for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
      Object key = snapshotEntry.getKey();
      Object value = snapshotEntry.getValue();
      checkForKeyType(key);

      if (value.equals(RemovedValue.INSTANCE))
        indexEngine.remove(key);
      else
        indexEngine.put(key, (Set<OIdentifiable>) value);
    }
  }

  public OIndexMultiValues create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {

    return (OIndexMultiValues) super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener,
        determineValueSerializer());
  }

  protected OStreamSerializer determineValueSerializer() {
    if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm))
      return OStreamSerializerSBTreeIndexRIDContainer.INSTANCE;
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
      return indexEngine.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder,
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
      return indexEngine.iterateEntriesMajor(fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE);

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
      return indexEngine.iterateEntriesMinor(toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE);
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
    checkForRebuild();
    acquireSharedLock();
    try {
      return indexEngine.size(MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }

  }

  public long getKeySize() {
    checkForRebuild();
    acquireSharedLock();
    try {
      return indexEngine.size(null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.cursor(MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

	@Override
	public OIndexCursor descCursor() {
		checkForRebuild();

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
