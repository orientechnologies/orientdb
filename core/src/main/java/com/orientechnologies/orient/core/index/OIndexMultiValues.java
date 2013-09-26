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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.resource.OSharedResourceIterator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Abstract index implementation that supports multi-values for the same key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexMultiValues extends OIndexAbstract<Set<OIdentifiable>> {
  public OIndexMultiValues(final String type, OIndexEngine<Set<OIdentifiable>> indexEngine) {
    super(type, indexEngine);
  }

  public Set<OIdentifiable> get(final Object key) {
    checkForRebuild();

    acquireSharedLock();
    try {

      final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) indexEngine.get(key);

      if (values == null)
        return Collections.emptySet();

      return new HashSet<OIdentifiable>(values);

    } finally {
      releaseSharedLock();
    }
  }

  public long count(final Object key) {
    checkForRebuild();

    acquireSharedLock();
    try {

      final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) indexEngine.get(key);

      if (values == null)
        return 0;

      return values.size();

    } finally {
      releaseSharedLock();
    }
  }

  public OIndexMultiValues put(final Object key, final OIdentifiable iSingleValue) {
    checkForRebuild();

    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        checkForKeyType(key);
        Set<OIdentifiable> values = indexEngine.get(key);

        if (values == null) {
          values = new OMVRBTreeRIDSet(OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger());
          ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
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

  public int remove(final OIdentifiable iRecord) {
    checkForRebuild();

    acquireExclusiveLock();
    try {
      return indexEngine.removeValue(iRecord, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean remove(final Object key, final OIdentifiable value) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        Set<OIdentifiable> recs = indexEngine.get(key);

        if (recs == null)
          return false;

        if (recs.remove(value)) {
          if (recs.isEmpty())
            indexEngine.remove(key);
          else
            indexEngine.put(key, recs);
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

  public OIndexMultiValues create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return (OIndexMultiValues) super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener,
        OStreamSerializerListRID.INSTANCE);
  }

  public Collection<OIdentifiable> getValuesBetween(final Object rangeFrom, final boolean fromInclusive, final Object rangeTo,
      final boolean toInclusive, final int maxValuesToFetch) {
    checkForRebuild();
    acquireSharedLock();
    try {
      return indexEngine.getValuesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, maxValuesToFetch,
          MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive, final int maxValuesToFetch) {
    checkForRebuild();
    acquireSharedLock();
    try {
      return indexEngine.getValuesMajor(fromKey, isInclusive, maxValuesToFetch, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive, final int maxValuesToFetch) {
    checkForRebuild();
    acquireSharedLock();
    try {
      return indexEngine.getValuesMinor(toKey, isInclusive, maxValuesToFetch, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys, final int maxValuesToFetch) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(iKeys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireSharedLock();
    try {
      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      for (final Object key : sortedKeys) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) indexEngine.get(key);

        if (values == null)
          continue;

        if (!values.isEmpty()) {
          for (final OIdentifiable value : values) {
            if (maxValuesToFetch > -1 && maxValuesToFetch == result.size())
              return result;

            result.add(value);
          }
        }
      }

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive, final int maxEntriesToFetch) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.getEntriesMajor(fromKey, isInclusive, maxEntriesToFetch, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.getEntriesMinor(toKey, isInclusive, maxEntriesToFetch, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntriesBetween(Object rangeFrom, Object rangeTo, boolean inclusive, int maxEntriesToFetch) {
    checkForRebuild();

    final OType[] types = getDefinition().getTypes();
    if (types.length == 1) {
      rangeFrom = OType.convert(rangeFrom, types[0].getDefaultJavaType());
      rangeTo = OType.convert(rangeTo, types[0].getDefaultJavaType());
    }

    acquireSharedLock();
    try {
      return indexEngine.getEntriesBetween(rangeFrom, rangeTo, inclusive, maxEntriesToFetch, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }

  }

  public long count(Object rangeFrom, final boolean fromInclusive, Object rangeTo, final boolean toInclusive,
      final int maxValuesToFetch) {
    checkForRebuild();

    final OType[] types = getDefinition().getTypes();
    if (types.length == 1) {
      rangeFrom = OType.convert(rangeFrom, types[0].getDefaultJavaType());
      rangeTo = OType.convert(rangeTo, types[0].getDefaultJavaType());
    }

    if (rangeFrom != null && rangeTo != null && rangeFrom.getClass() != rangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireSharedLock();
    try {
      return indexEngine.count(rangeFrom, fromInclusive, rangeTo, toInclusive, maxValuesToFetch, MultiValuesTransformer.INSTANCE);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(iKeys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireSharedLock();
    try {
      final Set<ODocument> result = new ODocumentFieldsHashSet();

      for (final Object key : sortedKeys) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) indexEngine.get(key);

        if (values == null)
          continue;

        if (!values.isEmpty()) {
          for (final OIdentifiable value : values) {
            if (maxEntriesToFetch > -1 && maxEntriesToFetch == result.size())
              return result;

            final ODocument document = new ODocument();
            document.field("key", key);
            document.field("rid", value.getIdentity());
            document.unsetDirty();

            result.add(document);
          }
        }
      }

      return result;
    } finally {
      releaseSharedLock();
    }
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

  public Iterator<OIdentifiable> valuesIterator() {
    checkForRebuild();
    acquireSharedLock();
    try {

      return new OSharedResourceIterator<OIdentifiable>(this, new OMultiCollectionIterator<OIdentifiable>(
          indexEngine.valuesIterator()));

    } finally {
      releaseSharedLock();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Iterator<OIdentifiable> valuesInverseIterator() {
    checkForRebuild();
    acquireSharedLock();
    try {

      return new OSharedResourceIterator(this, new OMultiCollectionIterator<OIdentifiable>(indexEngine.inverseValuesIterator()));

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

    @Override
    public Set<OIdentifiable> transformToValue(Collection<OIdentifiable> collection) {
      return (Set<OIdentifiable>) collection;
    }
  }
}
