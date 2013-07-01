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
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.resource.OSharedResourceIterator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
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
public abstract class OIndexMultiValues extends OIndexMVRBTreeAbstract<Set<OIdentifiable>> {
  public OIndexMultiValues(final String iType) {
    super(iType);
  }

  public Set<OIdentifiable> get(final Object iKey) {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) map.get(iKey);

      if (values == null)
        return Collections.emptySet();

      return new HashSet<OIdentifiable>(values);

    } finally {
      releaseExclusiveLock();
    }
  }

  public long count(final Object iKey) {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) map.get(iKey);

      if (values == null)
        return 0;

      return values.size();

    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexMultiValues put(final Object iKey, final OIdentifiable iSingleValue) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        checkForKeyType(iKey);

        Set<OIdentifiable> values = map.get(iKey);

        if (values == null) {
          values = new OMVRBTreeRIDSet();
          ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
        }

        if (!iSingleValue.getIdentity().isValid())
          ((ORecord<?>) iSingleValue).save();

        values.add(iSingleValue.getIdentity());

        map.put(iKey, values);
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public boolean remove(final Object iKey, final OIdentifiable iValue) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        Set<OIdentifiable> recs = map.get(iKey);

        if (recs == null)
          return false;

        if (recs.remove(iValue)) {
          if (recs.isEmpty())
            map.remove(iKey);
          else
            map.put(iKey, recs);
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

  public int remove(final OIdentifiable iRecord) {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      int tot = 0;
      Set<OIdentifiable> rids;
      for (final Entry<Object, Set<OIdentifiable>> entries : map.entrySet()) {
        rids = entries.getValue();
        if (rids != null) {
          if (rids.contains(iRecord)) {
            remove(entries.getKey(), iRecord);
            ++tot;
          }
        }
      }

      return tot;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexMultiValues create(final String iName, final OIndexDefinition indexDefinition, final ODatabaseRecord iDatabase,
      final String iClusterIndexName, final int[] iClusterIdsToIndex, boolean rebuild, final OProgressListener iProgressListener) {
    return (OIndexMultiValues) super.create(iName, indexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex, rebuild,
        iProgressListener, OStreamSerializerListRID.INSTANCE);
  }

  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive, final int maxValuesToFetch) {
    checkForRebuild();

    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, Set<OIdentifiable>> firstEntry;

      if (iFromInclusive)
        firstEntry = map.getCeilingEntry(iRangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(iRangeFrom);

      if (firstEntry == null)
        return Collections.emptySet();

      final int firstEntryIndex = map.getPageIndex();

      final OMVRBTreeEntry<Object, Set<OIdentifiable>> lastEntry;

      if (iToInclusive)
        lastEntry = map.getHigherEntry(iRangeTo);
      else
        lastEntry = map.getCeilingEntry(iRangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      final int lastEntryIndex;

      if (lastEntry != null)
        lastEntryIndex = map.getPageIndex();
      else
        lastEntryIndex = -1;

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = firstEntry;
      map.setPageIndex(firstEntryIndex);

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();
        if (values.isEmpty())
          continue;

        for (final OIdentifiable value : values) {
          if (maxValuesToFetch > -1 && maxValuesToFetch == result.size())
            return result;

          result.add(value);
        }

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive, final int maxValuesToFetch) {
    checkForRebuild();

    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, Set<OIdentifiable>> firstEntry;
      if (isInclusive)
        firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(fromKey);

      if (firstEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = firstEntry;

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      while (entry != null) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();

        if (values.isEmpty())
          continue;

        for (final OIdentifiable value : values) {
          if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
            return result;

          result.add(value);
        }

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive, final int maxValuesToFetch) {
    checkForRebuild();

    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, Set<OIdentifiable>> lastEntry;

      if (isInclusive)
        lastEntry = map.getFloorEntry(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
      else
        lastEntry = map.getLowerEntry(toKey);

      if (lastEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = lastEntry;

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      while (entry != null) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();
        if (values.isEmpty())
          continue;

        for (final OIdentifiable value : values) {
          if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
            return result;

          result.add(value);
        }

        entry = OMVRBTree.previous(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys, final int maxValuesToFetch) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(iKeys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireExclusiveLock();
    try {
      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      for (final Object key : sortedKeys) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) map.get(key);

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
      releaseExclusiveLock();
    }
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive, final int maxEntriesToFetch) {
    checkForRebuild();

    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, Set<OIdentifiable>> firstEntry;
      if (isInclusive)
        firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(fromKey);

      if (firstEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = firstEntry;

      final Set<ODocument> result = new HashSet<ODocument>();

      while (entry != null) {
        final Object key = entry.getKey();
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();

        if (values.isEmpty())
          continue;

        for (final OIdentifiable value : values) {
          if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
            return result;

          final ODocument document = new ODocument();
          document.field("key", key);
          document.field("rid", value.getIdentity());
          document.unsetDirty();

          result.add(document);
        }

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch) {
    checkForRebuild();

    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, Set<OIdentifiable>> lastEntry;

      if (isInclusive)
        lastEntry = map.getFloorEntry(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
      else
        lastEntry = map.getLowerEntry(toKey);

      if (lastEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = lastEntry;

      final Set<ODocument> result = new ODocumentFieldsHashSet();

      while (entry != null) {
        final Object key = entry.getKey();
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();

        if (values.isEmpty())
          continue;

        for (final OIdentifiable value : values) {
          if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
            return result;

          final ODocument document = new ODocument();
          document.field("key", key);
          document.field("rid", value.getIdentity());
          document.unsetDirty();

          result.add(document);
        }

        entry = OMVRBTree.previous(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch) {
    checkForRebuild();

    final OType[] types = getDefinition().getTypes();
    if (types.length == 1) {
      iRangeFrom = OType.convert(iRangeFrom, types[0].getDefaultJavaType());
      iRangeTo = OType.convert(iRangeTo, types[0].getDefaultJavaType());
    }

    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, Set<OIdentifiable>> firstEntry;

      if (iInclusive)
        firstEntry = map.getCeilingEntry(iRangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(iRangeFrom);

      if (firstEntry == null)
        return Collections.emptySet();

      final int firstEntryIndex = map.getPageIndex();

      final OMVRBTreeEntry<Object, Set<OIdentifiable>> lastEntry;

      if (iInclusive)
        lastEntry = map.getHigherEntry(iRangeTo);
      else
        lastEntry = map.getCeilingEntry(iRangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      final int lastEntryIndex;

      if (lastEntry != null)
        lastEntryIndex = map.getPageIndex();
      else
        lastEntryIndex = -1;

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = firstEntry;
      map.setPageIndex(firstEntryIndex);

      final Set<ODocument> result = new ODocumentFieldsHashSet();

      while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)) {
        final Object key = entry.getKey();
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();

        if (values.isEmpty())
          continue;

        for (final OIdentifiable value : values) {
          if (maxEntriesToFetch > -1 && maxEntriesToFetch == result.size())
            return result;

          final ODocument document = new ODocument();
          document.field("key", key);
          document.field("rid", value.getIdentity());
          document.unsetDirty();

          result.add(document);
        }

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }

  }

  public long count(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo, final boolean iToInclusive,
      final int maxValuesToFetch) {
    checkForRebuild();

    if (iRangeFrom != null && iRangeTo != null && iRangeFrom.getClass() != iRangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireExclusiveLock();
    try {

      final OMVRBTreeEntry<Object, Set<OIdentifiable>> firstEntry;

      if (iRangeFrom == null)
        firstEntry = (OMVRBTreeEntry<Object, Set<OIdentifiable>>) map.firstEntry();
      else if (iFromInclusive)
        firstEntry = map.getCeilingEntry(iRangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(iRangeFrom);

      if (firstEntry == null)
        return 0;

      long count = 0;
      final int firstEntryIndex = map.getPageIndex();

      final OMVRBTreeEntry<Object, Set<OIdentifiable>> lastEntry;

      if (iRangeFrom == null)
        lastEntry = (OMVRBTreeEntry<Object, Set<OIdentifiable>>) map.lastEntry();
      else if (iToInclusive)
        lastEntry = map.getHigherEntry(iRangeTo);
      else
        lastEntry = map.getCeilingEntry(iRangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      final int lastEntryIndex;

      if (lastEntry != null)
        lastEntryIndex = map.getPageIndex();
      else
        lastEntryIndex = -1;

      OMVRBTreeEntry<Object, Set<OIdentifiable>> entry = firstEntry;
      map.setPageIndex(firstEntryIndex);

      while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) entry.getValue();
        if (values.isEmpty())
          continue;

        count += values.size();

        if (maxValuesToFetch > -1 && maxValuesToFetch == count)
          return maxValuesToFetch;

        entry = OMVRBTree.next(entry);
      }

      return count;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(iKeys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireExclusiveLock();
    try {
      final Set<ODocument> result = new ODocumentFieldsHashSet();

      for (final Object key : sortedKeys) {
        final OMVRBTreeRIDSet values = (OMVRBTreeRIDSet) map.get(key);

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
      releaseExclusiveLock();
    }
  }

  public long getSize() {
    checkForRebuild();

    acquireExclusiveLock();
    try {
      if (map.size() == 0)
        return 0;

      OMVRBTreeEntry<Object, Set<OIdentifiable>> rootEntry = map.getRoot();
      long size = 0;

      OMVRBTreeEntry<Object, Set<OIdentifiable>> currentEntry = rootEntry;
      map.setPageIndex(0);

      while (currentEntry != null) {
        size += currentEntry.getValue().size();
        currentEntry = OMVRBTree.next(currentEntry);
      }

      map.setPageIndex(0);
      currentEntry = OMVRBTree.previous(rootEntry);

      while (currentEntry != null) {
        size += currentEntry.getValue().size();
        currentEntry = OMVRBTree.previous(currentEntry);
      }

      return size;
    } finally {
      releaseExclusiveLock();
    }
  }

  public long getKeySize() {
    checkForRebuild();

    acquireExclusiveLock();
    try {
      return map.size();
    } finally {
      releaseExclusiveLock();
    }
  }

  public Iterator<OIdentifiable> valuesIterator() {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      return new OSharedResourceIterator<OIdentifiable>(this, new OMultiCollectionIterator<OIdentifiable>(map.values().iterator()));

    } finally {
      releaseExclusiveLock();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Iterator<OIdentifiable> valuesInverseIterator() {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      return new OSharedResourceIterator(this, new OMultiCollectionIterator<OIdentifiable>(
          ((OMVRBTree.Values) map.values()).inverseIterator()));

    } finally {
      releaseExclusiveLock();
    }
  }

}
