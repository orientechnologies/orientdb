/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.*;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.collection.ONavigableMap;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;

/**
 * Abstract Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexOneValue extends OIndexMVRBTreeAbstract<OIdentifiable> {
	public OIndexOneValue(final String iType) {
		super(iType);
	}

	public OIdentifiable get(final Object iKey) {

		acquireExclusiveLock();
		try {

			return map.get(iKey);

		} finally {
			releaseExclusiveLock();
		}
	}

	public int remove(final OIdentifiable iRecord) {

		acquireExclusiveLock();
		try {

			int tot = 0;
			for (final Entry<Object, OIdentifiable> entries : map.entrySet()) {
				if (entries.getValue().equals(iRecord)) {
					remove(entries.getKey(), iRecord);
					++tot;
				}
			}

			return tot;
		} finally {
			releaseExclusiveLock();
		}
	}

	public int count(final OIdentifiable iRecord) {

		acquireExclusiveLock();
		try {

			int tot = 0;
			for (final Entry<Object, OIdentifiable> entries : map.entrySet()) {
				if (entries.getValue().equals((iRecord)))
					++tot;
			}

			return tot;

		} finally {
			releaseExclusiveLock();
		}
	}

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive) {

    acquireExclusiveLock();

    try {

      OMVRBTreeEntry<Object, OIdentifiable> firstEntry;
      if(isInclusive)
         firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
          firstEntry = map.getHigherEntry(fromKey);

      if (firstEntry == null)
        return Collections.emptySet();

      int firstPageIndex = map.getPageIndex();
      int size = 0;

      OMVRBTreeEntry<Object, OIdentifiable> entry = firstEntry;

      while (entry != null) {
        size += entry.getSize();
        entry = OMVRBTree.successor(entry);
      }

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>(size);

      entry = firstEntry;
      map.setPageIndex(firstPageIndex);

      while (entry != null) {
        result.add(entry.getValue(map.getPageIndex()));
        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive) {

		acquireExclusiveLock();

		try {
      final ONavigableMap<Object, OIdentifiable>  headMap = map.headMap(toKey, isInclusive);
      if(headMap == null)
        return Collections.emptySet();

			return new HashSet<OIdentifiable>(headMap.values());
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive) {

		acquireExclusiveLock();

		try {
			final Set<ODocument> result;

			final ONavigableMap<Object, OIdentifiable> subSet = map.tailMap(fromKey, isInclusive);
			if (subSet != null) {
        result = new ODocumentFieldsHashSet();

				for (final Entry<Object, OIdentifiable> v : subSet.entrySet()) {
					final ODocument document = new ODocument();
					document.field("key", v.getKey());
					document.field("rid", v.getValue().getIdentity());
					document.unsetDirty();
					result.add(document);
				}
			} else
        result = Collections.emptySet();

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive) {

		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new ODocumentFieldsHashSet();

			final ONavigableMap<Object, OIdentifiable> subSet = map.headMap(toKey, isInclusive);
			if (subSet != null) {
				for (final Entry<Object, OIdentifiable> v : subSet.entrySet()) {
					final ODocument document = new ODocument();
					document.field("key", v.getKey());
					document.field("rid", v.getValue().getIdentity());
					document.unsetDirty();
					result.add(document);
				}
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of records with key between the range passed as parameter.
	 * <p/>
	 * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iFromInclusive
	 *          Indicates whether start range boundary is included in result.
	 * @param iRangeTo
	 *          Ending range
	 * @param iToInclusive
	 *          Indicates whether end range boundary is included in result.
	 * @return Returns a set of records with key between the range passed as parameter.
	 * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
	 */
	public Set<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
			final boolean iToInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, OIdentifiable> subMap = map.subMap(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);

      if(subMap == null)
        return Collections.emptySet();

			return new HashSet<OIdentifiable>(subMap.values());

		} finally {
			releaseExclusiveLock();
		}
	}

	public Set<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new ODocumentFieldsHashSet();

			final ONavigableMap<Object, OIdentifiable> subSet = map.subMap(iRangeFrom, iInclusive, iRangeTo, iInclusive);
			if (subSet != null) {
				for (final Entry<Object, OIdentifiable> v : subSet.entrySet()) {
					final ODocument document = new ODocument();
					document.field("key", v.getKey());
					document.field("rid", v.getValue().getIdentity());
					document.unsetDirty();
					result.add(document);
				}
			}

			return result;

		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
		// CHECK IF ALREADY EXIST
		final OIdentifiable indexedRID = get(iKey);
		if (indexedRID != null && !indexedRID.getIdentity().equals(iRecord.getIdentity()))
			OLogManager.instance().exception("Found duplicated key '%s' previously assigned to the record %s", null,
					OIndexException.class, iKey, indexedRID);
	}

	public OIndexOneValue create(final String iName, final OIndexDefinition iIndexDefinition, final ODatabaseRecord iDatabase,
			final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener) {
		return (OIndexOneValue) super.create(iName, iIndexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex,
				iProgressListener, OStreamSerializerRID.INSTANCE);
	}


	public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
		acquireExclusiveLock();
		try {
			for (final Object key : sortedKeys) {
				final OIdentifiable val = map.get(key);
				if (val != null)
					result.add(val);
			}
		} finally {
			releaseExclusiveLock();
		}
		return result;
	}

	public Collection<ODocument> getEntries(final Collection<?> iKeys) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		final Set<ODocument> result = new ODocumentFieldsHashSet();
		acquireExclusiveLock();
		try {
			for (final Object key : sortedKeys) {
				final OIdentifiable val = map.get(key);
				if (val != null) {
					final ODocument document = new ODocument();
					document.field("key", key);
					document.field("rid", val.getIdentity());
					document.unsetDirty();
					result.add(document);
				}
			}
		} finally {
			releaseExclusiveLock();
		}
		return result;
	}
}
