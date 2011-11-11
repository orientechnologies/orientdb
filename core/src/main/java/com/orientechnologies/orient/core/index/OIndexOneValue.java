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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
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

	public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
			final boolean iToInclusive, final int maxValuesToFetch) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		acquireExclusiveLock();

		try {
			final OMVRBTreeEntry<Object, OIdentifiable> firstEntry;

			if (iFromInclusive)
				firstEntry = map.getCeilingEntry(iRangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
			else
				firstEntry = map.getHigherEntry(iRangeFrom);

			if (firstEntry == null)
				return Collections.emptySet();

			final int firstEntryIndex = map.getPageIndex();

			final OMVRBTreeEntry<Object, OIdentifiable> lastEntry;

			if (iToInclusive)
				lastEntry = map.getHigherEntry(iRangeTo);
			else
				lastEntry = map.getCeilingEntry(iRangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

			final int lastEntryIndex;

			if (lastEntry != null)
				lastEntryIndex = map.getPageIndex();
			else
				lastEntryIndex = -1;

			OMVRBTreeEntry<Object, OIdentifiable> entry = firstEntry;
			map.setPageIndex(firstEntryIndex);

			final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

			while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)
					&& !(maxValuesToFetch > -1 && result.size() == maxValuesToFetch)) {
				result.add(entry.getValue());

				entry = OMVRBTree.next(entry);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive, final int maxValuesToFetch) {
		acquireExclusiveLock();

		try {
			final OMVRBTreeEntry<Object, OIdentifiable> firstEntry;
			if (isInclusive)
				firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
			else
				firstEntry = map.getHigherEntry(fromKey);

			if (firstEntry == null)
				return Collections.emptySet();

			OMVRBTreeEntry<Object, OIdentifiable> entry = firstEntry;

			final HashSet<OIdentifiable> result = new HashSet<OIdentifiable>();

			while (entry != null && !(maxValuesToFetch > -1 && result.size() == maxValuesToFetch)) {
				result.add(entry.getValue());
				entry = OMVRBTree.next(entry);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive, final int maxValuesToFetch) {
		acquireExclusiveLock();

		try {

			final OMVRBTreeEntry<Object, OIdentifiable> lastEntry;

			if (isInclusive)
				lastEntry = map.getFloorEntry(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
			else
				lastEntry = map.getLowerEntry(toKey);

			if (lastEntry == null)
				return Collections.emptySet();

			OMVRBTreeEntry<Object, OIdentifiable> entry = lastEntry;

			final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

			while (entry != null && !(maxValuesToFetch > -1 && result.size() == maxValuesToFetch)) {
				result.add(entry.getValue());

				entry = OMVRBTree.previous(entry);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OIdentifiable> getValues(final Collection<?> iKeys, final int maxValuesToSearch) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		acquireExclusiveLock();

		final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
		try {
			for (final Object key : sortedKeys) {
				if (maxValuesToSearch > -1 && result.size() == maxValuesToSearch)
					return result;

				final OIdentifiable val = map.get(key);
				if (val != null) {
					result.add(val);
				}
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive, final int maxEntriesToFetch) {
		acquireExclusiveLock();

		try {
			final OMVRBTreeEntry<Object, OIdentifiable> firstEntry;
			if (isInclusive)
				firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
			else
				firstEntry = map.getHigherEntry(fromKey);

			if (firstEntry == null)
				return Collections.emptySet();

			OMVRBTreeEntry<Object, OIdentifiable> entry = firstEntry;

			final Set<ODocument> result = new ODocumentFieldsHashSet();

			while (entry != null && !(maxEntriesToFetch > -1 && result.size() != maxEntriesToFetch)) {
				final ODocument document = new ODocument();
				document.field("key", entry.getKey());
				document.field("rid", entry.getValue().getIdentity());
				document.unsetDirty();

				result.add(document);

				entry = OMVRBTree.next(entry);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}

	}

	public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive, final int maxEntriesToFetch) {
		acquireExclusiveLock();

		try {

			final OMVRBTreeEntry<Object, OIdentifiable> lastEntry;

			if (isInclusive)
				lastEntry = map.getFloorEntry(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
			else
				lastEntry = map.getLowerEntry(toKey);

			if (lastEntry == null)
				return Collections.emptySet();

			OMVRBTreeEntry<Object, OIdentifiable> entry = lastEntry;

			final Set<ODocument> result = new ODocumentFieldsHashSet();

			while (entry != null && !(maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)) {
				final ODocument document = new ODocument();
				document.field("key", entry.getKey());
				document.field("rid", entry.getValue().getIdentity());
				document.unsetDirty();

				result.add(document);

				entry = OMVRBTree.previous(entry);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}

	}

	public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive,
			final int maxEntriesToFetch) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		acquireExclusiveLock();

		try {
			final OMVRBTreeEntry<Object, OIdentifiable> firstEntry;

			if (iInclusive)
				firstEntry = map.getCeilingEntry(iRangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
			else
				firstEntry = map.getHigherEntry(iRangeFrom);

			if (firstEntry == null)
				return Collections.emptySet();

			final int firstEntryIndex = map.getPageIndex();

			final OMVRBTreeEntry<Object, OIdentifiable> lastEntry;

			if (iInclusive)
				lastEntry = map.getHigherEntry(iRangeTo);
			else
				lastEntry = map.getCeilingEntry(iRangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

			final int lastEntryIndex;

			if (lastEntry != null)
				lastEntryIndex = map.getPageIndex();
			else
				lastEntryIndex = -1;

			OMVRBTreeEntry<Object, OIdentifiable> entry = firstEntry;
			map.setPageIndex(firstEntryIndex);

			final Set<ODocument> result = new ODocumentFieldsHashSet();

			while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)
					&& !(maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)) {

				final ODocument document = new ODocument();
				document.field("key", entry.getKey());
				document.field("rid", entry.getValue().getIdentity());
				document.unsetDirty();

				result.add(document);

				entry = OMVRBTree.next(entry);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}

	}

	public Collection<ODocument> getEntries(final Collection<?> iKeys, final int maxEntriesToFetch) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		acquireExclusiveLock();

		final Set<ODocument> result = new ODocumentFieldsHashSet();
		try {
			for (final Object key : sortedKeys) {
				if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
					return result;

				final OIdentifiable val = map.get(key);
				if (val != null) {
					final ODocument document = new ODocument();
					document.field("key", key);
					document.field("rid", val.getIdentity());
					document.unsetDirty();

					result.add(document);
				}
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}

	}
}
