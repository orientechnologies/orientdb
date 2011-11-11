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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;

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

		acquireExclusiveLock();
		try {

			final ORecordLazySet values = (ORecordLazySet) map.get(iKey);
			if (values != null)
				values.setDatabase(getDatabase());

			if (values == null)
				return ORecordLazySet.EMPTY_SET;

			return values;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexMultiValues put(final Object iKey, final OIdentifiable iSingleValue) {

		acquireExclusiveLock();
		try {

			checkForKeyType(iKey);

			Set<OIdentifiable> values = map.get(iKey);

			if (values == null)
				values = new ORecordLazySet(getDatabase()).setRidOnly(true);

			if (!iSingleValue.getIdentity().isValid())
				((ORecord<?>) iSingleValue).save();

			values.add(iSingleValue);

			map.put(iKey, values);
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public boolean remove(final Object iKey, final OIdentifiable iValue) {

		acquireExclusiveLock();
		try {

			final Set<OIdentifiable> recs = get(iKey);
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
	}

	public int remove(final OIdentifiable iRecord) {

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

	public int count(final OIdentifiable iRecord) {

		acquireExclusiveLock();
		try {

			Set<OIdentifiable> rids;
			int tot = 0;
			for (final Entry<Object, Set<OIdentifiable>> entries : map.entrySet()) {
				rids = entries.getValue();
				if (rids != null) {
					if (rids.contains(iRecord)) {
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
			final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener) {
		return (OIndexMultiValues) super.create(iName, indexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex,
				iProgressListener, OStreamSerializerListRID.INSTANCE);
	}

	public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
			final boolean iToInclusive, final int maxValuesToFetch) {
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
				final ORecordLazySet values = (ORecordLazySet) entry.getValue();
				values.setDatabase(getDatabase());

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
				final ORecordLazySet values = (ORecordLazySet) entry.getValue();
				values.setDatabase(getDatabase());

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
				final ORecordLazySet values = (ORecordLazySet) entry.getValue();
				values.setDatabase(getDatabase());

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Collection<OIdentifiable> getValues(final Collection<?> iKeys, final int maxValuesToFetch) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		acquireExclusiveLock();
		try {
			final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

			for (final Object key : sortedKeys) {
				final ORecordLazySet values = (ORecordLazySet) map.get(key);

				if (values == null)
					continue;

				values.setDatabase(getDatabase());

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
				final ORecordLazySet values = (ORecordLazySet) entry.getValue();
				values.setDatabase(getDatabase());

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
				final ORecordLazySet values = (ORecordLazySet) entry.getValue();
				values.setDatabase(getDatabase());

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
				final ORecordLazySet values = (ORecordLazySet) entry.getValue();
				values.setDatabase(getDatabase());

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		acquireExclusiveLock();
		try {
			final Set<ODocument> result = new ODocumentFieldsHashSet();

			for (final Object key : sortedKeys) {
				final ORecordLazySet values = (ORecordLazySet) map.get(key);

				if (values == null)
					continue;

				values.setDatabase(getDatabase());

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
}
