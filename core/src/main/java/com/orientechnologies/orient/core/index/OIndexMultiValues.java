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

import com.orientechnologies.common.collection.ONavigableMap;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.metadata.schema.OType;
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
	public OIndexMultiValues(String iType) {
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
			for (Entry<Object, Set<OIdentifiable>> entries : map.entrySet()) {
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
			for (Entry<Object, Set<OIdentifiable>> entries : map.entrySet()) {
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

	public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {

		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.tailMap(fromKey, isInclusive);
			if (subSet == null)
				return ORecordLazySet.EMPTY_SET;

			final Set<OIdentifiable> result = new ORecordLazySet(getDatabase());
			for (Set<OIdentifiable> v : subSet.values()) {
				result.addAll(v);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {

		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.headMap(toKey, isInclusive);
			if (subSet == null)
				return ORecordLazySet.EMPTY_SET;

			final Set<OIdentifiable> result = new ORecordLazySet(getDatabase());
			for (Set<OIdentifiable> v : subSet.values()) {
				result.addAll(v);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {

		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.tailMap(fromKey, isInclusive);
			if (subSet != null) {
				for (Entry<Object, Set<OIdentifiable>> v : subSet.entrySet()) {
					for (OIdentifiable id : v.getValue()) {
						final ODocument document = new ODocument();
						document.field("key", v.getKey());
						document.field("rid", id.getIdentity());
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

	public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {

		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.headMap(toKey, isInclusive);
			if (subSet != null) {
				for (Entry<Object, Set<OIdentifiable>> v : subSet.entrySet()) {
					for (OIdentifiable id : v.getValue()) {
						final ODocument document = new ODocument();
						document.field("key", v.getKey());
						document.field("rid", id.getIdentity());
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

    /**
     * Returns a set of records with key between the range passed as parameter.
     * <p/>
     * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used
     * as values boundaries.
     *
     * @param iRangeFrom     Starting range
     * @param iFromInclusive Indicates whether start range boundary is included in result.
     * @param iRangeTo       Ending range
     * @param iToInclusive   Indicates whether end range boundary is included in result.
     * @return Returns a set of records with key between the range passed as parameter.
     * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
     */
    public Set<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive,
                                               Object iRangeTo, boolean iToInclusive) {
        if (iRangeFrom.getClass() != iRangeTo.getClass())
            throw new IllegalArgumentException("Range from-to parameters are of different types");

		acquireExclusiveLock();

        try {
            final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.subMap(iRangeFrom, iFromInclusive,
                    iRangeTo, iToInclusive);

            if (subSet == null)
                return ORecordLazySet.EMPTY_SET;

            final Set<OIdentifiable> result = new ORecordLazySet(getDatabase());
            for (Set<OIdentifiable> v : subSet.values()) {
                result.addAll(v);
            }

            return result;

        } finally {
            releaseExclusiveLock();
        }
    }


    public Set<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.subMap(iRangeFrom, iInclusive, iRangeTo, iInclusive);
			if (subSet != null) {
				for (Entry<Object, Set<OIdentifiable>> v : subSet.entrySet()) {
					for (OIdentifiable id : v.getValue()) {
						final ODocument document = new ODocument();
						document.field("key", v.getKey());
						document.field("rid", id.getIdentity());
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

	public OIndexMultiValues create(String iName, OIndexDefinition indexDefinition, ODatabaseRecord iDatabase, String iClusterIndexName,
                                    int[] iClusterIdsToIndex, OProgressListener iProgressListener) {
		return (OIndexMultiValues) super.create(iName, indexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex, iProgressListener,
				OStreamSerializerListRID.INSTANCE);
	}

	public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
		acquireExclusiveLock();
		try {
			for (final Object key : sortedKeys) {
				final ORecordLazySet values = (ORecordLazySet) map.get(key);
				if (values != null)
					values.setDatabase(getDatabase());

				if (values == null)
					continue;

				result.addAll(values);
			}
		} finally {
			releaseExclusiveLock();
		}
		return result;
	}

	public Collection<ODocument> getEntries(final Collection<?> iKeys) {
		final List<Comparable> sortedKeys = new ArrayList<Comparable>((Collection<? extends Comparable>) iKeys);
		Collections.sort(sortedKeys);

		final Set<ODocument> result = new HashSet<ODocument>();
		acquireExclusiveLock();
		try {
			for (final Object key : sortedKeys) {
				final ORecordLazySet values = (ORecordLazySet) map.get(key);
				if (values != null)
					values.setDatabase(getDatabase());

				if (values == null)
					continue;
				for (final OIdentifiable value : values) {
					final ODocument document = new ODocument();
					document.field("key", key);
					document.field("rid", value.getIdentity());
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
