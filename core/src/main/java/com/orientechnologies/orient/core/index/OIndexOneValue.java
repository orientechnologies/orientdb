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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.ONavigableMap;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;

/**
 * Abstract Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexOneValue extends OIndexMVRBTreeAbstract<OIdentifiable> {
	public OIndexOneValue(String iType) {
		super(iType);
	}

	public OIdentifiable get(final Object iKey) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			return map.get(iKey);

		} finally {
			releaseExclusiveLock();
		}
	}

	public int remove(final OIdentifiable iRecord) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			int tot = 0;
			for (Entry<Object, OIdentifiable> entries : map.entrySet()) {
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
		checkForOptimization();
		acquireExclusiveLock();
		try {

			int tot = 0;
			for (Entry<Object, OIdentifiable> entries : map.entrySet()) {
				if (entries.getValue().equals((iRecord)))
					++tot;
			}

			return tot;

		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			return getLazySet(map.tailMap(fromKey, isInclusive));
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			return getLazySet(map.headMap(toKey, isInclusive));
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, OIdentifiable> subSet = map.tailMap(fromKey, isInclusive);
			if (subSet != null) {
				for (Entry<Object, OIdentifiable> v : subSet.entrySet()) {
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

	public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, OIdentifiable> subSet = map.headMap(toKey, isInclusive);
			if (subSet != null) {
				for (Entry<Object, OIdentifiable> v : subSet.entrySet()) {
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

	public Set<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		checkForOptimization();
		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, OIdentifiable> subSet = map.subMap(iRangeFrom, iInclusive, iRangeTo, iInclusive);
			final Set<OIdentifiable> result = getLazySet(subSet);

			return result;

		} finally {
			releaseExclusiveLock();
		}
	}

	public Set<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		checkForOptimization();
		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, OIdentifiable> subSet = map.subMap(iRangeFrom, iInclusive, iRangeTo, iInclusive);
			if (subSet != null) {
				for (Entry<Object, OIdentifiable> v : subSet.entrySet()) {
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

	public OIndexOneValue create(String iName, OType iKeyType, ODatabaseRecord iDatabase, String iClusterIndexName,
			int[] iClusterIdsToIndex, OProgressListener iProgressListener, boolean iAutomatic) {
		return (OIndexOneValue) super.create(iName, iKeyType, iDatabase, iClusterIndexName, iClusterIdsToIndex, iProgressListener,
				iAutomatic, OStreamSerializerRID.INSTANCE);
	}

	private Set<OIdentifiable> getLazySet(final ONavigableMap<Object, OIdentifiable> iSubSet) {
		if (iSubSet == null)
			return ORecordLazySet.EMPTY_SET;

		final Set<OIdentifiable> result = new ORecordLazySet(configuration.getDatabase());
		result.addAll(iSubSet.values());
		return result;
	}
}
