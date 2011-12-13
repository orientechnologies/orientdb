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
package com.orientechnologies.orient.core.cache;

import java.util.Collection;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Per database cache of documents.
 * 
 * @author Luca Garulli
 * 
 */
public class OLevel2RecordCache extends OAbstractRecordCache {

	private STRATEGY	strategy;

	public enum STRATEGY {
		POP_RECORD, COPY_RECORD
	}

	public OLevel2RecordCache(final OStorage iStorage) {
		super("storage." + iStorage.getName(), OGlobalConfiguration.CACHE_LEVEL2_SIZE.getValueAsInteger());
		setStrategy(OGlobalConfiguration.CACHE_LEVEL2_STRATEGY.getValueAsInteger());
	}

	/**
	 * Moves records to the Level2 cache. Update only the records already present to avoid to put a non-updated record.
	 * 
	 * @param iValues
	 *          Collection of records to update
	 */
	public void moveRecords(final Collection<ORecordInternal<?>> iValues) {
		if (!enabled)
			return;

		acquireExclusiveLock();
		try {

			for (ORecordInternal<?> record : iValues) {
				if (record == null || record.isDirty() || record.getIdentity().isNew())
					continue;

				if (record.getIdentity().getClusterId() == excludedCluster)
					continue;

				if (record.isPinned()) {
					final ORecordInternal<?> prevEntry = entries.get(record.getIdentity());
					if (prevEntry != null && prevEntry.getVersion() >= record.getVersion())
						// UPDATE ONLY RECORDS NOT PRESENT OR WITH VERSION HIGHER THAN CURRENT
						continue;

					record.detach();
					entries.put(record.getIdentity(), record);

				} else
					entries.remove(record.getIdentity());
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateRecord(final ORecordInternal<?> iRecord) {
		if (!enabled || iRecord == null || iRecord.isDirty() || iRecord.getIdentity().isNew())
			// PRECONDITIONS
			return;

		if (iRecord.getIdentity().getClusterId() == excludedCluster)
			return;

		acquireExclusiveLock();
		try {
			if (iRecord.isPinned()) {
				final ORecordInternal<?> prevEntry = entries.get(iRecord.getIdentity());
				if (prevEntry != null && prevEntry.getVersion() >= iRecord.getVersion())
					// TRY TO UPDATE AN OLD RECORD, DISCARD IT
					return;

				if ((!ODatabaseRecordThreadLocal.INSTANCE.isDefined() || iRecord.getDatabase().isClosed())) {
					// DB CLOSED: MAKE THE RECORD INSTANCE AS REUSABLE AFTER A DETACH
					iRecord.detach();
					entries.put(iRecord.getIdentity(), iRecord);
				} else
					// DB OPEN: SAVES A COPY TO AVOID CHANGES IF THE SAME RECORD INSTANCE IS USED AGAIN
					entries.put(iRecord.getIdentity(), (ORecordInternal<?>) iRecord.flatCopy());
			} else
				entries.remove(iRecord.getIdentity());

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Retrieve the record if any following the supported strategies: 0 = If found remove it (pop): the client (database instances)
	 * will push it back when finished or on close. 1 = Return the instance but keep a copy in 2-level cache; this could help
	 * highly-concurrent environment.
	 * 
	 * @author Luca Garulli
	 * @author Sylvain Spinelli
	 * @param iRID
	 * @return
	 */
	protected ORecordInternal<?> retrieveRecord(final ORID iRID) {
		if (!enabled)
			// PRECONDITIONS
			return null;

		if (iRID.getClusterId() == excludedCluster)
			return null;

		acquireExclusiveLock();
		try {
			final ORecordInternal<?> record = entries.remove(iRID);
			if (record == null || record.isDirty())
				// NULL OR DIRTY RECORD: IGNORE IT
				return null;

			if (strategy == STRATEGY.COPY_RECORD)
				// PUT BACK A CLONE (THIS UPDATE ALSO THE LRU)
				entries.put(iRID, (ORecordInternal<?>) record.flatCopy());

			return record;

		} finally {
			releaseExclusiveLock();
		}
	}

	public STRATEGY getStrategy() {
		return strategy;
	}

	public void setStrategy(final STRATEGY iStrategy) {
		strategy = iStrategy;
	}

	public void setStrategy(final int iStrategy) {
		strategy = STRATEGY.values()[iStrategy];
	}

	@Override
	public String toString() {
		return "STORAGE level2 cache records=" + getSize() + ", maxSize=" + maxSize;
	}
}
