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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Per database cache of documents.
 * 
 * @author Luca Garulli
 * 
 */
public class OStorageRecordCache extends OAbstractRecordCache {

	@SuppressWarnings("unused")
	final private OStorage	storage;
	private STRATEGY				strategy;

	public enum STRATEGY {
		POP_RECORD, COPY_RECORD
	}

	public OStorageRecordCache(final OStorage iStorage) {
		super("storage." + iStorage.getName(), OGlobalConfiguration.STORAGE_CACHE_SIZE.getValueAsInteger());
		storage = iStorage;
		setStrategy(OGlobalConfiguration.STORAGE_CACHE_STRATEGY.getValueAsInteger());
	}

	public void pushRecord(final ORecordInternal<?> iRecord) {
		if (maxSize == 0 || iRecord == null || iRecord.isDirty() || iRecord.getIdentity().isNew() || !iRecord.isPinned())
			// PRECONDITIONS
			return;

		acquireExclusiveLock();
		try {
			final ORecord<?> record = entries.get(iRecord.getIdentity());
			if (record == null || iRecord.getVersion() > record.getVersion())
				entries.put(iRecord.getIdentity(), iRecord);

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
		if (maxSize == 0)
			// PRECONDITIONS
			return null;

		acquireExclusiveLock();
		try {
			if (strategy == STRATEGY.POP_RECORD)
				// POP THE RECORD
				return entries.remove(iRID);
			else {
				// COPY THE RECORD
				final ORecordInternal<?> record = entries.get(iRID);
				if (record == null)
					return null;

				// PUT IT AGAIN FOR REFRESHING ORDER ENTRIES (LRU CACHE)
				entries.put(iRID, (ORecordInternal<?>) record.copy());
				return record;
			}
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
