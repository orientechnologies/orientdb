/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Per database cache of documents. It's not synchronized since database object are not thread-safes.
 * 
 * @author Luca Garulli
 * 
 */
public class OLevel1RecordCache extends OAbstractRecordCache {

	private final ODatabaseRecord	database;
	private OLevel2RecordCache		level2cache;
	private String								PROFILER_CACHE_FOUND;
	private String								PROFILER_CACHE_NOTFOUND;

	public OLevel1RecordCache(final ODatabaseRecord iDatabase) {
		super("db." + iDatabase.getName(), OGlobalConfiguration.CACHE_LEVEL1_SIZE.getValueAsInteger());
		database = iDatabase;
	}

	@Override
	public void startup() {
		profilerPrefix = "db." + database.getName();
		PROFILER_CACHE_FOUND = profilerPrefix + ".cache.found";
		PROFILER_CACHE_NOTFOUND = profilerPrefix + ".cache.notFound";

		super.startup();

		level2cache = (OLevel2RecordCache) database.getLevel2Cache();
	}

	public void updateRecord(final ORecordInternal<?> iRecord) {
		if (enabled) {
			acquireExclusiveLock();
			try {
				if (entries.get(iRecord.getIdentity()) != iRecord)
					entries.put(iRecord.getIdentity(), iRecord);
			} finally {
				releaseExclusiveLock();
			}
		}

		level2cache.updateRecord(iRecord);
	}

	/**
	 * Search a record in the cache and if found add it in the Database's level-1 cache.
	 * 
	 * @param iRID
	 *          RecordID to search
	 * @param iDbCache
	 *          Database's cache
	 * @return The record if found, otherwise null
	 */
	public ORecordInternal<?> findRecord(final ORID iRID) {
		if (!enabled)
			// PRECONDITIONS
			return null;

		ORecordInternal<?> record;

		// SEARCH INTO DATABASE'S 1-LEVEL CACHE
		acquireSharedLock();
		try {
			record = entries.get(iRID);
		} finally {
			releaseSharedLock();
		}

		if (record == null) {
			// SEARCH INTO THE STORAGE'S 2-LEVEL CACHE
			record = level2cache.retrieveRecord(iRID);

			if (record != null) {
				// FOUND: MOVE IT INTO THE DB'S CACHE
				record.setDatabase(database);

				acquireExclusiveLock();
				try {
					entries.put(record.getIdentity(), record);
				} finally {
					releaseExclusiveLock();
				}
			}
		}

		OProfiler.getInstance().updateCounter(record != null ? PROFILER_CACHE_FOUND : PROFILER_CACHE_NOTFOUND, +1);

		return record;
	}

	/**
	 * Delete a record entry from both database and storage caches.
	 * 
	 * @param iRecord
	 *          Record to remove
	 */
	public void deleteRecord(final ORID iRecord) {
		super.deleteRecord(iRecord);
		level2cache.freeRecord(iRecord);
	}

	public void shutdown() {
		clear();
		super.shutdown();
		level2cache = null;
	}

	@Override
	public void clear() {
		acquireExclusiveLock();
		try {
			if (level2cache != null)
				// MOVE ALL THE LEVEL-1 CACHE INTO THE LEVEL-2 CACHE
				level2cache.moveRecords(entries.values());

			entries.clear();
		} finally {
			releaseExclusiveLock();
		}
	}

	public void invalidate() {
		acquireExclusiveLock();
		try {
			entries.clear();
		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public String toString() {
		return "DB level1 cache records=" + getSize() + ", maxSize=" + maxSize;
	}
}
