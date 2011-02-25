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

	public OStorageRecordCache(final OStorage iStorage) {
		super("storage." + iStorage.getName(), OGlobalConfiguration.STORAGE_CACHE_SIZE.getValueAsInteger());
		storage = iStorage;
	}

	public void pushRecord(final ORecordInternal<?> iRecord) {
		if (maxSize == 0 || iRecord == null || iRecord.isDirty() || iRecord.getIdentity().isNew())
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
	 * Remove, if found, a record from the cache. used by database's cache.
	 * 
	 * @param iRID
	 * @return
	 */
	protected ORecordInternal<?> popRecord(final ORID iRID) {
		if (maxSize == 0)
			// PRECONDITIONS
			return null;

		acquireExclusiveLock();
		try {
			final ORecordInternal<?> record = entries.remove(iRID);
			return record;
		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public String toString() {
		return "STORAGE level2 cache records=" + getSize() + ", maxSize=" + maxSize;
	}
}
