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
package com.orientechnologies.orient.kv.network.protocol.http.partitioned;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.kv.OSharedBinaryDatabase;
import com.orientechnologies.orient.kv.OSharedBinaryDatabaseDistributed;
import com.orientechnologies.orient.kv.network.protocol.http.OKVDictionaryBucketManager;
import com.orientechnologies.orient.kv.network.protocol.http.command.OKVServerCommandAbstract;

public class OMapLoaderStore implements MapLoader<String, String>, MapStore<String, String> {
	public OMapLoaderStore() {
		OLogManager.instance().info(this, "OMapLoaderStore started");
	}

	/**
	 * Load the record using the local storage.
	 */
	public String load(final String iKey) {
		OLogManager.instance().debug(this, "Loading entry from database: %s", iKey);

		ODatabaseBinary db = null;

		try {
			String[] parts = OKVServerCommandAbstract.getDbBucketKey(iKey, 3);

			db = OSharedBinaryDatabaseDistributed.acquireDatabase(parts[0] + ":admin:admin");

			final Map<String, String> bucketMap = OKVDictionaryBucketManager.getDictionaryBucket(db, parts[1], false);

			synchronized (bucketMap) {
				return bucketMap.get(parts[2]);
			}

		} catch (Exception e) {
			throw new OKVDistributedException("Error on load the entry with key: " + iKey, e);
		} finally {

			if (db != null)
				OSharedBinaryDatabase.release(db);
		}
	}

	/**
	 * Store the record in the local storage.
	 */
	public void store(final String iKey, final String iValue) {
		OLogManager.instance().debug(this, "Saving entry into database: %s", iKey);

		ODatabaseBinary db = null;

		try {
			final String[] parts = OKVServerCommandAbstract.getDbBucketKey(iKey, 3);

			db = OSharedBinaryDatabaseDistributed.acquireDatabase(parts[0] + ":admin:admin");
			final Map<String, String> bucketMap = OKVDictionaryBucketManager.getDictionaryBucket(db, parts[1], false);

			synchronized (bucketMap) {
				bucketMap.put(parts[2], iValue);
			}

		} catch (Exception e) {
			throw new OKVDistributedException("Error on save the entry with key: " + iKey, e);
		} finally {

			if (db != null)
				OSharedBinaryDatabase.release(db);
		}
	}

	/**
	 * Delete a record from the local storage.
	 */
	public void delete(final String iKey) {
		OLogManager.instance().debug(this, "Deleting entry into database: %s", iKey);

		ODatabaseBinary db = null;

		try {
			String[] parts = OKVServerCommandAbstract.getDbBucketKey(iKey, 3);

			db = OSharedBinaryDatabaseDistributed.acquireDatabase(parts[0] + ":admin:admin");
			final Map<String, String> bucketMap = OKVDictionaryBucketManager.getDictionaryBucket(db, parts[1], false);
			synchronized (bucketMap) {
				bucketMap.remove(parts[2]);
			}
		} catch (Exception e) {
			throw new OKVDistributedException("Error on delete the entry with key: " + iKey, e);
		} finally {

			if (db != null)
				OSharedBinaryDatabase.release(db);
		}
	}

	/**
	 * Load, in one shot, all the records with the ids specified.
	 */
	public Map<String, String> loadAll(final Collection<String> iKeys) {
		Map<String, String> entries = new HashMap<String, String>();

		for (String k : iKeys)
			entries.put(k, load(k));

		return entries;
	}

	/**
	 * Store, in one shot, all the records with the ids specified.
	 */
	public void storeAll(final Map<String, String> iEntries) {
		for (Map.Entry<String, String> entry : iEntries.entrySet())
			store(entry.getKey(), entry.getValue());
	}

	/**
	 * Delete, in one shot, all the records with the ids specified.
	 */
	public void deleteAll(final Collection<String> iKeys) {
		for (String k : iKeys)
			delete(k);
	}
}
