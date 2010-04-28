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
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.kv.OSharedDatabase;
import com.orientechnologies.orient.kv.network.protocol.http.ONetworkProtocolHttpKV;

public class OMapLoaderStore implements MapLoader<String, String>, MapStore<String, String> {
	public OMapLoaderStore() {
		OLogManager.instance().config(this, "OMapLoaderStore started");
	}

	/**
	 * Load the record using the local storage.
	 */
	public String load(final String iKey) {
		OLogManager.instance().config(this, "Loading entry from database: %s", iKey);

		ODatabaseBinary db = null;

		try {
			String[] parts = ONetworkProtocolHttpKV.getDbBucketKey(iKey, 3);

			db = OSharedDatabase.acquireDatabase(parts[0]);
			Map<String, String> bucket = OServerClusterMember.getDictionaryBucket(db, parts[1], false);
			return bucket.get(parts[2]);

		} catch (Exception e) {
			throw new ODistributedException("Error on load the entry with key: " + iKey, e);
		} finally {

			if (db != null)
				OSharedDatabase.releaseDatabase(db);
		}
	}

	/**
	 * Store the record in the local storage.
	 */
	public void store(final String iKey, final String iValue) {
		OLogManager.instance().config(this, "Saving entry into database: %s", iKey);

		ODatabaseBinary db = null;

		try {
			String[] parts = ONetworkProtocolHttpKV.getDbBucketKey(iKey, 3);

			db = OSharedDatabase.acquireDatabase(parts[0]);
			Map<String, String> bucket = OServerClusterMember.getDictionaryBucket(db, parts[1], false);

			bucket.put(parts[2], iValue);

		} catch (Exception e) {
			throw new ODistributedException("Error on save the entry with key: " + iKey, e);
		} finally {

			if (db != null)
				OSharedDatabase.releaseDatabase(db);
		}
	}

	/**
	 * Delete a record from the local storage.
	 */
	public void delete(final String iKey) {
		OLogManager.instance().config(this, "Deleting entry into database: %s", iKey);

		ODatabaseBinary db = null;

		try {
			String[] parts = ONetworkProtocolHttpKV.getDbBucketKey(iKey, 3);

			db = OSharedDatabase.acquireDatabase(parts[0]);
			Map<String, String> bucket = OServerClusterMember.getDictionaryBucket(db, parts[1], false);
			bucket.remove(parts[2]);

		} catch (Exception e) {
			throw new ODistributedException("Error on delete the entry with key: " + iKey, e);
		} finally {

			if (db != null)
				OSharedDatabase.releaseDatabase(db);
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
