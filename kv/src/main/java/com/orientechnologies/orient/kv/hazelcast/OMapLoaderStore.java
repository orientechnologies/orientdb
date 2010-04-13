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
package com.orientechnologies.orient.kv.hazelcast;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.storage.impl.local.ODictionaryLocal;
import com.orientechnologies.orient.kv.network.protocol.http.ONetworkProtocolHttpKV;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;
import com.orientechnologies.orient.server.network.protocol.OSharedDatabase;

public class OMapLoaderStore implements MapLoader<String, String>, MapStore<String, String> {
	public OMapLoaderStore() {
		OLogManager.instance().config(this, "OMapLoaderStore started");
	}

	public String load(final String iKey) {
		String parts[] = ONetworkProtocolHttpKV.getRequestParameters(iKey);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];

		ODatabaseBinary db;
		try {
			db = OSharedDatabase.acquireDatabase(dbName);

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);
				return bucketTree.get(key);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {

				OSharedDatabase.releaseDatabase(dbName, db);
			}
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on retrieving key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
		return null;
	}

	public Map<String, String> loadAll(final Collection<String> iKeys) {
		Map<String, String> entries = new HashMap<String, String>();

		for (String k : iKeys) {
			entries.put(k, load(k));
		}

		return entries;
	}

	public void delete(final String iKey) {
		String parts[] = ONetworkProtocolHttpKV.getRequestParameters(iKey);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];

		ODatabaseBinary db;
		try {
			db = OSharedDatabase.acquireDatabase(dbName);

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);
				bucketTree.remove(key);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {

				OSharedDatabase.releaseDatabase(dbName, db);
			}
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on delete key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
	}

	public void deleteAll(final Collection<String> iKeys) {
		for (String k : iKeys) {
			delete(k);
		}
	}

	public void store(final String iKey, final String iValue) {
		String parts[] = ONetworkProtocolHttpKV.getRequestParameters(iKey);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];

		ODatabaseBinary db;
		try {
			db = OSharedDatabase.acquireDatabase(dbName);

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);
				bucketTree.put(key, iValue);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {

				OSharedDatabase.releaseDatabase(dbName, db);
			}
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on store key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
	}

	public void storeAll(final Map<String, String> iEntries) {
		for (Map.Entry<String, String> entry : iEntries.entrySet()) {
			store(entry.getKey(), entry.getValue());
		}
	}

	private OTreeMapPersistent<String, String> getBucket(final ODatabaseRecordAbstract<ORecordBytes> iDb, final String iBucket)
			throws IOException {
		ORecordBytes rec = iDb.getDictionary().get(iBucket);

		OTreeMapPersistent<String, String> bucketTree = null;

		if (rec != null) {
			bucketTree = new OTreeMapPersistent<String, String>(iDb, ODictionaryLocal.DICTIONARY_DEF_CLUSTER_NAME, rec.getIdentity());
			bucketTree.load();
		}

		if (bucketTree == null) {
			// CREATE THE BUCKET
			bucketTree = new OTreeMapPersistent<String, String>(iDb, ODictionaryLocal.DICTIONARY_DEF_CLUSTER_NAME,
					OStreamSerializerString.INSTANCE, OStreamSerializerString.INSTANCE);
			bucketTree.save();

			iDb.getDictionary().put(iBucket, bucketTree.getRecord());
		}
		return bucketTree;
	}
}
