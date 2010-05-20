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
package com.orientechnologies.orient.kv.network.protocol.http.local;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.kv.OSharedBinaryDatabase;
import com.orientechnologies.orient.kv.index.OTreeMapPersistentAsynchThread;
import com.orientechnologies.orient.kv.network.protocol.http.OKVDictionary;
import com.orientechnologies.orient.kv.network.protocol.http.OKVDictionaryBucketManager;
import com.orientechnologies.orient.kv.network.protocol.http.ONetworkProtocolHttpKV;
import com.orientechnologies.orient.kv.network.protocol.http.command.OKVServerCommandAbstract;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerStorageConfiguration;

public class ONetworkProtocolHttpKVLocal extends ONetworkProtocolHttpKV implements OKVDictionary {
	private static Map<String, Map<String, Map<String, String>>>	memoryDatabases					= new HashMap<String, Map<String, Map<String, String>>>();
	private static final String																		ASYNCH_COMMIT_DELAY_PAR	= "asynch.commit.delay";
	private static boolean																				asynchMode							= false;

	static {
		// START ASYNCH THREAD IF CONFIGURED
		String v = OServerMain.server().getConfiguration().getProperty(ASYNCH_COMMIT_DELAY_PAR);
		if (v != null) {
			OTreeMapPersistentAsynchThread.getInstance().setDelay(Integer.parseInt(v));
			OTreeMapPersistentAsynchThread.getInstance().start();
			asynchMode = true;
		}

		// CREATE IN-MEMORY DATABASES EARLY
		for (OServerStorageConfiguration stg : OServerMain.server().getConfiguration().storages) {
			if (stg.path.startsWith(OEngineMemory.NAME)) {
				ODatabaseBinary db = new ODatabaseBinary(stg.path);

				// CREATE AND PUT IN THE MEMORY MAPTABLE TO AVOID LOCKING (IT'S THREAD SAFE)
				db.create();
				OServerMain.server().getMemoryDatabases().put(stg.name, db);
			}
		}
	}

	public ONetworkProtocolHttpKVLocal() {
		dictionary = this;
	}

	public String getKey(final String iDbBucketKey) {
		final String[] parts = OKVServerCommandAbstract.getDbBucketKey(iDbBucketKey, 2);
		if (parts.length > 2)
			return parts[2];
		return null;
	}

	public Map<String, String> getBucket(final String dbName, final String iAuthorization, final String iBucketName) {
		ODatabaseBinary db = null;

		// CHECK FOR IN-MEMORY DB
		db = (ODatabaseBinary) OServerMain.server().getMemoryDatabases().get(dbName);
		if (db != null)
			return getBucketFromMemory(dbName, iBucketName);
		else
			return getBucketFromDatabase(dbName, iAuthorization, iBucketName);
	}

	protected Map<String, String> getBucketFromDatabase(final String dbName, String iAuthorization, final String iBucketName) {
		ODatabaseBinary db = null;

		if (iAuthorization == null)
			// NO USER/PASSWD, GO AS ADMIN
			iAuthorization = "admin:admin";

		try {
			db = OSharedBinaryDatabase.acquireDatabase(dbName + ":" + iAuthorization);

			return OKVDictionaryBucketManager.getDictionaryBucket(db, iBucketName, asynchMode);

		} catch (Exception e) {
			throw new OException("Error on retrieving bucket '" + iBucketName + "' in database: " + dbName, e);
		} finally {

			if (db != null)
				OSharedBinaryDatabase.releaseDatabase(db);
		}
	}

	protected Map<String, String> getBucketFromMemory(final String dbName, final String iBucketName) {
		Map<String, Map<String, String>> db = memoryDatabases.get(dbName);
		if (db == null) {
			db = new HashMap<String, Map<String, String>>();
			memoryDatabases.put(dbName, db);
		}

		Map<String, String> bucket = db.get(iBucketName);
		if (bucket == null) {
			bucket = new HashMap<String, String>();
			db.put(iBucketName, bucket);
		}
		return bucket;
	}
}
