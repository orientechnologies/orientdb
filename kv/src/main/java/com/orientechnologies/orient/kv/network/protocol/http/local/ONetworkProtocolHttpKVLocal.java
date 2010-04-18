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

import java.util.Map;

import com.hazelcast.core.Hazelcast;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.kv.OSharedDatabase;
import com.orientechnologies.orient.kv.network.protocol.http.ONetworkProtocolHttpKV;
import com.orientechnologies.orient.kv.network.protocol.http.partitioned.ODistributedException;
import com.orientechnologies.orient.kv.network.protocol.http.partitioned.OServerClusterMember;

public class ONetworkProtocolHttpKVLocal extends ONetworkProtocolHttpKV {
	@Override
	protected Map<String, String> getBucket(final String dbName, final String iBucketName) {
		ODatabaseBinary db = null;

		try {
			db = OSharedDatabase.acquireDatabase(dbName);
			OTreeMapPersistent<String, String> bucket = OServerClusterMember.getDictionaryBucket(db, iBucketName);

			Map<String, String> bucketMap = Hazelcast.getMap(dbName + URL_SEPARATOR + bucket);

			return bucketMap;
		} catch (Exception e) {
			throw new ODistributedException("Error on retrieving bucket '" + iBucketName + "' in database: " + dbName, e);
		} finally {

			if (db != null)
				OSharedDatabase.releaseDatabase(db);
		}
	}

	@Override
	protected String getKey(final String iDbBucketKey) {
		return getDbBucketKey(iDbBucketKey)[2];
	}
}
