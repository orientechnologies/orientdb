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
package com.orientechnologies.orient.kv.network.protocol.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.kv.index.OTreeMapPersistentAsynch;

/**
 * Caches bucket tree maps to be reused across calls.
 * 
 * @author Luca Garulli
 * 
 */
public class OKVDictionaryBucketManager {
	private static Map<String, OTreeMapPersistent<String, String>>	bucketCache						= new HashMap<String, OTreeMapPersistent<String, String>>();
	private static final String																			DEFAULT_CLUSTER_NAME	= "default";

	public static synchronized Map<String, String> getDictionaryBucket(final ODatabaseBinary iDatabase, final String iName,
			final boolean iAsynchMode) throws IOException {
		OTreeMapPersistent<String, String> bucket = bucketCache.get(iDatabase.getName() + ":" + iName);

		if (bucket != null)
			return bucket;

		ORecordBytes record = iDatabase.getDictionary().get(iName);

		if (record == null) {
			// CREATE THE BUCKET TRANSPARENTLY
			if (iAsynchMode)
				bucket = new OTreeMapPersistentAsynch<String, String>(iDatabase, DEFAULT_CLUSTER_NAME, OStreamSerializerString.INSTANCE,
						OStreamSerializerString.INSTANCE);
			else
				bucket = new OTreeMapPersistent<String, String>(iDatabase, DEFAULT_CLUSTER_NAME, OStreamSerializerString.INSTANCE,
						OStreamSerializerString.INSTANCE);

			bucket.save();
			// REGISTER THE NEW BUCKET
			iDatabase.getDictionary().put(iName, bucket.getRecord());
		} else {
			if (iAsynchMode)
				bucket = new OTreeMapPersistentAsynch<String, String>(iDatabase, DEFAULT_CLUSTER_NAME, record.getIdentity());
			else
				bucket = new OTreeMapPersistent<String, String>(iDatabase, DEFAULT_CLUSTER_NAME, record.getIdentity());

			bucket.load();
		}

		bucketCache.put(iDatabase.getName() + ":" + iName, bucket);

		return bucket;
	}
}