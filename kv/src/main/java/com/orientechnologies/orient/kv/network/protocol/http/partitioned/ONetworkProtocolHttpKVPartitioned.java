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

import java.util.Map;

import com.hazelcast.core.Hazelcast;
import com.orientechnologies.orient.kv.network.protocol.http.OKVDictionary;
import com.orientechnologies.orient.kv.network.protocol.http.ONetworkProtocolHttpKV;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public class ONetworkProtocolHttpKVPartitioned extends ONetworkProtocolHttpKV implements OKVDictionary {

	public ONetworkProtocolHttpKVPartitioned() {
		dictionary = this;
	}

	public Map<String, String> getBucket(final String dbName, final String iAuthorization, final String bucket) {
		return Hazelcast.getMap(dbName + OHttpUtils.URL_SEPARATOR + bucket);
	}

	public String getKey(final String iDbBucketKey) {
		return iDbBucketKey;
	}
}
