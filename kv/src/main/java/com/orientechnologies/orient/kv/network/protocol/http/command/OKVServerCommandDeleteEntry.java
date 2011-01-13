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
package com.orientechnologies.orient.kv.network.protocol.http.command;

import java.util.Map;

import com.orientechnologies.orient.kv.network.protocol.http.OKVDictionary;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public class OKVServerCommandDeleteEntry extends OKVServerCommandAbstract {

	private static final String[]	NAMES	= { "DELETE|entry/*" };

	public OKVServerCommandDeleteEntry(OKVDictionary dictionary) {
		super(dictionary);
	}

	public boolean execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandInfo = "Delete entry";
		iRequest.data.commandDetail = iRequest.url;

		final String parts[] = getDbBucketKey(iRequest.url, 3);

		final String dbName = parts[0];
		final String bucket = parts[1];
		final String key = dictionary.getKey(iRequest.url);

		final int code;
		final String reason;
		final String content;

		final Map<String, String> bucketMap = dictionary.getBucket(dbName, iRequest.authorization, bucket);

		synchronized (bucketMap) {

			if (!bucketMap.containsKey(key)) {
				code = 404;
				reason = "Key not found";
				content = "The entry with key: " + key + " was not found in the bucket '" + bucket + "'";
			} else {
				code = OHttpUtils.STATUS_OK_CODE;
				reason = OHttpUtils.STATUS_OK_DESCRIPTION;
				content = bucketMap.remove(key);
			}
		}

		sendTextContent(iRequest, code, reason, null, OHttpUtils.CONTENT_TEXT_PLAIN, content);
		return false;
	}

	public String[] getNames() {
		return NAMES;
	}
}
