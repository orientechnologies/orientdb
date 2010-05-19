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

public class OKVServerCommandGetEntry extends OKVServerCommandAbstract {
	private static final String[]	NAMES	= { "GET.entry" };

	public OKVServerCommandGetEntry(OKVDictionary dictionary) {
		super(dictionary);
	}

	public void execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandInfo = "Lookup entry";
		iRequest.data.commandDetail = iRequest.url;

		final String[] urlParts = getDbBucketKey(iRequest.url, 2);

		final String dbName = urlParts[0];
		final String bucket = urlParts[1];
		final String key = dictionary.getKey(iRequest.url);

		String value;

		// SEARCH THE BUCKET
		final Map<String, String> bucketMap = dictionary.getBucket(dbName, iRequest.authorization, bucket);

		synchronized (bucketMap) {
			if (key != null)
				// SEARCH THE KEY
				value = bucketMap.get(key);
			else {
				// BROWSE ALL THE KEYS
				final StringBuilder buffer = new StringBuilder();
				for (String k : bucketMap.keySet()) {
					buffer.append(k + "\n");
				}
				value = buffer.toString();
			}
		}

		final int code = value == null ? 404 : OHttpUtils.STATUS_OK_CODE;
		final String reason = value == null ? "Not Found" : OHttpUtils.STATUS_OK_DESCRIPTION;
		final String content = value == null ? "The key '" + key + "' was not found in database '" + dbName + "'" : value.toString();

		sendTextContent(iRequest, code, reason, null, "text/plain", content);
	}

	public String[] getNames() {
		return NAMES;
	}
}
