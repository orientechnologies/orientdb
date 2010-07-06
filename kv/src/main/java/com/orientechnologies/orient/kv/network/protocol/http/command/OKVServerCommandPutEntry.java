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

public class OKVServerCommandPutEntry extends OKVServerCommandAbstract {
	private static final String[]	NAMES	= { "PUT|entry" };

	public OKVServerCommandPutEntry(OKVDictionary dictionary) {
		super(dictionary);
	}

	public void execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandInfo = "Update entry";
		iRequest.data.commandDetail = iRequest.url;

		final String parts[] = getDbBucketKey(iRequest.url, 3);

		final String dbName = parts[0];
		final String bucket = parts[1];
		final String key = dictionary.getKey(iRequest.url);

		final Map<String, String> bucketMap = dictionary.getBucket(dbName, iRequest.authorization, bucket);

		final int code;
		final String reason;
		final String content;

		if (bucketMap.containsKey(key)) {
			code = OHttpUtils.STATUS_OK_CODE;
			reason = OHttpUtils.STATUS_OK_DESCRIPTION;
			content = null;

			bucketMap.put(key, iRequest.content);
		} else {
			code = 404;
			reason = "Entry not exists. Use HTTP POST instead.";
			content = "The entry with key: " + key + " not exists in the bucket '" + bucket + "'";
		}

		sendTextContent(iRequest, code, reason, null, "text/plain", content);
	}

	public String[] getNames() {
		return NAMES;
	}
}
