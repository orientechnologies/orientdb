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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordBinary;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.storage.impl.local.ODictionaryLocal;
import com.orientechnologies.orient.enterprise.channel.text.OChannelTextServer;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;

public class ONetworkProtocolHttpKV extends ONetworkProtocolHttpAbstract {
	@Override
	public void doGet(String iURI, String iContent, OChannelTextServer iChannel) throws ONetworkProtocolException {
		if ("/".equals(iURI) || iURI.startsWith("/www")) {
			directAccess(iURI);
			return;
		}

		String parts[] = getRequestParameters(iURI);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];

		try {
			String content;
			String value;

			ODatabaseRecordBinary db = acquireDatabase(dbName);

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);

				value = bucketTree.get(key);

			} finally {

				releaseDatabase(dbName, db);
			}

			int code = value == null ? 404 : 200;
			String reason = value == null ? "Not Found" : "Ok";
			content = value == null ? "The key '" + key + "' was not found in database '" + dbName + "'" : value.toString();

			sendTextContent(code, reason, "text/plain", content);
		} catch (SocketException e) {
			connectionError();

		} catch (SocketTimeoutException e) {
			timeout();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on retrieving key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
	}

	private void directAccess(final String iURI) {
		String wwwPath = System.getProperty("orient.www.path", "src/site");

		InputStream bufferedFile = null;
		try {
			String url = "/".equals(iURI) ? url = "/www/index.htm" : iURI;
			url = wwwPath + url.substring("www".length() + 1, url.length());

			bufferedFile = new BufferedInputStream(new FileInputStream(url));

			String type = null;
			if (url.endsWith(".htm") || url.endsWith(".html"))
				type = "text/html";
			else if (url.endsWith(".png"))
				type = "image/png";
			else if (url.endsWith(".jpeg"))
				type = "image/jpeg";
			else if (url.endsWith(".js"))
				type = "application/x-javascript";
			else if (url.endsWith(".css"))
				type = "text/css";

			sendBinaryContent(200, null, type, bufferedFile);

		} catch (IOException e) {
			e.printStackTrace();

		} finally {
			if (bufferedFile != null)
				try {
					bufferedFile.close();
				} catch (IOException e) {
				}
		}
	}

	@Override
	public void doPut(String iURI, String iContent, OChannelTextServer iChannel) throws ONetworkProtocolException {
		String parts[] = getRequestParameters(iURI);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];
		String value;

		try {
			ODatabaseRecordBinary db = acquireDatabase(dbName);

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);

				value = bucketTree.put(key, iContent);
			} finally {

				releaseDatabase(dbName, db);
			}

			sendTextContent(200, "Ok", "text/plain", value);

		} catch (SocketTimeoutException e) {
			timeout();

		} catch (SocketException e) {
			connectionError();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on retrieving key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
	}

	@Override
	public void doPost(String iURI, String iContent, OChannelTextServer iChannel) throws ONetworkProtocolException {
		String parts[] = getRequestParameters(iURI);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];

		try {
			ODatabaseRecordBinary db = acquireDatabase(dbName);

			int code;
			String reason;
			String content;

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);

				if (bucketTree.containsKey(key)) {
					code = 503;
					reason = "Entry already exists";
					content = "The entry with key: " + key + " already exists in the bucket '" + bucket + "'";
				} else {
					code = 200;
					reason = "Ok";
					content = null;

					bucketTree.put(key, iContent);
				}
			} finally {

				releaseDatabase(dbName, db);
			}

			sendTextContent(code, reason, "text/plain", content);
		} catch (SocketTimeoutException e) {
			timeout();

		} catch (SocketException e) {
			connectionError();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on retrieving key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
	}

	@Override
	public void doDelete(String iURI, String iContent, OChannelTextServer iChannel) throws ONetworkProtocolException {
		String parts[] = getRequestParameters(iURI);

		String dbName = parts[0];
		String bucket = parts[1];
		String key = parts[2];

		try {
			ODatabaseRecordBinary db = acquireDatabase(dbName);

			int code;
			String reason;
			String content;

			try {
				OTreeMapPersistent<String, String> bucketTree = getBucket(db, bucket);

				if (!bucketTree.containsKey(key)) {
					code = 503;
					reason = "Key not found";
					content = "The entry with key: " + key + " was not found in the bucket '" + bucket + "'";
				} else {
					code = 200;
					reason = "Ok";
					content = bucketTree.remove(key);
				}
			} finally {

				releaseDatabase(dbName, db);
			}

			sendTextContent(code, reason, "text/plain", content);
		} catch (SocketTimeoutException e) {
			timeout();

		} catch (SocketException e) {
			connectionError();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on retrieving key '" + key + "' from database '" + dbName + "'", e,
					ONetworkProtocolException.class);
		}
	}

	private OTreeMapPersistent<String, String> getBucket(ODatabaseRecord<ORecordBytes> db, String bucket) throws IOException {
		ORecordBytes rec = db.getDictionary().get(bucket);

		OTreeMapPersistent<String, String> bucketTree = null;

		if (rec != null) {
			bucketTree = new OTreeMapPersistent<String, String>(db, ODictionaryLocal.DICTIONARY_DEF_CLUSTER_NAME, rec.getIdentity());
			bucketTree.load();
		}

		if (bucketTree == null) {
			// CREATE THE BUCKET
			bucketTree = new OTreeMapPersistent<String, String>(db, ODictionaryLocal.DICTIONARY_DEF_CLUSTER_NAME,
					OStreamSerializerString.INSTANCE, OStreamSerializerString.INSTANCE);
			bucketTree.save();

			db.getDictionary().put(bucket, bucketTree.getRecord());
		}
		return bucketTree;
	}

	private String[] getRequestParameters(String iParameters) {
		if (iParameters == null || iParameters.length() <= 1)
			throw new ONetworkProtocolException("Requested URI is invalid: " + iParameters);

		// REMOVE THE FIRST /
		iParameters = iParameters.substring(1);

		String[] pars = iParameters.split("/");

		if (pars == null || pars.length < 3)
			throw new ONetworkProtocolException("Requested URI is invalid: " + iParameters);

		return pars;
	}
}
