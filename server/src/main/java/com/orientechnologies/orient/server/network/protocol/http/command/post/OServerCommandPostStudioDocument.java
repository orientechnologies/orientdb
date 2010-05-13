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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandPostStudioDocument extends OServerCommandAbstract {
	private static final String[]	NAMES	= { "POST.studio-document" };

	@SuppressWarnings("unchecked")
	public void execute(final OHttpRequest iRequest) throws Exception {
		ODatabaseDocumentTx db = null;

		try {
			final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: studio-document/<database>");
			db = OSharedDocumentDatabase.acquireDatabase(urlParts[1]);

			final String req = iRequest.content;

			// PARSE PARAMETERS
			String operation = null;
			String rid = null;
			String className = null;

			final Map<String, String> fields = new HashMap<String, String>();

			final String[] params = req.split("&");
			String value;

			for (String p : params) {
				String[] pairs = p.split("=");
				value = pairs.length == 1 ? null : pairs[1];

				if ("oper".equals(pairs[0]))
					operation = value;
				else if ("0".equals(pairs[0]))
					rid = value;
				else if ("1".equals(pairs[0]))
					className = value;
				else if (pairs[0].startsWith("_") || pairs[0].equals("id"))
					continue;
				else {
					fields.put(pairs[0], value);
				}
			}

			if ("edit".equals(operation)) {
				iRequest.data.commandType = OChannelBinaryProtocol.RECORD_UPDATE;

				if (rid == null)
					throw new IllegalArgumentException("Record ID not found in request");

				final ODocument doc = new ODocument(db, className, new ORecordId(rid));
				doc.load();

				// BIND ALL CHANGED FIELDS
				Object oldValue;
				Object newValue;
				for (Entry<String, String> f : fields.entrySet()) {
					oldValue = doc.field(f.getKey());
					newValue = f.getValue();

					if (oldValue != null) {
						if (oldValue instanceof ORecord<?>)
							newValue = new ORecordId(f.getValue());
						else if (oldValue instanceof Collection<?>) {
							newValue = new ArrayList<ODocument>();

							if (f.getValue() != null) {
								String[] items = f.getValue().split(",");
								for (String s : items) {
									((List<ODocument>) newValue).add(new ODocument(db, new ORecordId(s)));
								}
							}
						}
					}

					doc.field(f.getKey(), newValue);
				}

				doc.save();
				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid
						+ " updated successfully.");
			} else if ("add".equals(operation)) {
				iRequest.data.commandType = OChannelBinaryProtocol.RECORD_CREATE;

				final ODocument doc = new ODocument(db, className);

				// BIND ALL CHANGED FIELDS
				for (Entry<String, String> f : fields.entrySet())
					doc.field(f.getKey(), f.getValue());

				doc.save();
				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + doc.getIdentity()
						+ " updated successfully.");

			} else if ("del".equals(operation)) {
				iRequest.data.commandType = OChannelBinaryProtocol.RECORD_DELETE;

				if (rid == null)
					throw new IllegalArgumentException("Record ID not found in request");

				final ODocument doc = new ODocument(db, new ORecordId(rid));
				doc.delete();
				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid
						+ " deleted successfully.");

			} else
				sendTextContent(iRequest, 500, "Error", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Operation not supported");

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
