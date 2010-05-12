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
package com.orientechnologies.orient.server.network.protocol.http.command.put;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPutDocument extends OServerCommandDocumentAbstract {
	private static final String[]	NAMES	= { "PUT.document" };

	public void execute(final OHttpRequest iRequest) throws Exception {
		final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: document/<database>/<record-id>");

		iRequest.data.commandType = OChannelBinaryProtocol.RECORD_UPDATE;

		ODatabaseDocumentTx db = null;

		// PARSE PARAMETERS
		final int parametersPos = urlParts[2].indexOf("?");
		final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
		final ORecordId recorId = new ORecordId(rid);

		if (!recorId.isValid())
			throw new IllegalArgumentException("Invalid Record ID in request: " + recorId);

		try {
			db = OSharedDocumentDatabase.acquireDatabase(urlParts[1]);

			final ODocument doc = new ODocument(db, rid);

			doc.load();

			doc.fromJSON(iRequest.content);

			doc.save();

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}

		sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid
				+ " updated successfully.");
	}

	public String[] getNames() {
		return NAMES;
	}
}
