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
package com.orientechnologies.orient.server.network.protocol.http.command.delete;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandDeleteDocument extends OServerCommandDocumentAbstract {
	private static final String[]	NAMES	= { "DELETE.document" };

	public void execute(final OHttpRequest iRequest) throws Exception {
		ODatabaseDocumentTx db = null;

		try {
			final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: document/<database>/<record-id>");

			iRequest.data.commandInfo = "Delete document";

			db = getProfiledDatabaseInstance(iRequest, urlParts[1]);

			// PARSE PARAMETERS
			final int parametersPos = urlParts[2].indexOf("?");
			final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
			final ORecordId recorId = new ORecordId(rid);

			if (!recorId.isValid())
				throw new IllegalArgumentException("Invalid Record ID in request: " + urlParts[2]);

			final ODocument doc = new ODocument(db, recorId);
			doc.delete();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + recorId
					+ " deleted successfully.");

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
