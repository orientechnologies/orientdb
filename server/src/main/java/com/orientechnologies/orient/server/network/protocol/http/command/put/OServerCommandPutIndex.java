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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPutIndex extends OServerCommandDocumentAbstract {
	private static final String[]	NAMES	= { "PUT|index/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: index/<database>/<index-name>/<key>[/<value>]");

		iRequest.data.commandInfo = "Index put";

		ODatabaseDocumentTx db = null;

		try {
			db = getProfiledDatabaseInstance(iRequest);

			final OIndex<?> index = db.getMetadata().getIndexManager().getIndex(urlParts[2]);
			if (index == null)
				throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");

			final OIdentifiable record;

			if (urlParts.length > 4)
				// GET THE RECORD ID AS VALUE
				record = new ORecordId(urlParts[4]);
			else {
				// GET THE REQUEST CONTENT AS DOCUMENT
				if (iRequest.content == null || iRequest.content.length() == 0)
					throw new IllegalArgumentException("Index's entry value is null");

				record = new ODocument(db).fromJSON(iRequest.content);
			}

			index.put(urlParts[3], record);

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Key '" + urlParts[3]
					+ "' correctly inserted into the index " + urlParts[2] + ".");
		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
