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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetDocument extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "GET|document/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		ODatabaseDocumentTx db = null;

		final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: document/<database>/<record-id>[/fetchPlan]");

		final String fetchPlan = urlParts.length > 3 ? urlParts[3] : null;

		iRequest.data.commandInfo = "Load document";

		final ORecord<?> rec;

		final int parametersPos = urlParts[2].indexOf('?');
		final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];

		try {
			db = getProfiledDatabaseInstance(iRequest);

			rec = db.load(new ORecordId(rid), fetchPlan);

		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}

		if (rec == null)
			sendTextContent(iRequest, 404, "Not Found", null, OHttpUtils.CONTENT_JSON, "Record with id '" + urlParts[2]
					+ "' was not found.");
		else
			sendRecordContent(iRequest, rec, fetchPlan);
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
