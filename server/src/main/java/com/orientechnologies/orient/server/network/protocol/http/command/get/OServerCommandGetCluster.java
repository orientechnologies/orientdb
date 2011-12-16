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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetCluster extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "GET|cluster/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		String[] urlParts = checkSyntax(
				iRequest.url,
				3,
				"Syntax error: cluster/<database>/<cluster-name>[/<limit>]<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		iRequest.data.commandInfo = "Browse cluster";
		iRequest.data.commandDetail = urlParts[2];

		ODatabaseDocumentTx db = null;

		try {
			db = getProfiledDatabaseInstance(iRequest);

			if (db.getClusterIdByName(urlParts[2]) == -1)
				throw new IllegalArgumentException("Invalid cluster '" + urlParts[2] + "'");

			final int limit = urlParts.length > 3 ? Integer.parseInt(urlParts[3]) : 20;

			final List<OIdentifiable> response = new ArrayList<OIdentifiable>();
			for (ORecord<?> rec : db.browseCluster(urlParts[2])) {
				if (limit > 0 && response.size() >= limit)
					break;

				response.add(rec);
			}

			sendRecordsContent(iRequest, response);
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
