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

import java.util.List;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetQuery extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "GET|query/*" };

	@Override
	@SuppressWarnings("unchecked")
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		String[] urlParts = checkSyntax(
				iRequest.url,
				4,
				"Syntax error: query/<database>/sql/query[/<limit>][/<fetchPlan>].<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4]) : 20;

		final String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;

		final String text = urlParts[3];

		iRequest.data.commandInfo = "Query";
		iRequest.data.commandDetail = text;

		if (!text.toLowerCase().startsWith("select"))
			throw new IllegalArgumentException("Only SQL Select commands are valid using Query command");

		ODatabaseDocumentTx db = null;

		final List<ORecord<?>> response;

		try {
			db = getProfiledDatabaseInstance(iRequest);

			response = (List<ORecord<?>>) db.command(new OSQLSynchQuery<ORecordSchemaAware<?>>(text, limit).setFetchPlan(fetchPlan))
					.execute();

		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}

		sendRecordsContent(iRequest, response, fetchPlan);
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
