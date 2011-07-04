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

import java.util.Collection;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandGetIndex extends OServerCommandDocumentAbstract {
	private static final String[]	NAMES	= { "GET|index/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: index/<database>/<index-name>/<key>");

		iRequest.data.commandInfo = "Index get";

		ODatabaseDocumentTx db = null;

		try {
			db = getProfiledDatabaseInstance(iRequest);

			final OIndex index = db.getMetadata().getIndexManager().getIndex(urlParts[2]);
			if (index == null)
				throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");

			final Collection<OIdentifiable> content = index.get(urlParts[3]);

			if (content == null || content.size() == 0)
				sendTextContent(iRequest, OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, null,
						OHttpUtils.CONTENT_TEXT_PLAIN, null);
			else {
				final StringBuilder buffer = new StringBuilder();

				buffer.append('[');
				for (OIdentifiable item : content) {
					buffer.append(item.getRecord().toJSON());
				}
				buffer.append(']');

				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
			}
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
