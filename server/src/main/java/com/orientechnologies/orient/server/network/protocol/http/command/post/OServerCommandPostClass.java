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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandPostClass extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "POST|class/*" };

	public void execute(final OHttpRequest iRequest) throws Exception {
		String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: class/<database>/<class-name>");

		iRequest.data.commandInfo = "Create class";
		iRequest.data.commandDetail = urlParts[2];

		ODatabaseDocumentTx db = null;

		try {
			db = getProfiledDatabaseInstance(iRequest, urlParts[1]);

			if (db.getMetadata().getSchema().getClass(urlParts[2]) != null)
				throw new IllegalArgumentException("Class '" + urlParts[2] + "' already exists");

			final OClass cls = db.getMetadata().getSchema().createClass(urlParts[2]);
			db.getMetadata().getSchema().save();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, cls.getId());

		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
