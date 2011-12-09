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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedPatternAbstract;

public class OServerCommandPostAction extends OServerCommandAuthenticatedPatternAbstract {
	public OServerCommandPostAction(final OServerCommandConfiguration iConfig) {
		super(iConfig);
	}

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		final String[] urlParts = checkSyntax(iRequest.url, 1, "Syntax error: *.action");

		iRequest.data.commandInfo = "Execute action";

		ODatabaseDocumentTx db = null;
		ODocument doc = new ODocument().fromJSON(iRequest.content);

		// ASSURE TO MAKE THE RECORD ID INVALID
		((ORecordId) doc.getIdentity()).clusterPosition = ORID.CLUSTER_POS_INVALID;

		try {
			db = getProfiledDatabaseInstance(iRequest);
			doc.save();

		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}

		sendTextContent(iRequest, 201, OHttpUtils.STATUS_OK_DESCRIPTION, null, OHttpUtils.CONTENT_TEXT_PLAIN, doc.getIdentity());
		return false;
	}
}
