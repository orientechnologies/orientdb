/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.monitor.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSession;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetLoggedUserInfo extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "GET|loggedUserInfo/*", "POST|loggedUserInfo/*" };

	public OServerCommandGetLoggedUserInfo(final OServerCommandConfiguration iConfiguration) {
	}

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
		OHttpSession session = OHttpSessionManager.getInstance().getSession(iRequest.sessionId);
		final String[] urlParts = checkSyntax(iRequest.url, 1, "Syntax error: loggedUserInfo/<db>/<type>");

		String command = urlParts[2];
		if ("configuration".equals(command)) {
			if (iRequest.httpMethod.equals("GET")) {
				ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("user", session.getUserName());
				final List<OIdentifiable> response = db.query(new OSQLSynchQuery<ORecordSchemaAware<?>>(
						"select from UserConfiguration where user.name = :user"), params);
				iResponse.writeRecords(response);
			} else {
				ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
				ODatabaseRecordThreadLocal.INSTANCE.set(db);
				ODocument doc = new ODocument().fromJSON(iRequest.content);
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("name", session.getUserName());
				List<ODocument> users = db.query(new OSQLSynchQuery<ORecordSchemaAware<?>>("select from OUser where name = :name"), params);
				ODocument user = users.iterator().next();
				doc.field("user", user);
				doc.save();
				iResponse.writeResult(doc, "indent:6");
			}
			return false;
		} else {
			try {
				ODocument document = new ODocument();
				document.field("user", session.getUserName());
				document.field("database", session.getDatabaseName());
				document.field("host", session.getParameter("host"));
				document.field("port", session.getParameter("port"));
				iResponse.writeResult(document, "indent:6");
			} catch (Exception e) {
				iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
			}
			return false;
		}
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
