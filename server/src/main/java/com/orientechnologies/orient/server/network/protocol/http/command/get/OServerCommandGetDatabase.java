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

import java.io.StringWriter;
import java.util.Arrays;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetDatabase extends OServerCommandAbstract {
	private static final String[]	NAMES	= { "GET.database" };

	public void execute(final OHttpRequest iRequest) throws Exception {
		String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: database/<database>");

		// if (iRequest.sessionId == null) {
		// sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION,
		// "WWW-Authenticate: Basic realm=\"Secure Area\"", OHttpUtils.CONTENT_TEXT_PLAIN, "401 Unauthorized.");
		// return;
		// }

		iRequest.data.commandInfo = "Database info";
		iRequest.data.commandDetail = urlParts[1];

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(urlParts[1]);

			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer);

			json.beginObject();
			if (db.getMetadata().getSchema().getClasses() != null) {
				json.beginCollection(1, false, "classes");
				for (OClass cls : db.getMetadata().getSchema().getClasses()) {
					json.beginObject(2, true, null);
					json.writeAttribute(3, true, "id", cls.getId());
					json.writeAttribute(3, true, "name", cls.getName());
					json.writeAttribute(3, true, "clusters", cls.getClusterIds());
					json.writeAttribute(3, true, "defaultCluster", cls.getDefaultClusterId());
					json.writeAttribute(3, false, "records", db.countClass(cls.getName()));

					if (cls.properties() != null && cls.properties().size() > 0) {
						json.beginCollection(3, true, "properties");
						for (OProperty prop : cls.properties()) {
							json.beginObject(4, true, null);
							json.writeAttribute(4, true, "id", prop.getId());
							json.writeAttribute(4, true, "name", prop.getName());
							if (prop.getLinkedClass() != null)
								json.writeAttribute(4, true, "linkedClass", prop.getLinkedClass().getName());
							if (prop.getLinkedType() != null)
								json.writeAttribute(4, true, "linkedType", prop.getLinkedType());
							json.writeAttribute(4, true, "type", prop.getType().toString());
							json.writeAttribute(4, true, "min", prop.getMin());
							json.writeAttribute(4, true, "max", prop.getMax());
							json.endObject(3, true);
						}
						json.endCollection(1, true);
					}
					json.endObject(1, false);
				}
				json.endCollection(1, true);
			}

			if (db.getClusterNames() != null) {
				json.beginCollection(1, false, "clusters");
				for (String clusterName : db.getClusterNames()) {
					json.beginObject(2, true, null);
					json.writeAttribute(3, false, "id", db.getClusterIdByName(clusterName));
					json.writeAttribute(3, false, "name", clusterName);
					json.writeAttribute(3, false, "type", db.getClusterIdByName(clusterName) > -1 ? "Physical" : "Logical");
					json.writeAttribute(3, false, "records", db.countClusterElements(clusterName));
					json.endObject(2, false);
				}
				json.endCollection(1, true);
			}

			json.beginCollection(1, false, "users");
			for (OUser user : db.getMetadata().getSecurity().getUsers()) {
				json.beginObject(2, true, null);
				json.writeAttribute(3, false, "name", user.getName());
				json.writeAttribute(3, false, "roles", user.getRoles() != null ? Arrays.toString(user.getRoles().toArray()) : "null");
				json.endObject(2, false);
			}
			json.endCollection(1, true);

			json.beginCollection(1, false, "roles");
			for (ORole role : db.getMetadata().getSecurity().getRoles()) {
				json.beginObject(2, true, null);
				json.writeAttribute(3, false, "name", role.getName());
				json.writeAttribute(3, false, "ACL", "not supported yet");
				json.endObject(2, false);
			}
			json.endCollection(1, true);

			json.endObject();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
