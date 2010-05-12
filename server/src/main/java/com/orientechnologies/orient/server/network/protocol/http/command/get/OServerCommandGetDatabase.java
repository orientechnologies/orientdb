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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetDatabase extends OServerCommandAbstract {
	private static final String[]	NAMES	= { "GET.database" };

	public void execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandType = -1;

		String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: database/<database>");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(urlParts[1]);

			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer);

			json.beginObject();
			if (db.getMetadata().getSchema().getClasses() != null) {
				json.beginCollection(1, false, "classes");
				for (OClass cls : db.getMetadata().getSchema().getClasses()) {
					json.beginObject(1, true, null);
					json.writeAttribute(2, true, "id", cls.getId());
					json.writeAttribute(2, true, "name", cls.getName());

					if (cls.properties() != null && cls.properties().size() > 0) {
						json.beginCollection(2, true, "properties");
						for (OProperty prop : cls.properties()) {
							json.beginObject(2, true, null);
							json.writeAttribute(3, true, "id", prop.getId());
							json.writeAttribute(3, true, "name", prop.getName());
							if (prop.getLinkedClass() != null)
								json.writeAttribute(3, true, "linkedClass", prop.getLinkedClass().getName());
							if (prop.getLinkedType() != null)
								json.writeAttribute(3, true, "linkedType", prop.getLinkedType());
							json.writeAttribute(3, true, "type", prop.getType().toString());
							json.writeAttribute(3, true, "min", prop.getMin());
							json.writeAttribute(3, true, "max", prop.getMax());
							json.endObject(2, true);
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
					json.endObject(2, false);
				}
				json.endCollection(1, true);
			}
			json.endObject();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
