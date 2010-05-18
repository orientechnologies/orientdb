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
package com.orientechnologies.orient.server.network.protocol.http.command;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OHttpSessionManager;

public abstract class OServerCommandAbstract implements OServerCommand {

	public boolean beforeExecute(final OHttpRequest iRequest) throws IOException {
		if (iRequest.executor.getAccount() == null) {
			if (iRequest.authorization == null) {
				// UNAUTHORIZED
				sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION,
						"WWW-Authenticate: Basic realm=\"Secure Area\"", OHttpUtils.CONTENT_TEXT_PLAIN, "401 Unauthorized.");
				return false;
			} else {
				String[] credentials = iRequest.authorization.split(":");

				// TODO: LOGIN IN DATABASE!

				iRequest.sessionId = OHttpSessionManager.getInstance().createSession();
			}
		}

		return true;
	}

	protected void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
			final String iContentType, final String iContent) throws IOException {
		sendStatus(iRequest, iCode, iReason);
		sendResponseHeaders(iRequest, iContentType);
		if (iHeaders != null)
			writeLine(iRequest, iHeaders);

		if (iRequest.sessionId != null)
			writeLine(iRequest, "sessionID: " + iRequest.sessionId);

		writeLine(iRequest, OHttpUtils.CONTENT_LENGTH + (iContent != null ? iContent.length() + 1 : 0));
		writeLine(iRequest, null);

		if (iContent != null && iContent.length() > 0) {
			writeLine(iRequest, iContent);
		}

		iRequest.channel.flush();
	}

	protected void sendStatus(final OHttpRequest iRequest, final int iStatus, final String iReason) throws IOException {
		writeLine(iRequest, iRequest.httpVersion + " " + iStatus + " " + iReason);
	}

	protected void sendResponseHeaders(final OHttpRequest iRequest, final String iContentType) throws IOException {
		writeLine(iRequest, "Cache-Control: no-cache, no-store, max-age=0, must-revalidate");
		writeLine(iRequest, "Pragma: no-cache");
		writeLine(iRequest, "Date: " + new Date());
		writeLine(iRequest, "Content-Type: " + iContentType);
		writeLine(iRequest, "Server: " + iRequest.data.serverInfo);
		writeLine(iRequest, "Connection: Keep-Alive");
	}

	protected void writeLine(final OHttpRequest iRequest, final String iContent) throws IOException {
		if (iContent != null)
			iRequest.channel.outStream.write(iContent.getBytes());
		iRequest.channel.outStream.write(OHttpUtils.EOL);
	}

	protected String[] checkSyntax(String iURL, final int iArgumentCount, final String iSyntax) {
		final int parametersPos = iURL.indexOf("?");
		if (parametersPos > -1)
			iURL = iURL.substring(0, parametersPos);

		String[] parts = iURL.substring(1).split("/");
		if (parts.length < iArgumentCount)
			throw new IllegalArgumentException(iSyntax);

		return parts;
	}

	protected void sendRecordsContent(final OHttpRequest iRequest, final List<ORecord<?>> iRecords) throws IOException {
		final StringWriter buffer = new StringWriter();
		final OJSONWriter json = new OJSONWriter(buffer);
		json.beginObject();

		// WRITE ENTITY SCHEMA IF ANY
		if (iRecords != null && iRecords.size() > 0) {
			ORecord<?> first = iRecords.get(0);
			if (first != null && first instanceof ODocument) {
				ODatabaseDocumentTx db = (ODatabaseDocumentTx) ((ODocument) first).getDatabase();

				String className = ((ODocument) first).getClassName();
				final OClass cls = db.getMetadata().getSchema().getClass(className);
				json.write(" \"schema\": ");
				exportClassSchema(db, json, cls);
			}
		}

		// WRITE RECORDS
		json.beginCollection(1, true, "result");
		if (iRecords != null) {
			int counter = 0;
			String objectJson;
			for (ORecord<?> rec : iRecords) {
				try {
					objectJson = rec.toJSON();

					if (counter++ > 0)
						buffer.append(",\r\n");

					buffer.append(objectJson);
				} catch (Exception e) {
				}
			}
		}
		json.endCollection(1, true);

		json.endObject();

		sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
	}

	protected void sendRecordContent(final OHttpRequest iRequest, final ORecord<?> iRecord) throws IOException {
		if (iRecord != null)
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, iRecord
					.toJSON("id,ver,class"));
	}

	protected void sendBinaryContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iContentType,
			final InputStream iContent, final long iSize) throws IOException {
		sendStatus(iRequest, iCode, iReason);
		sendResponseHeaders(iRequest, iContentType);
		writeLine(iRequest, OHttpUtils.CONTENT_LENGTH + (iSize));
		writeLine(iRequest, null);

		while (iContent.available() > 0)
			iRequest.channel.outStream.write((byte) iContent.read());

		iRequest.channel.flush();
	}

	public void exportClassSchema(final ODatabaseDocumentTx db, final OJSONWriter json, final OClass cls) throws IOException {
		json.beginObject(1, false, null);
		json.writeAttribute(2, true, "id", cls.getId());
		json.writeAttribute(2, true, "name", cls.getName());

		if (cls.properties() != null && cls.properties().size() > 0) {
			json.beginObject(2, true, "properties");
			for (OProperty prop : cls.properties()) {
				json.beginObject(3, true, prop.getName());
				json.writeAttribute(3, true, "id", prop.getId());
				json.writeAttribute(3, true, "name", prop.getName());
				if (prop.getLinkedClass() != null)
					json.writeAttribute(3, true, "linkedClass", prop.getLinkedClass().getName());
				if (prop.getLinkedType() != null)
					json.writeAttribute(3, true, "linkedType", prop.getLinkedType());
				json.writeAttribute(3, true, "type", prop.getType().toString());
				json.writeAttribute(3, true, "mandatory", prop.isMandatory());
				json.writeAttribute(3, true, "notNull", prop.isNotNull());
				json.writeAttribute(3, true, "min", prop.getMin());
				json.writeAttribute(3, true, "max", prop.getMax());
				json.endObject(3, true);
			}
			json.endObject(2, true);
		}
		json.endObject(1, true);
	}

}
