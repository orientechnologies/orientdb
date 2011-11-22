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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public abstract class OServerCommandAbstract implements OServerCommand {

	protected static final String	JSON_FORMAT	= "type,indent:2,rid,version,attribSameRow,class";

	/**
	 * Default constructor. Disable cache of content at HTTP level
	 */
	public OServerCommandAbstract() {
	}

	@Override
	public boolean beforeExecute(final OHttpRequest iRequest) throws IOException {
		return true;
	}

	protected void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
			final String iContentType, final Object iContent) throws IOException {
		sendTextContent(iRequest, iCode, iReason, iHeaders, iContentType, iContent, true);
	}

	protected void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
			final String iContentType, final Object iContent, final boolean iKeepAlive) throws IOException {
		final String content;
		final String contentType;
		if (iRequest.url.indexOf('?') > 0 && iRequest.url.indexOf(OHttpUtils.CALLBACK_PARAMETER_NAME, iRequest.url.indexOf('?')) > 0) {
			String callbackFunction = iRequest.url.substring(iRequest.url.indexOf(OHttpUtils.CALLBACK_PARAMETER_NAME,
					iRequest.url.indexOf('?'))
					+ OHttpUtils.CALLBACK_PARAMETER_NAME.length());
			if (callbackFunction.indexOf('&') > -1)
				callbackFunction = callbackFunction.substring(0, callbackFunction.indexOf('&'));
			content = callbackFunction + "(" + iContent + ")";
			contentType = "text/javascript";
		} else {
			content = iContent != null ? iContent.toString() : null;
			contentType = iContentType;
		}

		final boolean empty = content == null || content.length() == 0;

		sendStatus(iRequest, empty && iCode == 200 ? 204 : iCode, iReason);
		sendResponseHeaders(iRequest, contentType, iKeepAlive);
		if (iHeaders != null)
			writeLine(iRequest, iHeaders);

		final String sessId = iRequest.sessionId != null ? iRequest.sessionId : "-";

		writeLine(iRequest, "Set-Cookie: OSESSIONID=" + sessId + "; Path=/; HttpOnly");

		final byte[] binaryContent = empty ? null : OBinaryProtocol.string2bytes(content);

		writeLine(iRequest, OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : binaryContent.length));

		writeLine(iRequest, null);

		if (!empty)
			writeContent(iRequest, content);

		if (binaryContent != null)
			iRequest.channel.outStream.write(binaryContent);
		iRequest.channel.flush();
	}

	protected void sendStatus(final OHttpRequest iRequest, final int iStatus, final String iReason) throws IOException {
		writeLine(iRequest, iRequest.httpVersion + " " + iStatus + " " + iReason);
	}

	protected void sendResponseHeaders(final OHttpRequest iRequest, final String iContentType) throws IOException {
		sendResponseHeaders(iRequest, iContentType, true);
	}

	protected void sendResponseHeaders(final OHttpRequest iRequest, final String iContentType, final boolean iKeepAlive)
			throws IOException {
		onBeforeResponseHeader(iRequest);

		writeLine(iRequest, "Date: " + new Date());
		writeLine(iRequest, "Content-Type: " + iContentType + "; charset=" + iRequest.executor.getResponseCharSet());
		writeLine(iRequest, "Server: " + iRequest.data.serverInfo);
		writeLine(iRequest, "Connection: " + (iKeepAlive ? "Keep-Alive" : "close"));

		onAfterResponseHeader(iRequest);
	}

	protected void onBeforeResponseHeader(final OHttpRequest iRequest) throws IOException {
		// DEFAULT = DON'T CACHE
		writeLine(iRequest, "Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache");
	}

	protected void onAfterResponseHeader(final OHttpRequest iRequest) throws IOException {
	}

	protected void writeLine(final OHttpRequest iRequest, final String iContent) throws IOException {
		writeContent(iRequest, iContent);
		iRequest.channel.outStream.write(OHttpUtils.EOL);
	}

	protected void writeContent(final OHttpRequest iRequest, final String iContent) throws IOException {
		if (iContent != null)
			iRequest.channel.outStream.write(OBinaryProtocol.string2bytes(iContent));
	}

	protected String[] checkSyntax(String iURL, final int iArgumentCount, final String iSyntax) {
		final int parametersPos = iURL.indexOf('?');
		if (parametersPos > -1)
			iURL = iURL.substring(0, parametersPos);

		String[] parts = iURL.substring(1).split("/");
		if (parts.length < iArgumentCount)
			throw new OHttpRequestException(iSyntax);

		return parts;
	}

	protected void sendRecordsContent(final OHttpRequest iRequest, final List<ORecord<?>> iRecords) throws IOException {
		sendRecordsContent(iRequest, iRecords, null);
	}

	protected void sendRecordsContent(final OHttpRequest iRequest, final List<ORecord<?>> iRecords, final String iFetchPlan)
			throws IOException {
		final StringWriter buffer = new StringWriter();
		final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);
		json.beginObject();

		// WRITE ENTITY SCHEMA IF ANY
		if (iRecords != null && iRecords.size() > 0) {
			ORecord<?> first = iRecords.get(0);
			if (first != null && first instanceof ODocument) {
				ODatabaseRecord db = ((ODocument) first).getDatabase();

				final String className = ((ODocument) first).getClassName();
				exportClassSchema(db, json, db.getMetadata().getSchema().getClass(className));
			}
		}

		final String format = iFetchPlan != null ? JSON_FORMAT + ",fetchPlan:" + iFetchPlan : JSON_FORMAT;

		// WRITE RECORDS
		json.beginCollection(1, true, "result");
		if (iRecords != null) {
			int counter = 0;
			String objectJson;
			for (ORecord<?> rec : iRecords) {
				if (rec != null)
					try {
						objectJson = rec.toJSON(format);

						if (counter++ > 0)
							buffer.append(", ");

						buffer.append(objectJson);
					} catch (Exception e) {
						OLogManager.instance().error(this, "Error transforming record " + rec.getIdentity() + " to JSON", e);
					}
			}
		}
		json.endCollection(1, true);

		json.endObject();

		sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
	}

	protected void sendRecordContent(final OHttpRequest iRequest, final ORecord<?> iRecord) throws IOException {
		sendRecordContent(iRequest, iRecord, null);
	}

	protected void sendRecordContent(final OHttpRequest iRequest, final ORecord<?> iRecord, String iFetchPlan) throws IOException {
		final String format = iFetchPlan != null ? JSON_FORMAT + ",fetchPlan:" + iFetchPlan : JSON_FORMAT;
		if (iRecord != null)
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, iRecord.toJSON(format));
	}

	protected void sendBinaryContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iContentType,
			final InputStream iContent, final long iSize) throws IOException {
		sendStatus(iRequest, iCode, iReason);
		sendResponseHeaders(iRequest, iContentType);
		writeLine(iRequest, OHttpUtils.HEADER_CONTENT_LENGTH + (iSize));
		writeLine(iRequest, null);

		while (iContent != null && iContent.available() > 0)
			iRequest.channel.outStream.write((byte) iContent.read());

		iRequest.channel.flush();
	}

	public void exportClassSchema(final ODatabaseRecord db, final OJSONWriter json, final OClass cls) throws IOException {
		if (cls == null)
			return;

		json.write(" \"schema\": ");
		json.beginObject(1, false, null);
		json.writeAttribute(2, true, "name", cls.getName());

		if (cls.properties() != null && cls.properties().size() > 0) {
			json.beginObject(2, true, "properties");
			for (OProperty prop : cls.properties()) {
				json.beginObject(3, true, prop.getName());
				json.writeAttribute(4, true, "name", prop.getName());
				if (prop.getLinkedClass() != null)
					json.writeAttribute(4, true, "linkedClass", prop.getLinkedClass().getName());
				if (prop.getLinkedType() != null)
					json.writeAttribute(4, true, "linkedType", prop.getLinkedType().toString());
				json.writeAttribute(4, true, "type", prop.getType().toString());
				json.writeAttribute(4, true, "mandatory", prop.isMandatory());
				json.writeAttribute(4, true, "notNull", prop.isNotNull());
				json.writeAttribute(4, true, "min", prop.getMin());
				json.writeAttribute(4, true, "max", prop.getMax());
				json.endObject(3, true);
			}
			json.endObject(2, true);
		}
		json.endObject(1, true);
	}

	protected String nextChainUrl(final String iCurrentUrl) {
		if (!iCurrentUrl.contains("/"))
			return iCurrentUrl;

		return iCurrentUrl.startsWith("/") ? iCurrentUrl.substring(iCurrentUrl.indexOf('/', 1)) : iCurrentUrl.substring(iCurrentUrl
				.indexOf("/"));
	}

}
