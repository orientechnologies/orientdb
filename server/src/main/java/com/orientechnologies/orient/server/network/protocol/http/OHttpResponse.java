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
package com.orientechnologies.orient.server.network.protocol.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

/**
 * Maintains information about current HTTP response.
 * 
 * @author Luca Garulli
 * 
 */
public class OHttpResponse {
	public static final String	JSON_FORMAT		= "type,indent:2,rid,version,attribSameRow,class";
	public static final char[]	URL_SEPARATOR	= { '/' };

	private final OutputStream	out;
	public final String					httpVersion;
	public String								headers;
	public String[]							additionalHeaders;
	public String								characterSet;
	public String								serverInfo;
	public String								sessionId;
	public String								callbackFunction;

	public OHttpResponse(final OutputStream iOutStream, final String iHttpVersion, final String[] iAdditionalHeaders,
			final String iResponseCharSet, final String iServerInfo, final String iSessionId, final String iCallbackFunction) {
		out = iOutStream;
		httpVersion = iHttpVersion;
		additionalHeaders = iAdditionalHeaders;
		characterSet = iResponseCharSet;
		serverInfo = iServerInfo;
		sessionId = iSessionId;
		callbackFunction = iCallbackFunction;
	}

	public void send(final int iCode, final String iReason, final String iContentType, final Object iContent, final String iHeaders)
			throws IOException {
		send(iCode, iReason, iContentType, iContent, iHeaders, true);
	}

	public void send(final int iCode, final String iReason, final String iContentType, final Object iContent, final String iHeaders,
			final boolean iKeepAlive) throws IOException {
		final String content;
		final String contentType;

		if (callbackFunction != null) {
			content = callbackFunction + "(" + iContent + ")";
			contentType = "text/javascript";
		} else {
			content = iContent != null ? iContent.toString() : null;
			contentType = iContentType;
		}

		final boolean empty = content == null || content.length() == 0;

		writeStatus(empty && iCode == 200 ? 204 : iCode, iReason);
		writeHeaders(contentType, iKeepAlive);

		if (additionalHeaders != null)
			for (String h : additionalHeaders)
				writeLine(h);

		if (iHeaders != null)
			writeLine(iHeaders);

		final String sessId = sessionId != null ? sessionId : "-";

		writeLine("Set-Cookie: " + OHttpUtils.OSESSIONID + "=" + sessId + "; Path=/; HttpOnly");

		final byte[] binaryContent = empty ? null : OBinaryProtocol.string2bytes(content);

		writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : binaryContent.length));

		writeLine(null);

		if (binaryContent != null)
			out.write(binaryContent);
		out.flush();
	}

	public void writeStatus(final int iStatus, final String iReason) throws IOException {
		writeLine(httpVersion + " " + iStatus + " " + iReason);
	}

	public void writeHeaders(final String iContentType) throws IOException {
		writeHeaders(iContentType, true);
	}

	public void writeHeaders(final String iContentType, final boolean iKeepAlive) throws IOException {
		if (headers != null)
			writeLine(headers);

		writeLine("Date: " + new Date());
		writeLine("Content-Type: " + iContentType + "; charset=" + characterSet);
		writeLine("Server: " + serverInfo);
		writeLine("Connection: " + (iKeepAlive ? "Keep-Alive" : "close"));

		// INCLUDE COMMON CUSTOM HEADERS
		if (additionalHeaders != null)
			for (String h : additionalHeaders)
				writeLine(h);
	}

	public void writeLine(final String iContent) throws IOException {
		writeContent(iContent);
		out.write(OHttpUtils.EOL);
	}

	public void writeContent(final String iContent) throws IOException {
		if (iContent != null)
			out.write(OBinaryProtocol.string2bytes(iContent));
	}

	@SuppressWarnings("unchecked")
	public void writeResult(Object iResult) throws InterruptedException, IOException {
		if (iResult == null)
			send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", OHttpUtils.CONTENT_TEXT_PLAIN, null, null, true);
		else {
			if (iResult instanceof OIdentifiable) {
				// CONVERT SIGLE VLUE IN A COLLECTION
				final List<OIdentifiable> resultSet = new ArrayList<OIdentifiable>();
				resultSet.add((OIdentifiable) iResult);
				iResult = resultSet;
			}

			if (iResult instanceof Iterable<?>)
				iResult = ((Iterable<OIdentifiable>) iResult).iterator();
			else if (iResult.getClass().isArray())
				iResult = OMultiValue.getMultiValueIterator(iResult);

			if (iResult instanceof Iterator<?>)
				writeRecords((Iterator<OIdentifiable>) iResult);
			else if (iResult == null || iResult instanceof Integer)
				send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, iResult, null);
			else
				send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, iResult.toString(), null);
		}
	}

	public void writeRecords(final Iterable<OIdentifiable> iRecords) throws IOException {
		if (iRecords == null)
			return;

		writeRecords(iRecords.iterator(), null);
	}

	public void writeRecords(final Iterable<OIdentifiable> iRecords, final String iFetchPlan) throws IOException {
		if (iRecords == null)
			return;

		writeRecords(iRecords.iterator(), iFetchPlan);
	}

	public void writeRecords(final Iterator<OIdentifiable> iRecords) throws IOException {
		writeRecords(iRecords, null);
	}

	public void writeRecords(final Iterator<OIdentifiable> iRecords, final String iFetchPlan) throws IOException {
		if (iRecords == null)
			return;

		final StringWriter buffer = new StringWriter();
		final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);
		json.beginObject();

		final String format = iFetchPlan != null ? JSON_FORMAT + ",fetchPlan:" + iFetchPlan : JSON_FORMAT;

		// WRITE RECORDS
		json.beginCollection(1, true, "result");
		formatMultiValue(iRecords, buffer, format);
		json.endCollection(1, true);

		json.endObject();

		send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
	}

	public void formatMultiValue(final Iterator<OIdentifiable> iIterator, final StringWriter buffer, final String format) {
		if (iIterator != null) {
			int counter = 0;
			String objectJson;

			while (iIterator.hasNext()) {
				final OIdentifiable rec = iIterator.next();
				if (rec != null)
					try {
						objectJson = rec.getRecord().toJSON(format);

						if (counter++ > 0)
							buffer.append(", ");

						buffer.append(objectJson);
					} catch (Exception e) {
						OLogManager.instance().error(this, "Error transforming record " + rec.getIdentity() + " to JSON", e);
					}
			}
		}
	}

	public void writeRecord(final ORecord<?> iRecord) throws IOException {
		writeRecord(iRecord, null);
	}

	public void writeRecord(final ORecord<?> iRecord, String iFetchPlan) throws IOException {
		final String format = iFetchPlan != null ? JSON_FORMAT + ",fetchPlan:" + iFetchPlan : JSON_FORMAT;
		if (iRecord != null)
			send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, iRecord.toJSON(format), null);
	}

	public void sendStream(final int iCode, final String iReason, final String iContentType, final InputStream iContent,
			final long iSize) throws IOException {
		writeStatus(iCode, iReason);
		writeHeaders(iContentType);
		writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (iSize));
		writeLine(null);

		if (iContent != null) {
			int b;
			while ((b = iContent.read()) > -1)
				out.write(b);
		}

		out.flush();
	}

	/**
	 * Stores additional headers to send
	 * 
	 * @param iHeader
	 */
	public void setHeader(final String iHeader) {
		headers = iHeader;
	}

	public OutputStream getOutputStream() {
		return out;
	}

	public void flush() throws IOException {
		out.flush();
	}
}
