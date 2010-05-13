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
import java.util.Date;
import java.util.List;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public abstract class OServerCommandAbstract implements OServerCommand {

	protected void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
			final String iContentType, final String iContent) throws IOException {
		sendStatus(iRequest, iCode, iReason);
		sendResponseHeaders(iRequest, iContentType);
		if (iHeaders != null)
			writeLine(iRequest, iHeaders);
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
		StringBuilder buffer = new StringBuilder();
		buffer.append("{ \"result\": [\r\n");

		if (iRecords != null) {
			int counter = 0;
			String json;
			for (ORecord<?> rec : iRecords) {
				try {
					json = rec.toJSON();

					if (counter++ > 0)
						buffer.append(",\r\n");

					buffer.append(json);
				} catch (Exception e) {
				}
			}
		}

		buffer.append("] }");

		sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
	}

	protected void sendRecordContent(final OHttpRequest iRequest, final ORecord<?> iRecord) throws IOException {
		if (iRecord != null)
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, iRecord
					.toJSON("id,ver,class"));
	}

	protected void handleError(final OHttpRequest iRequest, Exception e) {
		int errorCode = 500;

		if (e instanceof ORecordNotFoundException)
			errorCode = 404;
		else if (e instanceof OLockException)
			e = (Exception) e.getCause();

		final String msg = e.getMessage() != null ? e.getMessage() : "Internal error";

		try {
			sendTextContent(iRequest, errorCode, "Error", null, OHttpUtils.CONTENT_TEXT_PLAIN, msg);
		} catch (IOException e1) {
			// TODO
			// sendShutdown();
		}
	}

	protected void sendBinaryContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iContentType,
			final InputStream iContent, final long iSize) throws IOException {
		sendStatus(iRequest, iCode, iReason);
		sendResponseHeaders(iRequest, iContentType);
		writeLine(iRequest, OHttpUtils.CONTENT_LENGTH + (iSize));
		writeLine(iRequest, null);

		int i = 0;
		while (iContent.available() > 0) {
			iRequest.channel.outStream.write((byte) iContent.read());
			++i;
		}

		iRequest.channel.flush();
	}

}
