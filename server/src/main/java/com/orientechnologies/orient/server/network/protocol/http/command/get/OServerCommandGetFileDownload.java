/*
 *
 * Copyright 2011 Luca Molino (luca.molino--AT--assetdata.it)
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

import java.io.IOException;
import java.util.Date;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

/**
 * @author luca.molino
 * 
 */
public class OServerCommandGetFileDownload extends OServerCommandAuthenticatedDbAbstract {

	private static final String[]	NAMES	= { "GET|fileDownload/*" };

	@Override
	public boolean execute(OHttpRequest iRequest) throws Exception {
		ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
		String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: fileDownload/<database>/rid/[/<fileName>][/<fileType>].");

		final String fileName = urlParts.length > 3 ? encodeResponseText(urlParts[3]) : "unknown";

		final String fileType = urlParts.length > 5 ? encodeResponseText(urlParts[4]) + '/' + encodeResponseText(urlParts[5])
				: (urlParts.length > 4 ? encodeResponseText(urlParts[4]) : "");

		final String rid = urlParts[2];

		iRequest.data.commandInfo = "Download";
		iRequest.data.commandDetail = rid;

		final ORecordBytes response;

		try {

			response = db.load(new ORecordId(rid));
			if (response != null) {
				sendBinaryFileContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, fileType, response, fileName);
			} else {
				sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Record requested not exists", null,
						OHttpUtils.CONTENT_TEXT_PLAIN, "Record requestes not exists");
			}
		} catch (Exception e) {
			sendTextContent(iRequest, OHttpUtils.STATUS_INTERNALERROR, OHttpUtils.STATUS_ERROR_DESCRIPTION, null,
					OHttpUtils.CONTENT_TEXT_PLAIN, e.getMessage());
		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}

		return false;
	}

	protected void sendBinaryFileContent(final OHttpRequest iRequest, final int iCode, final String iReason,
			final String iContentType, final ORecordBytes record, final String iFileName) throws IOException {
		sendStatus(iRequest, iCode, iReason);
		sendResponseHeaders(iRequest, iContentType);
		writeLine(iRequest, "Content-Disposition: attachment; filename=" + iFileName);
		writeLine(iRequest, "Date: " + new Date());
		writeLine(iRequest, OHttpUtils.HEADER_CONTENT_LENGTH + (record.getSize()));
		writeLine(iRequest, null);

		record.toOutputStream(iRequest.channel.outStream);

		iRequest.channel.flush();
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

	private String encodeResponseText(String iText) {
		iText = new String(iText.replaceAll(" ", "%20"));
		iText = new String(iText.replaceAll("&", "%26"));
		return iText;
	}

}
