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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.HashMap;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartContentBaseParser;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartFileToRecordContentParser;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartRequestCommand;

/**
 * @author luca.molino
 * 
 */
public class OServerCommandPostUploadSingleFile extends OHttpMultipartRequestCommand<String, ORID> {

	private static final String[]	NAMES	= { "POST|uploadSingleFile/*" };

	protected StringWriter				buffer;

	protected OJSONWriter					writer;

	protected ORID								fileRID;

	protected String							fileDocument;

	protected String							fileName;

	protected String							fileType;

	protected long								now;

	protected ODatabaseRecord			database;

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		if (!iRequest.isMultipart) {
			sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Request is not multipart/form-data", null,
					OHttpUtils.CONTENT_TEXT_PLAIN, "Request is not multipart/form-data");
		} else if (iRequest.multipartStream == null || iRequest.multipartStream.available() <= 0) {
			sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Content stream is null or empty", null,
					OHttpUtils.CONTENT_TEXT_PLAIN, "Content stream is null or empty");
		} else {
			database = getProfiledDatabaseInstance(iRequest);
			try {
				buffer = new StringWriter();
				writer = new OJSONWriter(buffer);
				writer.beginObject();
				parse(iRequest, new OHttpMultipartContentBaseParser(), new OHttpMultipartFileToRecordContentParser(), database);
				saveRecord(iRequest);
				writer.flush();
				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
			} finally {
				if (database != null)
					database.close();
				database = null;
				if (buffer != null)
					buffer.close();
				buffer = null;
				if (writer != null)
					writer.close();
				writer = null;
				fileDocument = null;
				fileName = null;
				fileType = null;
				if (fileRID != null)
					fileRID.reset();
				fileRID = null;
			}
		}
		return false;
	}

	@Override
	protected void processBaseContent(OHttpRequest iRequest, String iContentResult, HashMap<String, String> headers) throws Exception {
		if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_NAME)
				&& headers.get(OHttpUtils.MULTIPART_CONTENT_NAME).equals(getDocumentParamenterName())) {
			fileDocument = iContentResult;
		}
	}

	@Override
	protected void processFileContent(OHttpRequest iRequest, ORID iContentResult, HashMap<String, String> headers) throws Exception {
		if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_NAME)
				&& headers.get(OHttpUtils.MULTIPART_CONTENT_NAME).equals(getFileParamenterName())) {
			fileRID = iContentResult;
			if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_FILENAME)) {
				fileName = headers.get(OHttpUtils.MULTIPART_CONTENT_FILENAME);
				if (fileName.charAt(0) == '"') {
					fileName = new String(fileName.substring(1));
				}
				if (fileName.charAt(fileName.length() - 1) == '"') {
					fileName = new String(fileName.substring(0, fileName.length() - 1));
				}
				fileType = headers.get(OHttpUtils.MULTIPART_CONTENT_TYPE);

				final Calendar cal = Calendar.getInstance();
				final DateFormat formatter = database.getStorage().getConfiguration().getDateFormatInstance();
				now = cal.getTimeInMillis();

				writer.beginObject("uploadedFile");
				writer.writeAttribute(1, true, "name", fileName);
				writer.writeAttribute(1, true, "type", fileType);
				writer.writeAttribute(1, true, "date", formatter.format(cal.getTime()));
				writer.writeAttribute(1, true, "rid", fileRID);
				writer.endObject();
			}
		}
	}

	public void saveRecord(OHttpRequest iRequest) throws InterruptedException, IOException {
		if (fileDocument != null) {
			if (fileRID != null) {
				if (fileDocument.contains("$now")) {
					fileDocument = fileDocument.replace("$now", String.valueOf(now));
				}
				if (fileDocument.contains("$fileName")) {
					fileDocument = fileDocument.replace("$fileName", fileName);
				}
				if (fileDocument.contains("$fileType")) {
					fileDocument = fileDocument.replace("$fileType", fileType);
				}
				if (fileDocument.contains("$file")) {
					fileDocument = fileDocument.replace("$file", fileRID.toString());
				}
				ODocument doc = new ODocument(database);
				doc.fromJSON(fileDocument);
				doc.save();
				writer.beginObject("updatedDocument");
				writer.writeAttribute(1, true, "rid", doc.getIdentity().toString());
				writer.endObject();
				writer.endObject();
			} else {
				sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "File cannot be null", null, OHttpUtils.CONTENT_TEXT_PLAIN,
						"File cannot be null");
			}

			fileDocument = null;
		} else {
			if (fileRID != null) {
				ORecordBytes file = new ORecordBytes(fileRID);
				database.delete(file);
			}
			sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Document template cannot be null", null,
					OHttpUtils.CONTENT_TEXT_PLAIN, "Document template cannot be null");
		}
	}

	@Override
	protected String getDocumentParamenterName() {
		return "linkValue";
	}

	@Override
	protected String getFileParamenterName() {
		return "file";
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
