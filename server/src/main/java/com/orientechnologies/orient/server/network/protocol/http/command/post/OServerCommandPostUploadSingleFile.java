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
import java.util.HashMap;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

	protected ORID								file;

	protected String							fileMapping;

	protected String							fileName;

	protected String							fileType;

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		if (!iRequest.isMultipart) {
			sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Request is not multipart/form-data", null,
					OHttpUtils.CONTENT_TEXT_PLAIN, "Request is not multipart/form-data");
		} else if (iRequest.multipartStream == null || iRequest.multipartStream.available() <= 0) {
			sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Content stream is null or empty", null,
					OHttpUtils.CONTENT_TEXT_PLAIN, "Content stream is null or empty");
		} else {
			iRequest.channel.socket.setSoTimeout(100000);
			buffer = new StringWriter();
			parse(iRequest, new OHttpMultipartContentBaseParser(), new OHttpMultipartFileToRecordContentParser());
			saveRecord(iRequest);
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
		}
		return false;
	}

	@Override
	protected void processBaseContent(OHttpRequest iRequest, String iContentResult, HashMap<String, String> headers) throws Exception {
		if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_NAME)
				&& headers.get(OHttpUtils.MULTIPART_CONTENT_NAME).equals(getDocumentParamenterName())) {
			fileMapping = iContentResult;
		}
	}

	@Override
	protected void processFileContent(OHttpRequest iRequest, ORID iContentResult, HashMap<String, String> headers) throws Exception {
		if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_NAME)
				&& headers.get(OHttpUtils.MULTIPART_CONTENT_NAME).equals(getFileParamenterName())) {
			file = iContentResult;
			if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_FILENAME)) {
				fileName = headers.get(OHttpUtils.MULTIPART_CONTENT_FILENAME);
				if (fileName.charAt(0) == '"') {
					fileName = new String(fileName.substring(1));
				}
				if (fileName.charAt(fileName.length() - 1) == '"') {
					fileName = new String(fileName.substring(0, fileName.length() - 1));
				}
				fileType = headers.get(OHttpUtils.MULTIPART_CONTENT_TYPE);
				OJSONWriter writer = new OJSONWriter(buffer);
				writer.beginObject();
				writer.writeAttribute(1, true, "name", fileName);
				writer.writeAttribute(1, true, "type", fileType);
				writer.writeAttribute(1, true, "rid", file.toString());
				writer.endObject();
			}
		}
	}

	public void saveRecord(OHttpRequest iRequest) throws InterruptedException, IOException {
		if (fileMapping != null) {
			if (file != null) {
				if (fileMapping.contains("$fileName")) {
					fileMapping = new String(fileMapping.replace("$fileName", fileName));
				}
				if (fileMapping.contains("$fileType")) {
					fileMapping = new String(fileMapping.replace("$fileType", fileType));
				}
				if (fileMapping.contains("$file")) {
					fileMapping = new String(fileMapping.replace("$file", "#" + file.toString()));
				}
				ODocument doc = new ODocument(getProfiledDatabaseInstance(iRequest));
				doc.fromJSON(fileMapping);
				doc.save();
			} else {
				sendTextContent(iRequest, OHttpUtils.STATUS_INVALIDMETHOD_CODE, "File cannot be null", null, OHttpUtils.CONTENT_TEXT_PLAIN,
						"File cannot be null");
			}
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
