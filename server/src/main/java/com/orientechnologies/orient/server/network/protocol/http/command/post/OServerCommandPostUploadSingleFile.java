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

import java.io.StringWriter;
import java.util.HashMap;

import com.orientechnologies.orient.core.id.ORID;
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
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
		}
		return false;
	}

	@Override
	protected void processBaseContent(OHttpRequest iRequest, String iContentResult, HashMap<String, String> headers) throws Exception {
		fileMapping = iContentResult;
	}

	@Override
	protected void processFileContent(OHttpRequest iRequest, ORID iContentResult, HashMap<String, String> headers) throws Exception {
		file = iContentResult;
		if (headers.containsKey(OHttpUtils.MULTIPART_CONTENT_FILENAME)) {
			OJSONWriter writer = new OJSONWriter(buffer);
			writer.beginObject();
			writer.writeAttribute(1, true, "name", headers.get(OHttpUtils.MULTIPART_CONTENT_FILENAME));
			writer.writeAttribute(1, true, "type", headers.get(OHttpUtils.MULTIPART_CONTENT_TYPE));
			writer.writeAttribute(1, true, "rid", file.toString());
			writer.endObject();
		}
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
