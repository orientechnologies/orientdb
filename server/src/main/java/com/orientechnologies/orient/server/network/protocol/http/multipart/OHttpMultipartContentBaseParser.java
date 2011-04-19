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
package com.orientechnologies.orient.server.network.protocol.http.multipart;

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;

/**
 * @author luca.molino
 * 
 */
public class OHttpMultipartContentBaseParser implements OHttpMultipartContentParser<String> {

	@Override
	public String parse(OHttpRequest iRequest, Map<String, String> headers, OHttpMultipartContentInputStream in,
			ODatabaseRecord database) throws IOException {
		StringBuilder builder = new StringBuilder();
		while (in.available() > 0) {
			builder.append((char) in.read());
		}
		return builder.toString();
	}

}
