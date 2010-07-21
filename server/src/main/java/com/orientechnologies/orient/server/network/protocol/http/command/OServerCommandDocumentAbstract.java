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

import java.util.Map;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;

public abstract class OServerCommandDocumentAbstract extends OServerCommandAuthenticatedDbAbstract {

	protected String bindToFields(final OHttpRequest iRequest, final Map<String, String> iFields, final ORecordId iRid)
			throws Exception {
		if (iRequest.content == null)
			throw new IllegalArgumentException("HTTP Request content is empty");

		final String req = iRequest.content;

		// PARSE PARAMETERS
		String className = null;

		final String[] params = req.split("&");
		String value;

		for (String p : params) {
			if (OStringSerializerHelper.contains(p, '=')) {
				String[] pairs = p.split("=");
				value = pairs.length == 1 ? null : pairs[1];

				if ("0".equals(pairs[0]) && iRid != null)
					iRid.fromString(value);
				else if ("1".equals(pairs[0]))
					className = value;
				else if (pairs[0].startsWith("_") || pairs[0].equals("id"))
					continue;
				else if (iFields != null) {
					iFields.put(pairs[0], value);
				}
			}
		}
		return className;
	}
}
