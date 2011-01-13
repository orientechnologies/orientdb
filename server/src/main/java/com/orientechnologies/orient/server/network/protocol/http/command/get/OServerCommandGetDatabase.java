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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;

public class OServerCommandGetDatabase extends OServerCommandGetConnect {
	private static final String[]	NAMES	= { "GET|database/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: database/<database>");

		iRequest.data.commandInfo = "Database info";
		iRequest.data.commandDetail = urlParts[1];

		exec(iRequest, urlParts);
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
