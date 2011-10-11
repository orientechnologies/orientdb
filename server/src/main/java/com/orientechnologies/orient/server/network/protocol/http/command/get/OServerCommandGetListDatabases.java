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

import java.io.IOException;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetListDatabases extends OServerCommandAuthenticatedServerAbstract {
	private static final String[]	NAMES	= { "GET|listDatabases" };

	public OServerCommandGetListDatabases() {
		super("info-server");
	}

	@Override
	public boolean beforeExecute(OHttpRequest iRequest) throws IOException {
		return true;
	}

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		checkSyntax(iRequest.url, 1, "Syntax error: server");

		iRequest.data.commandInfo = "Server status";

		try {
			final ODocument result = new ODocument();
			result.field("databases", OServerMain.server().getAvailableStorageNames().keySet());
			sendRecordContent(iRequest, result);
		} finally {
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
