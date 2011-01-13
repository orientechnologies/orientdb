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

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetDisconnect extends OServerCommandAbstract {
	private static final String[]	NAMES	= { "GET|disconnect" };

	@Override
	public boolean beforeExecute(OHttpRequest iRequest) throws IOException {
		return true;
	}

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		checkSyntax(iRequest.url, 1, "Syntax error: disconnect");

		iRequest.data.commandInfo = "Disconnect";
		iRequest.data.commandDetail = null;

		if (iRequest.sessionId != null) {
			OHttpSessionManager.getInstance().removeSession(iRequest.sessionId);
			iRequest.sessionId = "!";
		}

		sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, null, OHttpUtils.CONTENT_TEXT_PLAIN,
				"Logged out", false);
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
