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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public abstract class OServerCommandAuthenticatedAbstract extends OServerCommandAbstract {

	public boolean beforeExecute(final OHttpRequest iRequest) throws IOException {
		if (iRequest.sessionId == null) {
			// NO SESSION
			if (iRequest.authorization == null) {
				// UNAUTHORIZED
				sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION,
						"WWW-Authenticate: Basic realm=\"OrientDB Server\"", OHttpUtils.CONTENT_TEXT_PLAIN, null);
				return false;
			} else {
				// AUTHENTICATED: CREATE THE SESSION
				iRequest.sessionId = OHttpSessionManager.getInstance().createSession();
			}
		} else {
			if (iRequest.sessionId.length() > 1 && OHttpSessionManager.getInstance().getSession(iRequest.sessionId) == null) {
				// SESSION EXPIRED
				iRequest.sessionId = null;
				sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION,
						"WWW-Authenticate: Basic realm=\"OrientDB Server\"", OHttpUtils.CONTENT_TEXT_PLAIN, "401 Unauthorized.");
				return false;
			}
		}

		return true;
	}

	protected ODatabaseDocumentTx getProfiledDatabaseInstance(final OHttpRequest iRequest, final String iDatabaseURL)
			throws InterruptedException {
		if (iRequest.authorization == null)
			throw new OSecurityAccessException("No user and password received");

		return OSharedDocumentDatabase.acquireDatabase(iDatabaseURL + ":" + iRequest.authorization);
	}
}
