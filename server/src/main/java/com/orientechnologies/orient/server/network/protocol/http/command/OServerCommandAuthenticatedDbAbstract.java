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
import java.util.List;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

/**
 * Database based authenticated command. Authenticats against the database taken as second parameter of the URL. The URL must be in
 * this format:
 * 
 * <pre>
 * <command>/<database>[/...]
 * </pre>
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OServerCommandAuthenticatedDbAbstract extends OServerCommandAbstract {

	public static final char		DBNAME_DIR_SEPARATOR		= '$';
	public static final String	SESSIONID_UNAUTHORIZED	= "-";
	public static final String	SESSIONID_LOGOUT				= "!";

	@Override
	public boolean beforeExecute(final OHttpRequest iRequest) throws IOException {
		final String[] urlParts = iRequest.url.substring(1).split("/");
		if (urlParts.length < 2)
			throw new OHttpRequestException("Syntax error in URL. Expected is: <command>/<database>[/...]");

		iRequest.databaseName = urlParts[1];

		iRequest.databaseName = iRequest.databaseName.replace(DBNAME_DIR_SEPARATOR, '/');

		if (iRequest.sessionId == null || (iRequest.sessionId != null && iRequest.sessionId.length() == 1)) {
			// NO SESSION
			if (iRequest.authorization == null || SESSIONID_LOGOUT.equals(iRequest.sessionId)) {
				sendAuthorizationRequest(iRequest, iRequest.databaseName);
				return false;
			} else {
				if (iRequest.url != null)
					return authenticate(iRequest, iRequest.databaseName);
			}
		} else {
			// CHECK THE SESSION VALIDITY
			if (iRequest.sessionId.length() > 1 && OHttpSessionManager.getInstance().getSession(iRequest.sessionId) == null) {
				// SESSION EXPIRED
				sendAuthorizationRequest(iRequest, iRequest.databaseName);
				return false;
			}
			return true;
		}

		return false;
	}

	private boolean authenticate(final OHttpRequest iRequest, final String iDatabaseName) throws IOException {
		ODatabaseDocumentTx db = null;
		try {
			final List<String> parts = OStringSerializerHelper.split(iRequest.authorization, ':');

			db = OSharedDocumentDatabase.acquire(iDatabaseName, parts.get(0), parts.get(1));

			// AUTHENTICATED: CREATE THE SESSION
			iRequest.sessionId = OHttpSessionManager.getInstance().createSession(iDatabaseName, parts.get(0));
			return true;

		} catch (OSecurityAccessException e) {
			// WRONG USER/PASSWD
		} catch (OLockException e) {
			OLogManager.instance().error(this, "Can't access to the database", ODatabaseException.class, e);
		} catch (InterruptedException e) {
			OLogManager.instance().error(this, "Can't access to the database", ODatabaseException.class, e);
		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
			else
				// WRONG USER/PASSWD
				sendAuthorizationRequest(iRequest, iDatabaseName);
		}
		return false;
	}

	private void sendAuthorizationRequest(final OHttpRequest iRequest, final String iDatabaseName) throws IOException {
		// UNAUTHORIZED
		iRequest.sessionId = SESSIONID_UNAUTHORIZED;
		sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION,
				"WWW-Authenticate: Basic realm=\"OrientDB db-" + iDatabaseName + "\"", OHttpUtils.CONTENT_TEXT_PLAIN, "401 Unauthorized.",
				false);
	}

	protected ODatabaseDocumentTx getProfiledDatabaseInstance(final OHttpRequest iRequest) throws InterruptedException {
		if (iRequest.authorization == null)
			throw new OSecurityAccessException(iRequest.databaseName, "No user and password received");

		final List<String> parts = OStringSerializerHelper.split(iRequest.authorization, ':');

		return OSharedDocumentDatabase.acquire(iRequest.databaseName, parts.get(0), parts.get(1));
	}
}
