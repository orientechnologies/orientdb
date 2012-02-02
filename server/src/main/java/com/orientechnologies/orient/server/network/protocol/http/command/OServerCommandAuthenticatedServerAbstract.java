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

import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

/**
 * Server based authenticated commands. Authenticates against the OrientDB server users found in configuration.
 * 
 * @author Luca Garulli
 */
public abstract class OServerCommandAuthenticatedServerAbstract extends OServerCommandAbstract {

	private static final String	SESSIONID_UNAUTHORIZED	= "-";
	private static final String	SESSIONID_LOGOUT				= "!";

	private final String				resource;

	protected OServerCommandAuthenticatedServerAbstract(final String iRequiredResource) {
		resource = iRequiredResource;
	}

	@Override
	public boolean beforeExecute(final OHttpRequest iRequest) throws IOException {
		return authenticate(iRequest, true);
	}

	protected boolean authenticate(final OHttpRequest iRequest, final boolean iAskForAuthentication) throws IOException {
		if (checkGuestAccess())
			// GUEST ACCESSES TO THE RESOURCE: OK ALSO WITHOUT AN AUTHENTICATION
			return true;

		if (iAskForAuthentication)
			if (iRequest.authorization == null || SESSIONID_LOGOUT.equals(iRequest.sessionId)) {
				// NO AUTHENTICATION AT ALL
				sendAuthorizationRequest(iRequest);
				return false;
			}

		if (iRequest.authorization != null) {
			// GET CREDENTIALS
			final String[] authParts = iRequest.authorization.split(":");
			if (authParts.length == 2 && OServerMain.server().authenticate(authParts[0], authParts[1], resource))
				// AUTHORIZED
				return true;
		}

		// NON AUTHORIZED FOR RESOURCE
		sendNotAuthorizedResponse(iRequest);
		return false;
	}

	protected boolean checkGuestAccess() {
		return OServerMain.server().authenticate(OServerConfiguration.SRV_ROOT_GUEST, null, resource);
	}

	protected void sendNotAuthorizedResponse(final OHttpRequest iRequest) throws IOException {
		// UNAUTHORIZED
		iRequest.sessionId = SESSIONID_UNAUTHORIZED;
		sendTextContent(iRequest, OHttpUtils.STATUS_FORBIDDEN_CODE, OHttpUtils.STATUS_FORBIDDEN_DESCRIPTION,
				"WWW-Authenticate: Basic realm=\"OrientDB Server\"", OHttpUtils.CONTENT_TEXT_PLAIN, "403 Forbidden.", false);
	}

	protected void sendAuthorizationRequest(final OHttpRequest iRequest) throws IOException {
		// UNAUTHORIZED
		iRequest.sessionId = SESSIONID_UNAUTHORIZED;
		sendTextContent(iRequest, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION,
				"WWW-Authenticate: Basic realm=\"OrientDB Server\"", OHttpUtils.CONTENT_TEXT_PLAIN, "401 Unauthorized.", false);
	}
}
