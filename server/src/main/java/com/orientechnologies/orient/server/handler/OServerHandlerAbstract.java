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
package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public abstract class OServerHandlerAbstract implements OServerHandler {
	public void startup() {
	}

	public void shutdown() {
	}

	public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
	}

	public void onClientConnection(final OClientConnection iConnection) {
	}

	public void onClientDisconnection(final OClientConnection iConnection) {
	}

	public void onBeforeClientRequest(final OClientConnection iConnection, final byte iRequestType) {
	}

	public void onAfterClientRequest(final OClientConnection iConnection, final byte iRequestType) {
	}

	public void onClientError(final OClientConnection iConnection, final Throwable iThrowable) {
	}
}
