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

import com.orientechnologies.common.util.OService;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

/**
 * Server handler interface. Used when configured in the server configuration.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OServerHandler extends OService {
	/**
	 * Callback invoked when a client connection begins.
	 */
	public void onClientConnection(OClientConnection iConnection);

	/**
	 * Callback invoked when a client connection ends.
	 */
	public void onClientDisconnection(OClientConnection iConnection);

	/**
	 * Callback invoked before a client request is processed.
	 */
	public void onBeforeClientRequest(OClientConnection iConnection, byte iRequestType);

	/**
	 * Callback invoked after a client request is processed.
	 */
	public void onAfterClientRequest(OClientConnection iConnection, byte iRequestType);

	/**
	 * Callback invoked when a client connection has errors.
	 * 
	 * @param iThrowable
	 *          Throwable instance received
	 */
	public void onClientError(OClientConnection iConnection, Throwable iThrowable);

	/**
	 * Configures the handler. Called at startup.
	 */
	public void config(OServer oServer, OServerParameterConfiguration[] iParams);
}
