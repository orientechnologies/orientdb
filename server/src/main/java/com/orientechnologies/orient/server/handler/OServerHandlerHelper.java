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

import java.util.List;

import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServerMain;

public class OServerHandlerHelper {

	public static void invokeHandlerCallbackOnClientConnection(final OClientConnection connection) {
		final List<OServerHandler> handlers = OServerMain.server().getHandlers();
		if (handlers != null)
			for (OServerHandler handler : handlers) {
				handler.onClientConnection(connection);
			}
	}

	public static void invokeHandlerCallbackOnClientDisconnection(final OClientConnection connection) {
		final List<OServerHandler> handlers = OServerMain.server().getHandlers();
		if (handlers != null)
			for (OServerHandler handler : handlers) {
				handler.onClientDisconnection(connection);
			}
	}

	public static void invokeHandlerCallbackOnBeforeClientRequest(final OClientConnection connection, final byte iRequestType) {
		final List<OServerHandler> handlers = OServerMain.server().getHandlers();
		if (handlers != null)
			for (OServerHandler handler : handlers) {
				handler.onBeforeClientRequest(connection, iRequestType);
			}
	}

	public static void invokeHandlerCallbackOnAfterClientRequest(final OClientConnection connection, final byte iRequestType) {
		final List<OServerHandler> handlers = OServerMain.server().getHandlers();
		if (handlers != null)
			for (OServerHandler handler : handlers) {
				handler.onAfterClientRequest(connection, iRequestType);
			}
	}

	public static void invokeHandlerCallbackOnClientError(final OClientConnection connection, final Throwable iThrowable) {
		final List<OServerHandler> handlers = OServerMain.server().getHandlers();
		if (handlers != null)
			for (OServerHandler handler : handlers) {
				handler.onClientError(connection, iThrowable);
			}
	}

}
