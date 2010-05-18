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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Handles the HTTP session such as a real HTTP Server.
 * 
 * @author Luca Garulli
 * 
 */
public class OHttpSessionManager {
	private static final OHttpSessionManager	instance	= new OHttpSessionManager();
	private Map<String, Object>								sessions	= new HashMap<String, Object>();

	protected OHttpSessionManager() {
	}

	public Object[] getSessions(final String iId) {
		return sessions.keySet().toArray();
	}

	public Object getSession(final String iId) {
		return sessions.get(iId);
	}

	public String createSession() {
		final String id = "OS" + System.currentTimeMillis() + new Random().nextLong();
		sessions.put(id, Boolean.TRUE);
		return id;
	}

	public static OHttpSessionManager getInstance() {
		return instance;
	}

	public Object removeSession(final String iSessionId) {
		return sessions.remove(iSessionId);
	}
}
