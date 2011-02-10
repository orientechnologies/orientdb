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
package com.orientechnologies.orient.server.network.protocol.http;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles session information.
 * 
 * @author Luca Garulli
 */
public class OHttpSession {
	private String							id;
	private final long					createdOn;
	private long								lastUpdatedOn;
	private final String				databaseName;
	private final String				userName;
	private Map<Object, Object>	parameters;

	protected OHttpSession(final String iId, final String iDatabaseName, final String iUserName) {
		id = iId;
		createdOn = System.currentTimeMillis();
		lastUpdatedOn = createdOn;

		databaseName = iDatabaseName;
		userName = iUserName;
	}

	public long getCreatedOn() {
		return createdOn;
	}

	public long getUpdatedOn() {
		return lastUpdatedOn;
	}

	public OHttpSession updateLastUpdatedOn() {
		this.lastUpdatedOn = System.currentTimeMillis();
		return this;
	}

	public Object getParameters(final Object iKey) {
		if (this.parameters == null)
			return null;

		return parameters.entrySet();
	}

	public Object getParameter(final Object iKey) {
		if (this.parameters == null)
			return null;

		return parameters.get(iKey);
	}

	public OHttpSession setParameter(final Object iKey, final Object iValue) {
		if (this.parameters == null)
			this.parameters = new HashMap<Object, Object>();

		if (iValue == null)
			this.parameters.remove(iKey);
		else
			this.parameters.put(iKey, iValue);
		return this;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getUserName() {
		return userName;
	}

	public String getId() {
		return id;
	}
}
