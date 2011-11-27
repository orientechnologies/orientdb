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
package com.orientechnologies.orient.core.command;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OCommandManager {
	private Map<String, Class<? extends OCommandRequest>>															commandRequesters	= new HashMap<String, Class<? extends OCommandRequest>>();
	private Map<Class<? extends OCommandRequest>, Class<? extends OCommandExecutor>>	commandReqExecMap	= new HashMap<Class<? extends OCommandRequest>, Class<? extends OCommandExecutor>>();
	private static OCommandManager																										instance					= new OCommandManager();

	protected OCommandManager() {
		registerRequester("sql", OCommandSQL.class);

		registerExecutor(OSQLAsynchQuery.class, OCommandExecutorSQLDelegate.class);
		registerExecutor(OSQLSynchQuery.class, OCommandExecutorSQLDelegate.class);
		registerExecutor(OCommandSQL.class, OCommandExecutorSQLDelegate.class);
	}

	public OCommandManager registerRequester(final String iType, final Class<? extends OCommandRequest> iRequest) {
		commandRequesters.put(iType, iRequest);
		return this;
	}

	public boolean existsRequester(final String iType) {
		return commandRequesters.containsKey(iType);
	}

	public OCommandRequest getRequester(final String iType) {
		final Class<? extends OCommandRequest> reqClass = commandRequesters.get(iType);

		if (reqClass == null)
			throw new IllegalArgumentException("Cannot find a command requester for type: " + iType);

		try {
			return reqClass.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot create the command requester of class " + reqClass + " for type: " + iType, e);
		}
	}

	public OCommandManager registerExecutor(final Class<? extends OCommandRequest> iRequest,
			final Class<? extends OCommandExecutor> iExecutor) {
		commandReqExecMap.put(iRequest, iExecutor);
		return this;
	}

	public OCommandExecutor getExecutor(OCommandRequestInternal iCommand) {
		final Class<? extends OCommandExecutor> executorClass = commandReqExecMap.get(iCommand.getClass());

		if (executorClass == null)
			throw new IllegalArgumentException("Cannot find a command executor for the command request: " + iCommand);

		try {
			return executorClass.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot create the command executor of class " + executorClass
					+ " for the command request: " + iCommand, e);
		}
	}

	public static OCommandManager instance() {
		return instance;
	}
}
