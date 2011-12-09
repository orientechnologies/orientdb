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
package com.orientechnologies.orient.graph.gremlin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;

/**
 * Executes a GREMLIN command.
 * 
 * @author Luca Garulli
 */
public class OCommandGremlinExecutor extends OCommandExecutorAbstract {
	private OGraphDatabase	db;

	@SuppressWarnings("unchecked")
	@Override
	public <RET extends OCommandExecutor> RET parse(OCommandRequestText iRequest) {
		text = iRequest.getText();
		db = OGremlinHelper.getGraphDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());
		return (RET) this;
	}

	@Override
	public Object execute(final Map<Object, Object> iArgs) {
		parameters = iArgs;
		final List<Object> result = new ArrayList<Object>();
		final Object scriptResult = OGremlinHelper.execute(db, text, parameters, new HashMap<Object, Object>(), result, null, null);
		return scriptResult != null ? scriptResult : result;
	}
}
