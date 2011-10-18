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

import javax.script.ScriptContext;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientEdge;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientElement;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientVertex;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngine;

/**
 * Executes a GREMLIN expression as function of SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionGremlin extends OSQLFunctionAbstract {
	public static final String	NAME	= "gremlin";
	private List<Object>				result;
	private Map<Object, Object>	currentParameters;

	public OSQLFunctionGremlin() {
		super(NAME, 1, 1);
	}

	public Object execute(final ORecord<?> iCurrentRecord, final Object[] iParameters, final OCommandExecutor iRequester) {
		if (!(iCurrentRecord instanceof ODocument))
			// NOT DOCUMENT OR GRAPHDB? IGNORE IT
			return null;

		final OGraphDatabase db = OGremlinHelper.getGraphDatabase(iCurrentRecord.getDatabase());

		if (result == null)
			result = new ArrayList<Object>();

		if (iRequester.getParameters() != null)
			currentParameters = new HashMap<Object, Object>();

		final Object scriptResult = OGremlinHelper.execute(db, (String) iParameters[0], iRequester != null ? iRequester.getParameters()
				: null, currentParameters, result, new OGremlinHelper.OGremlinCallback() {

			@Override
			public boolean call(GremlinScriptEngine iEngine, OrientGraph iGraph) {
				final OrientElement graphElement;

				final ODocument document = (ODocument) iCurrentRecord;
				if (db.isVertex(document))
					// VERTEX TYPE, CREATE THE BLUEPRINTS'S WRAPPER
					graphElement = new OrientVertex(iGraph, document);
				else if (db.isEdge(document))
					// EDGE TYPE, CREATE THE BLUEPRINTS'S WRAPPER
					graphElement = new OrientEdge(iGraph, document);
				else
					// UNKNOWN CLASS: IGNORE IT
					return false;

				iEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("current", graphElement);

				return true;
			}
		}, null);

		return scriptResult;
	}

	@Override
	public boolean aggregateResults(final Object[] iConfiguredParameters) {
		return false;
	}

	public String getSyntax() {
		return "Syntax error: gremlin(<gremlin-expression>)";
	}

	@Override
	public boolean filterResult() {
		return true;
	}

	@Override
	public Object getResult() {
		return result;
	}
}
