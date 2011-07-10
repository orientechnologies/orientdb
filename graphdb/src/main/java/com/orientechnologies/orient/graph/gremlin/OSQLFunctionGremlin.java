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
import java.util.Iterator;
import java.util.List;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientElement;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientVertex;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import com.tinkerpop.gremlin.pipes.GremlinPipeline;

/**
 * Executes a GREMLIN expression as function of SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionGremlin extends OSQLFunctionAbstract {
	public static final String	NAME	= "gremlin";

	private List<Object>				result;

	private OrientGraph					graph	= null;

	private ScriptEngine				engine;

	public OSQLFunctionGremlin() {
		super(NAME, 1, 1);
	}

	public Object execute(final ORecord<?> iCurrentRecord, final Object[] iParameters) {
		if (!(iCurrentRecord instanceof ODocument))
			// NOT DOCUMENT: IGNORE IT
			return null;

		final ODocument document = (ODocument) iCurrentRecord;

		if (engine == null) {
			engine = new GremlinScriptEngine();
			graph = new OrientGraph(iCurrentRecord.getDatabase().getURL());
			engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", graph);
		}

		final OrientElement graphElement;

		if (document.getSchemaClass().isSubClassOf(OGraphDatabase.VERTEX_CLASS_NAME))
			graphElement = new OrientVertex(graph, document);
		else if (document.getSchemaClass().isSubClassOf(OGraphDatabase.VERTEX_CLASS_NAME))
			graphElement = new OrientVertex(graph, document);
		else
			// UNKNOWN CLASS: IGNORE IT
			return null;

		engine.getBindings(ScriptContext.ENGINE_SCOPE).put("current", graphElement);

		final Object scriptResult;
		try {
			scriptResult = engine.eval((String) iParameters[0]);
		} catch (ScriptException e) {
			return new OCommandExecutionException("Error on execution of the GREMLIN function", e);
		}

		if (result == null)
			result = new ArrayList<Object>();

		if (scriptResult instanceof GremlinPipeline) {
			final Iterator<?> it = ((GremlinPipeline<?, ?>) scriptResult).iterator();
			Object finalResult = null;
			List<Object> resultCollection = null;

			while (it.hasNext()) {
				Object current = it.next();

				if (current instanceof OrientElement)
					current = ((OrientElement) current).getRawElement();

				if (finalResult != null) {
					if (resultCollection == null)
						resultCollection = new ArrayList<Object>();

					resultCollection.add(current);
				} else
					finalResult = current;
			}

			if (resultCollection != null) {
				result.addAll(resultCollection);
				return resultCollection;
			} else {
				if (finalResult != null)
					result.add(finalResult);
				return finalResult;
			}

		} else if (scriptResult != null)
			result.add(scriptResult);

		return scriptResult;
	}

	@Override
	public boolean aggregateResults(final Object[] iConfiguredParameters) {
		return true;
	}

	public String getSyntax() {
		return "Syntax error: gremlin(<gremlin-expression>)";
	}

	@Override
	public Object getResult() {
		return result;
	}
}
