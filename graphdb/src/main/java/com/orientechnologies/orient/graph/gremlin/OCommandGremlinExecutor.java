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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import com.tinkerpop.gremlin.pipes.GremlinPipeline;

/**
 * Executes a GREMLIN command.
 * 
 * @author Luca Garulli
 */
public class OCommandGremlinExecutor extends OCommandExecutorAbstract {

	protected ScriptEngine				engine;
	protected OrientGraph					graph;
	protected Map<Object, Object>	clonedParameters	= new HashMap<Object, Object>();

	@SuppressWarnings("unchecked")
	@Override
	public <RET extends OCommandExecutor> RET parse(OCommandRequestText iRequest) {
		engine = new GremlinScriptEngine();
		graph = new OrientGraph(iRequest.getDatabase().getURL());
		text = iRequest.getText();

		engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", graph);
		return (RET) this;
	}

	@Override
	public Object execute(Map<Object, Object> iArgs) {
		try {
			final Object result = engine.eval(text);

			if (result instanceof GremlinPipeline) {
				final List<Object> coll = new ArrayList<Object>();
				final Iterator<?> it = ((GremlinPipeline<?, ?>) result).iterator();
				while (it.hasNext())
					coll.add(it.next());

				return coll;
			}

			return result;
		} catch (ScriptException e) {
			throw new OCommandExecutionException("Error on executing GREMLIN command: " + text, e);
		} finally {
			graph.shutdown();
		}
	}

	@Override
	public Map<Object, Object> getParameters() {
		if (parameters == null)
			return null;

		// Every call to the function is a execution itself. Therefore, it requires a fresh set of input parameters.
		// Therefore, clone the parameters map trying to recycle previous instances
		for (Entry<Object, Object> param : parameters.entrySet()) {
			String key = (String) param.getKey();
			Object objectToClone = param.getValue();
			Object previousItem = clonedParameters.get(key); // try to recycle it
			Object newItem = OCloneHelper.cloneObject(objectToClone, previousItem);
			clonedParameters.put(key, newItem);
		}
		return clonedParameters;
	}
}
