/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;

public class OGremlinEngineThreadLocal extends ThreadLocal<ScriptEngine> {

  public static OGremlinEngineThreadLocal INSTANCE = new OGremlinEngineThreadLocal();

  public ScriptEngine get(final OrientGraph iGraph) {
    ScriptEngine engine = super.get();
    if (engine != null) {
      final OrientGraph currGraph = (OrientGraph) engine.getBindings(ScriptContext.ENGINE_SCOPE).get("g");
      if (currGraph == iGraph || (currGraph != null && currGraph.getRawGraph().getURL().equals(iGraph.getRawGraph().getURL())))
        // REUSE IT
        return engine;
    }

    // CREATE A NEW ONE
    engine = new GremlinGroovyScriptEngine();
    engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", iGraph);
    set(engine);

    return engine;
  }

  public ScriptEngine getIfDefined() {
    return super.get();
  }

  public boolean isDefined() {
    return super.get() != null;
  }

}
