/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.graph.gremlin;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;

public class OGremlinEngineThreadLocal extends ThreadLocal<ScriptEngine> {

  public static volatile OGremlinEngineThreadLocal INSTANCE = new OGremlinEngineThreadLocal();

  static {
    Orient.instance()
        .registerListener(
            new OOrientListenerAbstract() {
              @Override
              public void onStartup() {
                if (INSTANCE == null) INSTANCE = new OGremlinEngineThreadLocal();
              }

              @Override
              public void onShutdown() {
                INSTANCE = null;
              }
            });
  }

  public ScriptEngine get(final OrientBaseGraph iGraph) {
    ScriptEngine engine = super.get();
    if (engine != null) {
      final OrientBaseGraph currGraph =
          (OrientBaseGraph) engine.getBindings(ScriptContext.ENGINE_SCOPE).get("g");
      if (currGraph == iGraph
          || (currGraph != null
              && currGraph.getRawGraph().getURL().equals(iGraph.getRawGraph().getURL()))) {
        // REUSE IT
        engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", iGraph);
        return engine;
      }
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
