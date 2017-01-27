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
package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.orientdb.OrientElement;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertexProperty;
import org.apache.tinkerpop.gremlin.orientdb.executor.transformer.OElementTransformer;
import org.apache.tinkerpop.gremlin.orientdb.executor.transformer.OrientPropertyTransformer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.util.Map;

/**
 * Executes a GREMLIN command.
 *
 * @author Enrico Risa (e.risa-(at)--orientdb.com)
 */
public class OCommandGremlinExecutor implements OScriptExecutor {

  private final OScriptManager                   scriptManager;
  private       GremlinGroovyScriptEngineFactory factory;

  public OCommandGremlinExecutor() {

    factory = new GremlinGroovyScriptEngineFactory();
    factory.setCustomizerManager(new CachedGremlinScriptEngineManager());
    this.scriptManager = Orient.instance().getScriptManager();

    initCustomTransformer(this.scriptManager);

  }

  private void initCustomTransformer(OScriptManager scriptManager) {

    scriptManager.getTransformer().registerResultTransformer(OrientVertex.class, new OElementTransformer());
    scriptManager.getTransformer().registerResultTransformer(OrientElement.class, new OElementTransformer());
    scriptManager.getTransformer().registerResultTransformer(OrientVertexProperty.class, new OrientPropertyTransformer());
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... params) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public OResultSet execute(final ODatabaseDocumentInternal iDatabase, final String iText, final Map params) {

    try {
      final ScriptEngine engine = getGremlinEngine(acquireGraph(iDatabase));
      bindParameters(engine, params);

      final Traversal result = (Traversal) engine.eval(iText);

      return new OGremlinResultSet(result, scriptManager.getTransformer(), true);

    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException("Error on execution of the GREMLIN script"), e);
    } finally {

    }
  }

  protected ScriptEngine getGremlinEngine(final OrientGraph graph) {
    GremlinScriptEngine engine = factory.getScriptEngine();
    engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", graph.traversal());
    return engine;
  }

  public void bindParameters(final ScriptEngine iEngine, final Map<Object, Object> iParameters) {
    if (iParameters != null && !iParameters.isEmpty())
      // Every call to the function is a execution itself. Therefore, it requires a fresh set of input parameters.
      // Therefore, clone the parameters map trying to recycle previous instances
      for (Map.Entry<Object, Object> param : iParameters.entrySet()) {
        final String paramName = param.getKey().toString().trim();
        iEngine.getBindings(ScriptContext.ENGINE_SCOPE).put(paramName, param.getValue());
      }

  }

  public OrientGraph acquireGraph(final ODatabaseDocumentInternal database) {
    return new OrientGraph(database, new BaseConfiguration() {
      {
        setProperty(OrientGraph.CONFIG_TRANSACTIONAL, true);
      }
    }, null, null);
  }

  public String getEngineVersion() {
    return factory.getEngineVersion();
  }
}
