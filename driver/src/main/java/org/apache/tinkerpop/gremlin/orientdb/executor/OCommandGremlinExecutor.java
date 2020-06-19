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

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.OScriptResultHandler;
import com.orientechnologies.orient.core.command.script.formatter.OGroovyScriptFormatter;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import groovy.lang.MissingPropertyException;
import java.util.HashMap;
import java.util.Map;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.orientdb.*;
import org.apache.tinkerpop.gremlin.orientdb.executor.transformer.OElementTransformer;
import org.apache.tinkerpop.gremlin.orientdb.executor.transformer.OGremlinTransformer;
import org.apache.tinkerpop.gremlin.orientdb.executor.transformer.OTraversalMetricTransformer;
import org.apache.tinkerpop.gremlin.orientdb.executor.transformer.OrientPropertyTransformer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;

/**
 * Executes a GREMLIN command.
 *
 * @author Enrico Risa (e.risa-(at)--orientdb.com)
 */
public class OCommandGremlinExecutor extends OAbstractScriptExecutor
    implements OScriptInjection, OScriptResultHandler {

  public static final String GREMLIN_GROOVY = "gremlin-groovy";
  private final OScriptManager scriptManager;
  private GremlinGroovyScriptEngineFactory factory;

  private OScriptTransformer transformer;

  public OCommandGremlinExecutor(OScriptManager scriptManager, OScriptTransformer transformer) {
    super("gremlin");
    factory = new GremlinGroovyScriptEngineFactory();
    factory.setCustomizerManager(new CachedGremlinScriptEngineManager());
    this.scriptManager = scriptManager;
    this.transformer = new OGremlinTransformer(transformer);

    initCustomTransformer(this.transformer);

    scriptManager.registerInjection(this);

    scriptManager.registerFormatter(GREMLIN_GROOVY, new OGroovyScriptFormatter());
    scriptManager.registerEngine(GREMLIN_GROOVY, factory);

    scriptManager.registerResultHandler(GREMLIN_GROOVY, this);
  }

  private void initCustomTransformer(OScriptTransformer transformer) {

    transformer.registerResultTransformer(
        DefaultTraversalMetrics.class, new OTraversalMetricTransformer());
    transformer.registerResultTransformer(OrientEdge.class, new OElementTransformer());
    transformer.registerResultTransformer(OrientVertex.class, new OElementTransformer());
    transformer.registerResultTransformer(OrientElement.class, new OElementTransformer());
    transformer.registerResultTransformer(
        OrientVertexProperty.class, new OrientPropertyTransformer(transformer));
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... params) {
    preExecute(database, script, params);
    Map<Object, Object> mapParams = new HashMap<>();
    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        mapParams.put("par_" + i, params[i]);
      }
    }
    return execute(database, script, mapParams);
  }

  @Override
  public OResultSet execute(
      final ODatabaseDocumentInternal iDatabase, final String iText, final Map params) {
    preExecute(iDatabase, iText, params);
    OPartitionedObjectPool.PoolEntry<ScriptEngine> entry = null;
    try {
      entry = acquireGremlinEngine(acquireGraph(iDatabase));
      ScriptEngine engine = entry.object;
      bindParameters(engine, params);

      Object eval = engine.eval(iText);

      if (eval instanceof Traversal) {

        Traversal result = (Traversal) eval;

        return new OGremlinScriptResultSet(iText, result, this.transformer, false);
      } else if (eval instanceof TraversalExplanation) {
        OInternalResultSet resultSet = new OInternalResultSet();
        resultSet.setPlan(new OGremlinExecutionPlan((TraversalExplanation) eval));
        OResultInternal item = new OResultInternal();
        item.setProperty("executionPlan", ((TraversalExplanation) eval).prettyPrint());
        resultSet.add(item);
        return resultSet;
      } else {
        OInternalResultSet resultSet = new OInternalResultSet();
        OResultInternal item = new OResultInternal();
        item.setProperty("value", this.transformer.toResult(eval));
        resultSet.add(item);
        return resultSet;
      }

    } catch (ScriptException e) {
      if (isGroovyException(e)) {
        throw new OCommandExecutionException(e.getMessage());
      } else {
        throw OException.wrapException(
            new OCommandExecutionException("Error on execution of the GREMLIN script"), e);
      }
    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException("Error on execution of the GREMLIN script"), e);
    } finally {
      if (entry != null) {
        releaseGremlinEngine(iDatabase.getName(), entry);
      }
    }
  }

  protected boolean isGroovyException(Throwable throwable) {

    if (throwable == null) return false;

    if (throwable instanceof MultipleCompilationErrorsException) {
      return true;
    }

    if (throwable instanceof MissingPropertyException) {
      return true;
    }

    return isGroovyException(throwable.getCause());
  }

  protected final OPartitionedObjectPool.PoolEntry<ScriptEngine> acquireGremlinEngine(
      final OrientGraph graph) {

    final OPartitionedObjectPool.PoolEntry<ScriptEngine> entry =
        scriptManager.acquireDatabaseEngine(graph.getRawDatabase().getName(), GREMLIN_GROOVY);
    final ScriptEngine engine = entry.object;
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    bindGraph(graph, bindings);
    return entry;
  }

  protected void releaseGremlinEngine(
      String dbName, OPartitionedObjectPool.PoolEntry<ScriptEngine> engine) {
    scriptManager.releaseDatabaseEngine(GREMLIN_GROOVY, dbName, engine);
  }

  private void bindGraph(OrientGraph graph, Bindings bindings) {
    bindings.put("graph", graph);
    bindings.put("g", graph.traversal());
  }

  private void unbindGraph(Bindings bindings) {
    bindings.put("graph", null);
    bindings.put("g", null);
  }

  public void bindParameters(final ScriptEngine iEngine, final Map<Object, Object> iParameters) {
    if (iParameters != null && !iParameters.isEmpty())
      // Every call to the function is a execution itself. Therefore, it requires a fresh set of
      // input parameters.
      // Therefore, clone the parameters map trying to recycle previous instances
      for (Map.Entry<Object, Object> param : iParameters.entrySet()) {
        final String paramName = param.getKey().toString().trim();
        iEngine.getBindings(ScriptContext.ENGINE_SCOPE).put(paramName, param.getValue());
      }
  }

  public OrientGraph acquireGraph(final ODatabaseDocument database) {
    return new OrientGraph(
        null,
        database,
        new BaseConfiguration() {
          {
            setProperty(OrientGraph.CONFIG_TRANSACTIONAL, database.getTransaction().isActive());
          }
        },
        null,
        null);
  }

  public String getEngineVersion() {
    return factory.getEngineVersion();
  }

  @Override
  public void bind(ScriptEngine engine, Bindings binding, ODatabaseDocument database) {

    OrientGraph graph = acquireGraph(database);

    bindGraph(graph, binding);
  }

  @Override
  public void unbind(ScriptEngine engine, Bindings binding) {
    unbindGraph(binding);
  }

  @Override
  public Object handle(
      Object result, ScriptEngine engine, Bindings binding, ODatabaseDocument database) {

    //        if(result instanceof Traversal){
    //            return new OGremlinResultSet((Traversal) result, scriptManager.getTransformer(),
    // false);
    //        }
    return result;
  }
}
