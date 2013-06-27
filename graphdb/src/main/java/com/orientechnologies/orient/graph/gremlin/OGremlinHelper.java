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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class OGremlinHelper {
  private static final String                        PARAM_OUTPUT = "output";
  private static GremlinGroovyScriptEngineFactory    factory      = new GremlinGroovyScriptEngineFactory();
  private static OGremlinHelper                      instance     = new OGremlinHelper();

  private int                                        maxPool      = 50;

  private OResourcePool<OGraphDatabase, OrientGraph> graphPool;
  private long                                       timeout;

  public static interface OGremlinCallback {
    public boolean call(ScriptEngine iEngine, OrientGraph iGraph);
  }

  public OGremlinHelper() {
    OCommandManager.instance().registerRequester("gremlin", OCommandGremlin.class);
    OCommandManager.instance().registerExecutor(OCommandGremlin.class, OCommandGremlinExecutor.class);
    timeout = OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsLong();
  }

  /**
   * Initializes the pools.
   */
  public void create() {
    if (graphPool != null)
      // ALREADY CREATED
      return;
    graphPool = new OResourcePool<OGraphDatabase, OrientGraph>(maxPool, new OResourcePoolListener<OGraphDatabase, OrientGraph>() {

      @Override
      public OrientGraph createNewResource(final OGraphDatabase iKey, final Object... iAdditionalArgs) {
        return new OrientGraph(iKey);
      }

      @Override
      public boolean reuseResource(final OGraphDatabase iKey, final Object[] iAdditionalArgs, final OrientGraph iReusedGraph) {
        iReusedGraph.reuse(iKey);
        return true;
      }
    });
  }

  /**
   * Destroys the helper by cleaning all the in memory objects.
   */
  public void destroy() {
    if (graphPool != null) {
      for (OrientGraph graph : graphPool.getResources()) {
        graph.shutdown();
      }
      graphPool.close();
    }
  }

  public ScriptEngine acquireEngine() {
    checkStatus();
    return new GremlinGroovyScriptEngine();// enginePool.getResource(ONE, Long.MAX_VALUE);
  }

  public void releaseEngine(final ScriptEngine engine) {
    checkStatus();
    // engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
    // enginePool.returnResource(engine);
  }

  public OrientGraph acquireGraph(final OGraphDatabase iDatabase) {
    checkStatus();
    return (OrientGraph) ((OrientGraph) graphPool.getResource(iDatabase, timeout));
  }

  public void releaseGraph(final OrientGraph iGraph) {
    checkStatus();
    graphPool.returnResource(iGraph);
  }

  @SuppressWarnings("unchecked")
  public static Object execute(final OGraphDatabase iDatabase, final String iText, final Map<Object, Object> iConfiguredParameters,
      Map<Object, Object> iCurrentParameters, final List<Object> iResult, final OGremlinCallback iBeforeExecution,
      final OGremlinCallback iAfterExecution) {
    final OrientGraph graph = OGremlinHelper.global().acquireGraph(iDatabase);
    try {
      final ScriptEngine engine = getGremlinEngine(graph);
      try {
        final String output = OGremlinHelper.bindParameters(engine, iConfiguredParameters, iCurrentParameters);

        if (iBeforeExecution != null)
          if (!iBeforeExecution.call(engine, graph))
            return null;

        final Object scriptResult = engine.eval(iText);

        if (iAfterExecution != null)
          if (!iAfterExecution.call(engine, graph))
            return null;

        // Case of 1 output bound variable. Return as:
        // - Map -> ODocument
        if (output != null) {
          if (scriptResult instanceof GremlinPipeline) {
            Iterator<?> it = ((GremlinPipeline<?, ?>) scriptResult).iterator();
            while (it.hasNext())
              // ignore iCurrentRecord but traverse still required
              it.next();
          }
          final Map<String, Object> map = (Map<String, Object>) engine.get(output);
          ODocument oDocument = new ODocument(map);
          iResult.add(oDocument);
          return oDocument;
        }

        // Case of no bound variables. Return as:
        // - List<ODocument>
        // - ODocument
        // - Integer
        // returned for this call in the last pipe
        if (scriptResult instanceof GremlinPipeline) {
          final Iterator<?> it = ((GremlinPipeline<?, ?>) scriptResult).iterator();
          Object finalResult = null;
          List<Object> resultCollection = null;

          while (it.hasNext()) {
            Object current = it.next();

            // if (current instanceof OrientElement)
            // current = ((OrientElement) current).getRawElement();

            if (finalResult != null) {
              if (resultCollection == null) {
                // CONVERT IT INTO A COLLECTION
                resultCollection = new ArrayList<Object>();
                resultCollection.add(finalResult);
              }

              resultCollection.add(current);
            } else
              finalResult = current;
          }

          if (resultCollection != null) {
            iResult.addAll(resultCollection);
            return resultCollection;
          } else {
            if (finalResult != null)
              iResult.add(finalResult);
            return finalResult;
          }

        } else if (scriptResult != null)
          iResult.add(scriptResult);

        return scriptResult;
      } catch (Exception e) {
        throw new OCommandExecutionException("Error on execution of the GREMLIN script", e);
      } finally {
        OGremlinHelper.global().releaseEngine(engine);
      }
    } finally {
      OGremlinHelper.global().releaseGraph(graph);
    }
  }

  protected static ScriptEngine getGremlinEngine(final OrientGraph graph) {
    return OGremlinEngineThreadLocal.INSTANCE.get(graph);
  }

  public static String bindParameters(final ScriptEngine iEngine, final Map<Object, Object> iParameters,
      Map<Object, Object> iCurrentParameters) {
    if (iParameters != null && !iParameters.isEmpty())
      // Every call to the function is a execution itself. Therefore, it requires a fresh set of input parameters.
      // Therefore, clone the parameters map trying to recycle previous instances
      for (Entry<Object, Object> param : iParameters.entrySet()) {
        final String key = (String) param.getKey();
        final Object objectToClone = param.getValue();
        final Object previousItem = iCurrentParameters.get(key); // try to recycle it
        final Object newItem = OGremlinHelper.cloneObject(objectToClone, previousItem);
        iCurrentParameters.put(key, newItem);
      }

    String output = null;
    for (Entry<Object, Object> param : iCurrentParameters.entrySet()) {
      final String paramName = param.getKey().toString().trim();
      if (paramName.equals(PARAM_OUTPUT)) {
        output = param.getValue().toString();
        continue;
      }
      iEngine.getBindings(ScriptContext.ENGINE_SCOPE).put(paramName, param.getValue());
    }
    return output;
  }

  /*
   * Tries to clone any Java object by using 3 techniques: - instanceof (most verbose but faster performance) - reflection (medium
   * performance) - serialization (applies for any object type but has a performance overhead)
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Object cloneObject(final Object objectToClone, final Object previousClone) {

    // ***************************************************************************************************************************************
    // 1. Class by class cloning (only clones known types)
    // ***************************************************************************************************************************************
    // Clone any Map (shallow clone should be enough at this level)
    if (objectToClone instanceof Map) {
      Map recycledMap = (Map) previousClone;
      if (recycledMap == null)
        recycledMap = new HashMap();
      else
        recycledMap.clear();
      recycledMap.putAll((Map<?, ?>) objectToClone);
      return recycledMap;

      // Clone any collection (shallow clone should be enough at this level)
    } else if (objectToClone instanceof Collection) {
      Collection recycledCollection = (Collection) previousClone;
      if (recycledCollection == null)
        recycledCollection = new ArrayList();
      else
        recycledCollection.clear();
      recycledCollection.addAll((Collection<?>) objectToClone);
      return recycledCollection;

      // Clone String
    } else if (objectToClone instanceof String) {
      return objectToClone;
    } else if (objectToClone instanceof Number) {
      return objectToClone;
      // Clone Date
    } else if (objectToClone instanceof Date) {
      Date clonedDate = (Date) ((Date) objectToClone).clone();
      return clonedDate;

    } else {
      // ***************************************************************************************************************************************
      // 2. Polymorphic clone (by reflection, looks for a clone() method in hierarchy and invoke it)
      // ***************************************************************************************************************************************
      try {
        Object newClone = null;
        for (Class<?> obj = objectToClone.getClass(); !obj.equals(Object.class); obj = obj.getSuperclass()) {
          Method m[] = obj.getDeclaredMethods();
          for (int i = 0; i < m.length; i++) {
            if (m[i].getName().equals("clone")) {
              m[i].setAccessible(true);
              newClone = m[i].invoke(objectToClone);
              System.out.println(objectToClone.getClass()
                  + " cloned by Reflection. Performance can be improved by adding the class to the list of known types");
              return newClone;
            }
          }
        }
        throw new Exception("Method clone not found");

        // ***************************************************************************************************************************************
        // 3. Polymorphic clone (Deep cloning by Serialization)
        // ***************************************************************************************************************************************
      } catch (Throwable e1) {
        try {
          final ByteArrayOutputStream bytes = new ByteArrayOutputStream() {
            public synchronized byte[] toByteArray() {
              return buf;
            }
          };
          final ObjectOutputStream out = new ObjectOutputStream(bytes);
          out.writeObject(objectToClone);
          out.close();
          final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
          System.out.println(objectToClone.getClass()
              + " cloned by Serialization. Performance can be improved by adding the class to the list of known types");
          return in.readObject();

          // ***************************************************************************************************************************************
          // 4. Impossible to clone
          // ***************************************************************************************************************************************
        } catch (Throwable e2) {
          e2.printStackTrace();
          return null;
        }
      }
    }
  }

  public static OGremlinHelper global() {
    return instance;
  }

  public int getMaxPool() {
    return maxPool;
  }

  public OGremlinHelper setMaxGraphPool(final int maxGraphs) {
    this.maxPool = maxGraphs;
    return this;
  }

  private void checkStatus() {
    if (graphPool == null)
      throw new IllegalStateException(
          "OGremlinHelper instance has been not created. Call OGremlinHelper.global().create() to iniziailze it");
  }

  public static OGraphDatabase getGraphDatabase(final ODatabaseRecord iCurrentDatabase) {
    ODatabaseRecord currentDb = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (currentDb == null && iCurrentDatabase != null)
      // GET FROM THE RECORD
      currentDb = iCurrentDatabase;

    currentDb = (ODatabaseRecord) currentDb.getDatabaseOwner();

    final OGraphDatabase db;
    if (currentDb instanceof OGraphDatabase)
      db = (OGraphDatabase) currentDb;
    else if (currentDb instanceof ODatabaseDocumentTx) {
      db = new OGraphDatabase((ODatabaseRecordTx) currentDb.getUnderlying());
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
    } else if (currentDb instanceof ODatabaseRecordTx) {
      db = new OGraphDatabase((ODatabaseRecordTx) currentDb);
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
    } else
      throw new OCommandExecutionException("Cannot find a database of type OGraphDatabase or ODatabaseRecordTx");
    return db;
  }

  public static String getEngineVersion() {
    return factory.getEngineVersion();
  }

  protected ScriptEngine getGroovyEngine() {
    return factory.getScriptEngine();
  }
}