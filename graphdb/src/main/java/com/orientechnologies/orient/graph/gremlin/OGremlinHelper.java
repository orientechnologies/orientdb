/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.graph.gremlin;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
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

public class OGremlinHelper {
  private static final String                     PARAM_OUTPUT = "output";
  private static GremlinGroovyScriptEngineFactory factory      = new GremlinGroovyScriptEngineFactory();
  private static OGremlinHelper                   instance     = new OGremlinHelper();

  private int                                     maxPool      = 50;

  public interface OGremlinCallback {
    boolean call(ScriptEngine iEngine, OrientBaseGraph iGraph);
  }

  public OGremlinHelper() {
    OCommandManager.instance().registerRequester("gremlin", OCommandGremlin.class);
    OCommandManager.instance().registerExecutor(OCommandGremlin.class, OCommandGremlinExecutor.class);
    final long timeout = OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsLong();
  }

  @SuppressWarnings("unchecked")
  public static Object execute(final ODatabaseDocumentTx iDatabase, final String iText,
      final Map<Object, Object> iConfiguredParameters, Map<Object, Object> iCurrentParameters, final List<Object> iResult,
      final OGremlinCallback iBeforeExecution, final OGremlinCallback iAfterExecution) {
    return execute(OGremlinHelper.global().acquireGraph(iDatabase), iText, iConfiguredParameters, iCurrentParameters, iResult,
        iBeforeExecution, iAfterExecution);
  }

  public static Object execute(final OrientBaseGraph graph, final String iText, final Map<Object, Object> iConfiguredParameters,
      Map<Object, Object> iCurrentParameters, final List<Object> iResult, final OGremlinCallback iBeforeExecution,
      final OGremlinCallback iAfterExecution) {
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

  protected static ScriptEngine getGremlinEngine(final OrientBaseGraph graph) {
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
    if (iCurrentParameters != null)
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
      return (Date) ((Date) objectToClone).clone();

    } else {
      // ***************************************************************************************************************************************
      // 2. Polymorphic clone (by reflection, looks for a clone() method in hierarchy and invoke it)
      // ***************************************************************************************************************************************
      try {
        Object newClone;
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
          OLogManager.instance().error(null, "[GremlinHelper] error on cloning object %s, previous %s", e2, objectToClone,
              previousClone);
          return null;
        }
      }
    }
  }

  public static OGremlinHelper global() {
    return instance;
  }

  public static ODatabaseDocumentTx getGraphDatabase(final ODatabaseDocumentInternal iCurrentDatabase) {
    ODatabaseDocumentInternal currentDb = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (currentDb == null && iCurrentDatabase != null)
      // GET FROM THE RECORD
      currentDb = iCurrentDatabase;

    if (currentDb != null)
      currentDb = (ODatabaseDocumentInternal) currentDb.getDatabaseOwner();

    final ODatabaseDocumentTx db;
    if (currentDb instanceof ODatabaseDocumentTx)
      db = (ODatabaseDocumentTx) currentDb;
    else
      throw new OCommandExecutionException("Cannot find a database of type ODatabaseDocumentTx or ODatabaseDocumentTx");
    return db;
  }

  public static String getEngineVersion() {
    return factory.getEngineVersion();
  }

  /**
   * Initializes the pools.
   */
  public void create() {
  }

  /**
   * Destroys the helper by cleaning all the in memory objects.
   */
  public void destroy() {
  }

  public ScriptEngine acquireEngine() {
    return new GremlinGroovyScriptEngine();
  }

  public void releaseEngine(final ScriptEngine engine) {
  }

  public OrientGraph acquireGraph(final ODatabaseDocumentTx database) {
    return new OrientGraph(database);
  }

  public void releaseGraph(final OrientBaseGraph graph) {
    graph.shutdown(false);
  }

  public int getMaxPool() {
    return maxPool;
  }

  public OGremlinHelper setMaxGraphPool(final int maxGraphs) {
    this.maxPool = maxGraphs;
    return this;
  }

  protected ScriptEngine getGroovyEngine() {
    return factory.getScriptEngine();
  }

}
