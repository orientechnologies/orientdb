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

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import com.tinkerpop.gremlin.pipes.GremlinPipeline;

public class OGremlinHelper {
	private static final String													PARAM_OUTPUT	= "output";
	private static Object																ONE						= new Object();
	private static OGremlinHelper												instance			= new OGremlinHelper();

	private int																					maxEngines		= 50;
	private int																					maxGraphs			= 50;

	private OResourcePool<Object, GremlinScriptEngine>	enginePool;
	private OResourcePool<String, OrientGraph>					graphPool;

	public static interface OGremlinCallback {
		public boolean call(GremlinScriptEngine iEngine, OrientGraph iGraph);
	}

	public OGremlinHelper() {
		OCommandManager.instance().registerRequester("gremlin", OCommandGremlin.class);
		OCommandManager.instance().registerExecutor(OCommandGremlin.class, OCommandGremlinExecutor.class);
		OSQLEngine.getInstance().registerFunction(OSQLFunctionGremlin.NAME, OSQLFunctionGremlin.class);
	}

	/**
	 * Initializes the pools.
	 */
	public void create() {
		if (enginePool != null)
			// ALREADY CREATED
			return;

		enginePool = new OResourcePool<Object, GremlinScriptEngine>(maxEngines,
				new OResourcePoolListener<Object, GremlinScriptEngine>() {

					@Override
					public GremlinScriptEngine createNewResource(Object iKey, Object... iAdditionalArgs) {
						try {
							return new GremlinScriptEngine();
						} catch (Throwable e) {
							throw new OConfigurationException("Error on loading Gremlin engine", e);
						}
					}

					@Override
					public GremlinScriptEngine reuseResource(Object iKey, Object[] iAdditionalArgs, GremlinScriptEngine iReusedEngine) {
						iReusedEngine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
						return iReusedEngine;
					}
				});

		graphPool = new OResourcePool<String, OrientGraph>(maxGraphs, new OResourcePoolListener<String, OrientGraph>() {

			@Override
			public OrientGraph createNewResource(final String iKey, final Object... iAdditionalArgs) {
				final String[] parts = iKey.split(",");
				return new OrientGraph(parts[1]);
			}

			@Override
			public OrientGraph reuseResource(final String iKey, final Object[] iAdditionalArgs, final OrientGraph iReusedGraph) {
				return iReusedGraph;
			}
		});
	}

	/**
	 * Destroys the helper by cleaning all the in memory objects.
	 */
	public void destroy() {
		if (enginePool != null) {
			for (GremlinScriptEngine engine : enginePool.getResources()) {
				engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
			}
			enginePool.close();
		}

		if (graphPool != null) {
			for (OrientGraph graph : graphPool.getResources()) {
				graph.shutdown();
			}
			graphPool.close();
		}
	}

	public GremlinScriptEngine acquireEngine() {
		checkStatus();
		return enginePool.getResource(ONE, Long.MAX_VALUE);
	}

	public void releaseEngine(final GremlinScriptEngine engine) {
		checkStatus();
		engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
		enginePool.returnResource(engine);
	}

	public OrientGraph acquireGraph(final String iURL) {
		return acquireGraph("admin", "admin", iURL);
	}

	public OrientGraph acquireGraph(final String iURL, final String iUserName, final String iUserPassword) {
		checkStatus();
		final OrientGraph g = graphPool.getResource(iUserName + "," + iURL, Long.MAX_VALUE);
		if (!g.getRawGraph().getUser().checkPassword(iUserPassword)) {
			graphPool.returnResource(g);
			throw new OSecurityException("User and/or password not valid to open database: " + iURL);
		}

		return g;
	}

	public OrientGraph acquireGraph(final OGraphDatabase iDatabase) {
		checkStatus();
		return graphPool.getResource(iDatabase.getUser().getName() + "," + iDatabase.getURL(), Long.MAX_VALUE).reuse(iDatabase);
	}

	public void releaseGraph(final OrientGraph iGraph) {
		checkStatus();
		graphPool.returnResource(iGraph);
	}

	public static Object execute(final OGraphDatabase iDatabase, final String iText, final Map<Object, Object> iConfiguredParameters,
			Map<Object, Object> iCurrentParameters, final List<Object> iResult, final OGremlinCallback iBeforeExecution,
			final OGremlinCallback iAfterExecution) {
		final OrientGraph graph = OGremlinHelper.global().acquireGraph(iDatabase);
		try {
			final GremlinScriptEngine engine = OGremlinHelper.global().acquireEngine();
			try {
				engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", graph);

				final String output = iConfiguredParameters != null ? OGremlinHelper.bindParameters(engine, iConfiguredParameters,
						iCurrentParameters) : null;

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

	public static String bindParameters(final GremlinScriptEngine iEngine, final Map<Object, Object> iParameters,
			Map<Object, Object> iCurrentParameters) {
		if (iParameters == null || iParameters.isEmpty())
			return null;

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
			recycledMap.putAll((Map) objectToClone);
			return recycledMap;

			// Clone any collection (shallow clone should be enough at this level)
		} else if (objectToClone instanceof Collection) {
			Collection recycledCollection = (Collection) previousClone;
			if (recycledCollection == null)
				recycledCollection = new ArrayList();
			else
				recycledCollection.clear();
			recycledCollection.addAll((Collection) objectToClone);
			return recycledCollection;

			// Clone String
		} else if (objectToClone instanceof String) {
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
				for (Class obj = objectToClone.getClass(); !obj.equals(Object.class); obj = obj.getSuperclass()) {
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

	public int getMaxEngines() {
		return maxEngines;
	}

	public void setMaxEngines(int maxEngines) {
		this.maxEngines = maxEngines;
	}

	public int getMaxGraphs() {
		return maxGraphs;
	}

	public void setMaxGraphs(int maxGraphs) {
		this.maxGraphs = maxGraphs;
	}

	private void checkStatus() {
		if (enginePool == null || graphPool == null)
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
			throw new OCommandExecutionException("Can't find a database of type OGraphDatabase or ODatabaseRecordTx");
		return db;
	}

}