/*
 * Copyright 2010-2013 Orient Technologies LTD
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 */
package com.orientechnologies.workbench.event;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.workbench.event.metric.OEventMetricExecutor;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

@EventConfig(when = "MetricsWhen", what = "FunctionWhat")
public class OEventMetricFunctionExecutor extends OEventMetricExecutor {

	private ODatabaseDocumentTx	db;

	public OEventMetricFunctionExecutor(ODatabaseDocumentTx database) {

		this.db = database;
	}

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {

		fillMapResolve(source, when);
		// pre-conditions
		if (canExecute(source, when)) {
			executeFunction(what);
		}
	}

	private void executeFunction(ODocument what) {

		OFunction fun = new OFunction(what);
		String language = what.field("language");
		String name = what.field("name");
		Object[] iArgs = what.field("parameters");

		Object[] args = null;
		if (iArgs != null) {
			args = new Object[iArgs.length];
			int i = 0;
			for (Object arg : iArgs)
				args[i++] = EventHelper.resolve(getBody2name(), arg);
		}

		final OScriptManager scriptManager = Orient.instance().getScriptManager();
		final ScriptEngine scriptEngine = scriptManager.getEngine(language);

		db.checkSecurity(ODatabaseSecurityResources.FUNCTION, ORole.PERMISSION_READ, name);

		try {
			// COMPILE FUNCTION LIBRARY
			final String lib = scriptManager.getFunctionDefinition(fun);
			if (lib != null)
				try {
					scriptEngine.eval(lib);
				} catch (ScriptException e) {
					scriptManager.throwErrorMessage(e, lib);
				}

			if (scriptEngine instanceof Invocable) {
				final Invocable invocableEngine = (Invocable) scriptEngine;
				try {
					invocableEngine.invokeFunction(name, args);
				} catch (ScriptException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				}
			}
		} catch (OCommandScriptException e) {
			throw e;
		}
	}
}
