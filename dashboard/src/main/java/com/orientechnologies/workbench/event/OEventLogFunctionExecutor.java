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
import com.orientechnologies.workbench.event.metric.OEventLogExecutor;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.List;

@EventConfig(when = "LogWhen", what = "FunctionWhat")
public class OEventLogFunctionExecutor extends OEventLogExecutor {
	private ODatabaseDocumentTx db;

	public OEventLogFunctionExecutor(ODatabaseDocumentTx database) {

		this.db = database;
	}

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {

	
		

		// pre-conditions
		fillMapResolve(source, when);
		if (canExecute(source, when)) {
			executeFunction(what);
		}
	}

	@SuppressWarnings("restriction")
	private void executeFunction(ODocument what) {

		
		String language = what.field("language");
		String name = what.field("name");
		List<String> iArgs = what.field("parameters");

		Object[] args = null;
		if (iArgs != null) {
			args = new Object[iArgs.size()];
			int i = 0;
			for (Object arg : iArgs)
				args[i++] = EventHelper.resolve(getBody2name(), arg);
		}
		
		
		final OScriptManager scriptManager = Orient.instance()
				.getScriptManager();
//		what.field("parameters", args);

		OFunction fun = new OFunction(what);
		ScriptEngine scriptEngine = scriptManager.getEngine(language);
		db.checkSecurity(ODatabaseSecurityResources.FUNCTION,
				ORole.PERMISSION_READ, name);

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
