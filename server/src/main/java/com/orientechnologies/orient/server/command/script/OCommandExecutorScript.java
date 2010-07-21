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
package com.orientechnologies.orient.server.command.script;

import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.enterprise.command.script.OCommandScript;
import com.orientechnologies.orient.enterprise.command.script.OCommandScriptException;

/**
 * Executes Script Commands.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorScript extends OCommandExecutorAbstract {
	protected static final String								DEF_LANGUAGE		= "JavaScript";

	protected static ScriptEngineManager				scriptEngineManager;
	protected static Map<String, ScriptEngine>	engines;
	protected static String											defaultLanguage	= DEF_LANGUAGE;
	protected OCommandScript										request;

	static {
		if (engines == null) {
			engines = new HashMap<String, ScriptEngine>();
			scriptEngineManager = new ScriptEngineManager();
			for (ScriptEngineFactory f : scriptEngineManager.getEngineFactories()) {
				engines.put(f.getLanguageName(), f.getScriptEngine());

				if (defaultLanguage == null)
					defaultLanguage = f.getLanguageName();
			}

			if (!engines.containsKey(DEF_LANGUAGE)) {
				engines.put(DEF_LANGUAGE, scriptEngineManager.getEngineByName(DEF_LANGUAGE));
				defaultLanguage = DEF_LANGUAGE;
			}
		}
	}

	public OCommandExecutorScript() {
	}

	@SuppressWarnings("unchecked")
	public OCommandExecutorScript parse(final OCommandRequestText iRequest) {
		request = (OCommandScript) iRequest;
		return this;
	}

	public Object execute(final Object... iArgs) {
		final String iLanguage = request.getLanguage();
		final String iScript = request.getText();

		if (iLanguage == null)
			throw new OCommandScriptException("No language was specified");

		if (!engines.containsKey(iLanguage))
			throw new OCommandScriptException("Unsupported language: " + iLanguage + ". Supported languages are: " + engines);

		if (iScript == null)
			throw new OCommandScriptException("Invalid script: " + iScript);

		ScriptEngine scriptEngine = engines.get(iLanguage);

		if (scriptEngine == null)
			throw new OCommandScriptException("Cannot find script engine: " + iLanguage);

		Bindings binding = scriptEngine.createBindings();

		try {
			Object result = null;
			result = scriptEngine.eval(iScript, binding);

			return result;
		} catch (ScriptException e) {
			throw new OCommandScriptException("Error on execution of the script", request.getText(), 0, e);
		}
	}
}
