/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.event;

import com.orientechnologies.agent.event.metric.OEventMetricExecutor;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Map;

@EventConfig(when = "MetricWhen", what = "FunctionWhat")
public class OEventMetricFunctionExecutor extends OEventMetricExecutor {

  public OEventMetricFunctionExecutor() {

  }

  @Override
  public void execute(ODocument source, ODocument when, ODocument what) {

    // pre-conditions
    if (canExecute(source, when)) {

      Map<String, Object> params = fillMapResolve(source, when);
      executeFunction(what, params);
    }
  }

  private void executeFunction(ODocument what, Map<String, Object> params) {

    OFunction fun = new OFunction(what);
    String language = what.field("language");
    String name = what.field("name");
    Object[] iArgs = what.field("parameters");

    Object[] args = null;
    if (iArgs != null) {
      args = new Object[iArgs.length];
      int i = 0;
      for (Object arg : iArgs)
        args[i++] = EventHelper.resolve(params, arg);
    }

    final OScriptManager scriptManager = Orient.instance().getScriptManager();
    final ScriptEngine scriptEngine = scriptManager.getEngine(language);

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
