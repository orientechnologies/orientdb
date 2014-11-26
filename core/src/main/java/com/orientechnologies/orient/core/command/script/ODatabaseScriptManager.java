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
package com.orientechnologies.orient.core.command.script;

import java.util.Iterator;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

/**
 * Manages Script engines per database. Parsing of function library is done only the first time and when changes.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class ODatabaseScriptManager {
  private final String                          name;
  private final OScriptManager                  scriptManager;
  protected OResourcePool<String, ScriptEngine> pooledEngines;

  public ODatabaseScriptManager(final OScriptManager iScriptManager, final String iDatabaseName) {
    scriptManager = iScriptManager;
    name = iDatabaseName;

    pooledEngines = new OResourcePool<String, ScriptEngine>(OGlobalConfiguration.SCRIPT_POOL.getValueAsInteger(),
        new OResourcePoolListener<String, ScriptEngine>() {
          @Override
          public ScriptEngine createNewResource(final String iLanguage, final Object... iAdditionalArgs) {
            final ScriptEngine scriptEngine = scriptManager.getEngine(iLanguage);
            final String library = scriptManager.getLibrary(ODatabaseRecordThreadLocal.INSTANCE.get(), iLanguage);

            if (library != null)
              try {
                scriptEngine.eval(library);
              } catch (ScriptException e) {
                scriptManager.throwErrorMessage(e, library);
              }

            return scriptEngine;
          }

          @Override
          public boolean reuseResource(final String iKey, final Object[] iAdditionalArgs, final ScriptEngine iValue) {
            return true;
          }
        });
  }

  public ScriptEngine acquireEngine(final String iLanguage) {
    return pooledEngines.getResource(iLanguage, Long.MAX_VALUE);
  }

  public void releaseEngine(final ScriptEngine iEngine) {

    pooledEngines.returnResource(iEngine);
  }

  public void close() {
    pooledEngines.close();
  }
}
