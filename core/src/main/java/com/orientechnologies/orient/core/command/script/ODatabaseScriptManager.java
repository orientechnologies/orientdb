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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.concur.resource.OResourcePoolFactory;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Manages Script engines per database. Parsing of function library is done only the first time and
 * when changes.
 *
 * @see OCommandScript
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODatabaseScriptManager {
  private final OScriptManager scriptManager;
  protected OResourcePoolFactory<String, ScriptEngine> pooledEngines;

  public ODatabaseScriptManager(final OScriptManager iScriptManager, final String iDatabaseName) {
    scriptManager = iScriptManager;

    pooledEngines =
        new OResourcePoolFactory<String, ScriptEngine>(
            new OResourcePoolFactory.ObjectFactoryFactory<String, ScriptEngine>() {
              @Override
              public OResourcePoolListener<String, ScriptEngine> create(final String language) {
                return new OResourcePoolListener<String, ScriptEngine>() {
                  @Override
                  public ScriptEngine createNewResource(String key, Object... args) {
                    final ScriptEngine scriptEngine = scriptManager.getEngine(language);
                    final String library =
                        scriptManager.getLibrary(
                            ODatabaseRecordThreadLocal.instance().get(), language);

                    if (library != null)
                      try {
                        scriptEngine.eval(library);
                      } catch (ScriptException e) {
                        scriptManager.throwErrorMessage(e, library);
                      }

                    return scriptEngine;
                  }

                  @Override
                  public boolean reuseResource(
                      String iKey, Object[] iAdditionalArgs, ScriptEngine iValue) {
                    if (language.equals("sql")) {
                      if (!language.equals(iValue.getFactory().getLanguageName())) return false;
                    } else {
                      if ((iValue.getFactory().getLanguageName()).equals("sql")) return false;
                    }
                    return true;
                  }
                };
              }
            });
    pooledEngines.setMaxPoolSize(OGlobalConfiguration.SCRIPT_POOL.getValueAsInteger());
    pooledEngines.setMaxPartitions(1);
  }

  public ScriptEngine acquireEngine(final String language) {
    return pooledEngines.get(language).getResource(language, 0);
  }

  public void releaseEngine(final String language, ScriptEngine entry) {
    pooledEngines.get(language).returnResource(entry);
  }

  public void close() {
    pooledEngines.close();
  }
}
