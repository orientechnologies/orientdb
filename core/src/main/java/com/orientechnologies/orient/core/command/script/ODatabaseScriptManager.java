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

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.concur.resource.OPartitionedObjectPoolFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Manages Script engines per database. Parsing of function library is done only the first time and when changes.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class ODatabaseScriptManager {
  private final OScriptManager                                  scriptManager;
  protected OPartitionedObjectPoolFactory<String, ScriptEngine> pooledEngines;

  public ODatabaseScriptManager(final OScriptManager iScriptManager, final String iDatabaseName) {
    scriptManager = iScriptManager;

    pooledEngines = new OPartitionedObjectPoolFactory<String, ScriptEngine>(
        new OPartitionedObjectPoolFactory.ObjectFactoryFactory<String, ScriptEngine>() {
          @Override
          public OPartitionedObjectPool.ObjectFactory<ScriptEngine> create(final String language) {
            return new OPartitionedObjectPool.ObjectFactory<ScriptEngine>() {
              @Override
              public ScriptEngine create() {
                final ScriptEngine scriptEngine = scriptManager.getEngine(language);
                final String library = scriptManager.getLibrary(ODatabaseRecordThreadLocal.INSTANCE.get(), language);

                if (library != null)
                  try {
                    scriptEngine.eval(library);
                  } catch (ScriptException e) {
                    scriptManager.throwErrorMessage(e, library);
                  }

                return scriptEngine;
              }

              @Override
              public void init(ScriptEngine object) {
              }

              @Override
              public void close(ScriptEngine object) {
              }

              @Override
              public boolean isValid(ScriptEngine object) {
                if (language.equals("sql")) {
                  if (!language.equals(object.getFactory().getLanguageName()))
                    return false;
                } else {
                  if ((object.getFactory().getLanguageName()).equals("sql"))
                    return false;
                }
                return true;
              }
            };
          }
        });
    pooledEngines.setMaxPoolSize(OGlobalConfiguration.SCRIPT_POOL.getValueAsInteger());
    pooledEngines.setMaxPartitions(1);
  }

  public OPartitionedObjectPool.PoolEntry<ScriptEngine> acquireEngine(final String language) {
    return pooledEngines.get(language).acquire();
  }

  public void releaseEngine(final String language, final OPartitionedObjectPool.PoolEntry<ScriptEngine> entry) {
    pooledEngines.get(language).release(entry);
  }

  public void close() {
    pooledEngines.close();
  }
}
