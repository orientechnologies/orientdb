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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.script.OCommandExecutorFunction;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLLiveSelect;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLResultsetDelegate;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLResultset;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class OCommandManager {
  private Map<String, Class<? extends OCommandRequest>> commandRequesters =
      new HashMap<String, Class<? extends OCommandRequest>>();
  private Map<Class<? extends OCommandRequest>, OCallable<Void, OCommandRequest>> configCallbacks =
      new HashMap<Class<? extends OCommandRequest>, OCallable<Void, OCommandRequest>>();
  private Map<Class<? extends OCommandRequest>, Class<? extends OCommandExecutor>>
      commandReqExecMap =
          new HashMap<Class<? extends OCommandRequest>, Class<? extends OCommandExecutor>>();
  private Map<String, OScriptExecutor> scriptExecutors = new HashMap<>();

  public OCommandManager() {
    registerScriptExecutor("sql", new OSqlScriptExecutor());
    registerScriptExecutor("script", new OSqlScriptExecutor());
    registerRequester("sql", OCommandSQL.class);
    registerRequester("script", OCommandScript.class);

    registerExecutor(OSQLAsynchQuery.class, OCommandExecutorSQLDelegate.class);
    registerExecutor(OSQLSynchQuery.class, OCommandExecutorSQLDelegate.class);
    registerExecutor(OSQLNonBlockingQuery.class, OCommandExecutorSQLDelegate.class);
    registerExecutor(OLiveQuery.class, OCommandExecutorSQLLiveSelect.class);
    registerExecutor(OCommandSQL.class, OCommandExecutorSQLDelegate.class);
    registerExecutor(OCommandSQLResultset.class, OCommandExecutorSQLResultsetDelegate.class);
    registerExecutor(OCommandScript.class, OCommandExecutorScript.class);
    registerExecutor(OCommandFunction.class, OCommandExecutorFunction.class);
  }

  public OCommandManager registerRequester(
      final String iType, final Class<? extends OCommandRequest> iRequest) {
    commandRequesters.put(iType, iRequest);
    return this;
  }

  public OScriptExecutor getScriptExecutor(String language) {
    if (language == null) {
      throw new IllegalArgumentException("Invalid script languange: null");
    }
    OScriptExecutor scriptExecutor = this.scriptExecutors.get(language);
    if (scriptExecutor == null) {
      scriptExecutor = this.scriptExecutors.get(language.toLowerCase(Locale.ENGLISH));
    }
    if (scriptExecutor == null)
      throw new IllegalArgumentException(
          "Cannot find a script executor requester for language: " + language);

    return scriptExecutor;
  }

  public OCommandRequest getRequester(final String iType) {
    final Class<? extends OCommandRequest> reqClass = commandRequesters.get(iType);

    if (reqClass == null)
      throw new IllegalArgumentException("Cannot find a command requester for type: " + iType);

    try {
      return reqClass.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot create the command requester of class " + reqClass + " for type: " + iType, e);
    }
  }

  public OCommandManager registerExecutor(
      final Class<? extends OCommandRequest> iRequest,
      final Class<? extends OCommandExecutor> iExecutor,
      final OCallable<Void, OCommandRequest> iConfigCallback) {
    registerExecutor(iRequest, iExecutor);
    configCallbacks.put(iRequest, iConfigCallback);
    return this;
  }

  public void registerScriptExecutor(String language, OScriptExecutor executor) {
    this.scriptExecutors.put(language, executor);
  }

  public Map<String, OScriptExecutor> getScriptExecutors() {
    return scriptExecutors;
  }

  public OCommandManager registerExecutor(
      final Class<? extends OCommandRequest> iRequest,
      final Class<? extends OCommandExecutor> iExecutor) {
    commandReqExecMap.put(iRequest, iExecutor);
    return this;
  }

  public OCommandManager unregisterExecutor(final Class<? extends OCommandRequest> iRequest) {
    commandReqExecMap.remove(iRequest);
    configCallbacks.remove(iRequest);
    return this;
  }

  public OCommandExecutor getExecutor(OCommandRequestInternal iCommand) {
    final Class<? extends OCommandExecutor> executorClass =
        commandReqExecMap.get(iCommand.getClass());

    if (executorClass == null)
      throw new OCommandExecutorNotFoundException(
          "Cannot find a command executor for the command request: " + iCommand);

    try {
      final OCommandExecutor exec = executorClass.newInstance();

      final OCallable<Void, OCommandRequest> callback = configCallbacks.get(iCommand.getClass());
      if (callback != null) callback.call(iCommand);

      return exec;

    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException(
              "Cannot create the command executor of class "
                  + executorClass
                  + " for the command request: "
                  + iCommand),
          e);
    }
  }

  public void close(String iDatabaseName) {
    for (OScriptExecutor executor : scriptExecutors.values()) {
      executor.close(iDatabaseName);
    }
  }

  public void closeAll() {
    for (OScriptExecutor executor : scriptExecutors.values()) {
      executor.closeAll();
    }
  }
}
