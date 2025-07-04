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

import com.orientechnologies.orient.core.command.script.OScriptManager;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class OCommandManager {
  private Map<String, OScriptExecutor> scriptExecutors = new HashMap<>();

  public OCommandManager(OScriptManager scriptManager) {
    registerScriptExecutor("sql", new OSqlScriptExecutor());
    registerScriptExecutor("script", new OSqlScriptExecutor());
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

  public void registerScriptExecutor(String language, OScriptExecutor executor) {
    this.scriptExecutors.put(language, executor);
  }

  public Map<String, OScriptExecutor> getScriptExecutors() {
    return scriptExecutors;
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
