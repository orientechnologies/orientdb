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
package com.orientechnologies.orient.graph.gremlin;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes a GREMLIN command.
 * 
 * @author Luca Garulli
 */
public class OCommandGremlinExecutor extends OCommandExecutorAbstract {
  private ODatabaseDocumentTx db;

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest) {
    parserText = ((OCommandRequestText) iRequest).getText();
    db = OGremlinHelper.getGraphDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());
    return (RET) this;
  }

  @Override
  public Object execute(final Map<Object, Object> iArgs) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.COMMAND_GREMLIN, ORole.PERMISSION_READ);
    parameters = iArgs;
    final List<Object> result = new ArrayList<Object>();
    final Object scriptResult = OGremlinHelper.execute(db, parserText, parameters, new HashMap<Object, Object>(), result, null,
        null);
    return scriptResult != null ? scriptResult : result;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandScriptException("Error on parsing of the script: " + iText);
  }
}
