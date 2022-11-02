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

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OBasicLegacyResultSet;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

/**
 * Dynamic script engine for OrientDB SQL commands. This implementation is multi-threads.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLScriptEngine implements ScriptEngine {

  public static final String NAME = "sql";
  private ScriptEngineFactory factory;

  public OSQLScriptEngine(ScriptEngineFactory factory) {
    this.factory = factory;
  }

  @Override
  public Object eval(String script, ScriptContext context) throws ScriptException {
    return eval(script, (Bindings) null);
  }

  @Override
  public Object eval(Reader reader, ScriptContext context) throws ScriptException {
    return eval(reader, (Bindings) null);
  }

  @Override
  public Object eval(String script) throws ScriptException {
    return eval(script, (Bindings) null);
  }

  @Override
  public Object eval(Reader reader) throws ScriptException {
    return eval(reader, (Bindings) null);
  }

  @Override
  public Object eval(String script, Bindings n) throws ScriptException {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      throw new OCommandExecutionException("No database available in threadlocal");
    }
    Map<Object, Object> params = convertToParameters(n);
    OResultSet queryResult;
    if (params.keySet().stream().anyMatch(x -> !(x instanceof String))) {
      queryResult = db.execute("sql", script, params);
    } else {
      queryResult = db.execute("sql", script, (Map) params);
    }
    try (OResultSet res = queryResult) {
      OLegacyResultSet finalResult = new OBasicLegacyResultSet();
      res.stream().forEach(x -> finalResult.add(x));
      return finalResult;
    }
  }

  @SuppressWarnings("unchecked")
  protected Map<Object, Object> convertToParameters(Object... iArgs) {
    final Map<Object, Object> params;

    if (iArgs.length == 1 && iArgs[0] instanceof Map) {
      params = (Map<Object, Object>) iArgs[0];
    } else {
      if (iArgs.length == 1
          && iArgs[0] != null
          && iArgs[0].getClass().isArray()
          && iArgs[0] instanceof Object[]) iArgs = (Object[]) iArgs[0];

      params = new HashMap<Object, Object>(iArgs.length);
      for (int i = 0; i < iArgs.length; ++i) {
        Object par = iArgs[i];

        if (par instanceof OIdentifiable && ((OIdentifiable) par).getIdentity().isValid())
          // USE THE RID ONLY
          par = ((OIdentifiable) par).getIdentity();

        params.put(i, par);
      }
    }
    return params;
  }

  @Override
  public Object eval(Reader reader, Bindings n) throws ScriptException {
    final StringBuilder buffer = new StringBuilder();
    try {
      while (reader.ready()) buffer.append((char) reader.read());
    } catch (IOException e) {
      throw new ScriptException(e);
    }

    return new OCommandScript(buffer.toString()).execute(n);
  }

  @Override
  public void put(String key, Object value) {}

  @Override
  public Object get(String key) {
    return null;
  }

  @Override
  public Bindings getBindings(int scope) {
    return new SimpleBindings();
  }

  @Override
  public void setBindings(Bindings bindings, int scope) {}

  @Override
  public Bindings createBindings() {
    return new SimpleBindings();
  }

  @Override
  public ScriptContext getContext() {
    return null;
  }

  @Override
  public void setContext(ScriptContext context) {}

  @Override
  public ScriptEngineFactory getFactory() {
    return factory;
  }
}
