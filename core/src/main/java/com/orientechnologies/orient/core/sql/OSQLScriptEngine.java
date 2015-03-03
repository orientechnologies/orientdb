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

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.script.OCommandScript;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.Reader;

/**
 * Dynamic script engine for OrientDB SQL commands. This implementation is multi-threads.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLScriptEngine implements ScriptEngine {

  public static final String  NAME = "sql";
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
    return new OCommandScript(script).execute(n);
  }

  @Override
  public Object eval(Reader reader, Bindings n) throws ScriptException {
    final StringBuilder buffer = new StringBuilder();
    try {
      while (reader.ready())
        buffer.append((char) reader.read());
    } catch (IOException e) {
      throw new ScriptException(e);
    }

    return new OCommandScript(buffer.toString()).execute(n);
  }

  @Override
  public void put(String key, Object value) {
  }

  @Override
  public Object get(String key) {
    return null;
  }

  @Override
  public Bindings getBindings(int scope) {
    return new SimpleBindings();
  }

  @Override
  public void setBindings(Bindings bindings, int scope) {
  }

  @Override
  public Bindings createBindings() {
    return new SimpleBindings();
  }

  @Override
  public ScriptContext getContext() {
    return null;
  }

  @Override
  public void setContext(ScriptContext context) {
  }

  @Override
  public ScriptEngineFactory getFactory() {
    return factory;
  }
}
