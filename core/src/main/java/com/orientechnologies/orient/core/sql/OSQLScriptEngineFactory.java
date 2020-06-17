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

import com.orientechnologies.orient.core.OConstants;
import java.util.ArrayList;
import java.util.List;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;

/**
 * Dynamic script engine factory for OrientDB SQL commands.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLScriptEngineFactory implements ScriptEngineFactory {

  private static final List<String> NAMES = new ArrayList<String>();
  private static final List<String> EXTENSIONS = new ArrayList<String>();

  static {
    NAMES.add(OSQLScriptEngine.NAME);
    EXTENSIONS.add(OSQLScriptEngine.NAME);
  }

  @Override
  public String getEngineName() {
    return OSQLScriptEngine.NAME;
  }

  @Override
  public String getEngineVersion() {
    return OConstants.getVersion();
  }

  @Override
  public List<String> getExtensions() {
    return EXTENSIONS;
  }

  @Override
  public List<String> getMimeTypes() {
    return null;
  }

  @Override
  public List<String> getNames() {
    return NAMES;
  }

  @Override
  public String getLanguageName() {
    return OSQLScriptEngine.NAME;
  }

  @Override
  public String getLanguageVersion() {
    return OConstants.getVersion();
  }

  @Override
  public Object getParameter(String key) {
    return null;
  }

  @Override
  public String getMethodCallSyntax(String obj, String m, String... args) {
    return null;
  }

  @Override
  public String getOutputStatement(String toDisplay) {
    return null;
  }

  @Override
  public String getProgram(String... statements) {
    final StringBuilder buffer = new StringBuilder();
    for (String s : statements) buffer.append(s).append(";\n");
    return buffer.toString();
  }

  @Override
  public ScriptEngine getScriptEngine() {
    return new OSQLScriptEngine(this);
  }
}
