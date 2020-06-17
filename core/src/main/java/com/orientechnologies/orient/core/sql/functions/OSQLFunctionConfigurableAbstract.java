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
package com.orientechnologies.orient.core.sql.functions;

/**
 * Abstract class to extend to build Custom SQL Functions that saves the configured parameters.
 * Extend it and register it with: <code>OSQLParser.getInstance().registerStatelessFunction()</code>
 * or <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL
 * engine.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OSQLFunctionConfigurableAbstract extends OSQLFunctionAbstract {
  protected Object[] configuredParameters;

  protected OSQLFunctionConfigurableAbstract(
      final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
    configuredParameters = iConfiguredParameters;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(name);
    buffer.append('(');
    if (configuredParameters != null) {
      for (int i = 0; i < configuredParameters.length; ++i) {
        if (i > 0) buffer.append(',');
        buffer.append(configuredParameters[i]);
      }
    }
    buffer.append(')');
    return buffer.toString();
  }
}
