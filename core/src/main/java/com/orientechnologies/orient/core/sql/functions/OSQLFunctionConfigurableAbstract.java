/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions;

/**
 * Abstract class to extend to build Custom SQL Functions that saves the configured parameters. Extend it and register it with:
 * <code>OSQLParser.getInstance().registerStatelessFunction()</code> or
 * <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunctionConfigurableAbstract extends OSQLFunctionAbstract {
  protected Object[] configuredParameters;

  protected OSQLFunctionConfigurableAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
    configuredParameters = iConfiguredParameters;
  }
}
