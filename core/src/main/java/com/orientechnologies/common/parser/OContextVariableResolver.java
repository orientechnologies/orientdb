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
package com.orientechnologies.common.parser;

import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Resolve variables by using a context.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (luca.garulli--at--assetdata.it)
 */
public class OContextVariableResolver implements OVariableParserListener {
  public static final String VAR_BEGIN = "${";
  public static final String VAR_END = "}";

  private final OCommandContext context;

  public OContextVariableResolver(final OCommandContext iContext) {
    this.context = iContext;
  }

  public String parse(final String iValue) {
    return parse(iValue, null);
  }

  public String parse(final String iValue, final String iDefault) {
    if (iValue == null) return iDefault;

    return (String) OVariableParser.resolveVariables(iValue, VAR_BEGIN, VAR_END, this, iDefault);
  }

  @Override
  public String resolve(final String variable) {
    if (variable == null) return null;

    final Object value = context.getVariable(variable);

    if (value != null) return value.toString();

    return null;
  }
}
