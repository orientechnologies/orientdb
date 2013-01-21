/*
 * Copyright 2013 Geomatys.
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
package com.orientechnologies.orient.core.sql.method;

import java.util.List;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;

/**
 * Wraps methods managing the binding of parameters.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OSQLMethodRuntime extends OSQLFunctionRuntime {

  public OSQLMethodRuntime(final OBaseParser iQueryToParse, final String iText) {
    super(iQueryToParse, iText);
  }

  @Override
  protected void setRoot(final OBaseParser iQueryToParse, final String iText) {
    final int beginParenthesis = iText.indexOf('(');

    // SEARCH FOR THE METHOD
    final String methodName = iText.substring(0, beginParenthesis);

    final List<String> funcParamsText = OStringSerializerHelper.getParameters(iText);

    function = getMethod(methodName);
    if (function == null)
      throw new OCommandSQLParsingException("Unknow method " + methodName );

    // PARSE PARAMETERS
    this.configuredParameters = new Object[funcParamsText.size()];
    for (int i = 0; i < funcParamsText.size(); ++i) {
      this.configuredParameters[i] = OSQLHelper.parseValue(null, iQueryToParse, funcParamsText.get(i), null);
    }

    function.config(configuredParameters);

    // COPY STATIC VALUES
    this.runtimeParameters = new Object[configuredParameters.length];
    for (int i = 0; i < configuredParameters.length; ++i) {
      if (!(configuredParameters[i] instanceof OSQLFilterItemField) && !(configuredParameters[i] instanceof OSQLMethodRuntime))
        runtimeParameters[i] = configuredParameters[i];
    }
  }

}
