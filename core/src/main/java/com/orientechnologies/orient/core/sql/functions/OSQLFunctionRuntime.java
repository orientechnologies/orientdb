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

import java.util.List;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorNotFoundException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemAbstract;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;

/**
 * Wraps functions managing the binding of parameters.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionRuntime extends OSQLFilterItemAbstract {

  public OSQLFunction function;
  public Object[]     configuredParameters;
  public Object[]     runtimeParameters;

  public OSQLFunctionRuntime(final OBaseParser iQueryToParse, final String iText) {
    super(iQueryToParse, iText);
  }

  public boolean aggregateResults() {
    return function.aggregateResults();
  }

  public boolean filterResult() {
    return function.filterResult();
  }

  /**
   * Execute a function.
   * 
   * @param iCurrentRecord
   *          Current record
   * @param iCurrentResult
   *          TODO
   * @param iRequester
   * @return
   */
  public Object execute(final OIdentifiable iCurrentRecord, final Object iCurrentResult, final OCommandContext iContext) {
    // RESOLVE VALUES USING THE CURRENT RECORD
    for (int i = 0; i < configuredParameters.length; ++i) {
      if (configuredParameters[i] instanceof OSQLFilterItemField)
        runtimeParameters[i] = ((OSQLFilterItemField) configuredParameters[i]).getValue(iCurrentRecord, iContext);
      else if (configuredParameters[i] instanceof OSQLFunctionRuntime)
        runtimeParameters[i] = ((OSQLFunctionRuntime) configuredParameters[i]).execute(iCurrentRecord, iCurrentResult, iContext);
      else if (configuredParameters[i] instanceof OSQLFilterItemVariable)
        runtimeParameters[i] = ((OSQLFilterItemVariable) configuredParameters[i]).getValue(iCurrentRecord, iContext);
      else if (configuredParameters[i] instanceof OCommandSQL) {
        try {
          runtimeParameters[i] = ((OCommandSQL) configuredParameters[i]).setContext(iContext).execute();
        } catch (OCommandExecutorNotFoundException e) {
          // TRY WITH SIMPLE CONDITION
          final String text = ((OCommandSQL) configuredParameters[i]).getText();
          final OSQLPredicate pred = new OSQLPredicate(text);
          runtimeParameters[i] = pred.evaluate(iCurrentRecord instanceof ORecord<?> ? (ORecord<?>) iCurrentRecord : null,
              (ODocument) iCurrentResult, iContext);
          // REPLACE ORIGINAL PARAM
          configuredParameters[i] = pred;

        }
      } else if (configuredParameters[i] instanceof OSQLPredicate) {
        runtimeParameters[i] = ((OSQLPredicate) configuredParameters[i]).evaluate(iCurrentRecord.getRecord(),
            (iCurrentRecord instanceof ODocument ? (ODocument) iCurrentResult : null), iContext);
      } else {
        // plain value
        runtimeParameters[i] = configuredParameters[i];
      }
    }

    final Object functionResult = function.execute(iCurrentRecord, iCurrentResult, runtimeParameters, iContext);

    return transformValue(iCurrentRecord, iContext, functionResult);
  }

  public Object getResult() {
    return transformValue(null, null, function.getResult());
  }

  public void setResult(final Object iValue) {
    function.setResult(iValue);
  }

  @Override
  public Object getValue(final OIdentifiable iRecord, OCommandContext iContext) {
    return execute(iRecord != null ? (ORecordSchemaAware<?>) iRecord.getRecord() : null, null, iContext);
  }

  @Override
  public String getRoot() {
    return function.getName();
  }

  @Override
  protected void setRoot(final OBaseParser iQueryToParse, final String iText) {
    final int beginParenthesis = iText.indexOf('(');

    // SEARCH FOR THE FUNCTION
    final String funcName = iText.substring(0, beginParenthesis);

    final List<String> funcParamsText = OStringSerializerHelper.getParameters(iText);

    function = OSQLEngine.getInstance().getFunction(funcName);
    if (function == null)
      throw new OCommandSQLParsingException("Unknow function " + funcName + "()");

    // STRICT CHECK ON PARAMETERS
    // if (function.getMinParams() > -1 && funcParamsText.size() < function.getMinParams() || function.getMaxParams() > -1
    // && funcParamsText.size() > function.getMaxParams())
    // throw new IllegalArgumentException("Syntax error. Expected: " + function.getSyntax());

    // PARSE PARAMETERS
    this.configuredParameters = new Object[funcParamsText.size()];
    for (int i = 0; i < funcParamsText.size(); ++i) {
      this.configuredParameters[i] = OSQLHelper.parseValue(null, iQueryToParse, funcParamsText.get(i), null);
    }

    function.config(configuredParameters);

    // COPY STATIC VALUES
    this.runtimeParameters = new Object[configuredParameters.length];
    for (int i = 0; i < configuredParameters.length; ++i) {
      if (!(configuredParameters[i] instanceof OSQLFilterItemField) && !(configuredParameters[i] instanceof OSQLFunctionRuntime))
        runtimeParameters[i] = configuredParameters[i];
    }
  }

  public OSQLFunction getFunction() {
    return function;
  }

  public Object[] getConfiguredParameters() {
    return configuredParameters;
  }

  public Object[] getRuntimeParameters() {
    return runtimeParameters;
  }
}
