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
package com.orientechnologies.orient.core.sql.method;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorNotFoundException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
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
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import java.util.List;

/**
 * Wraps function managing the binding of parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodRuntime extends OSQLFilterItemAbstract
    implements Comparable<OSQLMethodRuntime> {

  public OSQLMethod method;
  public Object[] configuredParameters;
  public Object[] runtimeParameters;

  public OSQLMethodRuntime(final OBaseParser iQueryToParse, final String iText) {
    super(iQueryToParse, iText);
  }

  public OSQLMethodRuntime(final OSQLMethod iFunction) {
    method = iFunction;
  }

  /**
   * Execute a method.
   *
   * @param iCurrentRecord Current record
   * @param iCurrentResult TODO
   * @param iContext
   * @return
   */
  public Object execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final OCommandContext iContext) {
    if (iThis == null) return null;

    if (configuredParameters != null) {
      // RESOLVE VALUES USING THE CURRENT RECORD
      for (int i = 0; i < configuredParameters.length; ++i) {
        runtimeParameters[i] = configuredParameters[i];

        if (method.evaluateParameters()) {
          if (configuredParameters[i] instanceof OSQLFilterItemField) {
            runtimeParameters[i] =
                ((OSQLFilterItemField) configuredParameters[i])
                    .getValue(iCurrentRecord, iCurrentResult, iContext);
            if (runtimeParameters[i] == null && iCurrentResult instanceof OIdentifiable)
              // LOOK INTO THE CURRENT RESULT
              runtimeParameters[i] =
                  ((OSQLFilterItemField) configuredParameters[i])
                      .getValue((OIdentifiable) iCurrentResult, iCurrentResult, iContext);
          } else if (configuredParameters[i] instanceof OSQLMethodRuntime)
            runtimeParameters[i] =
                ((OSQLMethodRuntime) configuredParameters[i])
                    .execute(iThis, iCurrentRecord, iCurrentResult, iContext);
          else if (configuredParameters[i] instanceof OSQLFunctionRuntime)
            runtimeParameters[i] =
                ((OSQLFunctionRuntime) configuredParameters[i])
                    .execute(iCurrentRecord, iCurrentRecord, iCurrentResult, iContext);
          else if (configuredParameters[i] instanceof OSQLFilterItemVariable) {
            runtimeParameters[i] =
                ((OSQLFilterItemVariable) configuredParameters[i])
                    .getValue(iCurrentRecord, iCurrentResult, iContext);
            if (runtimeParameters[i] == null && iCurrentResult instanceof OIdentifiable)
              // LOOK INTO THE CURRENT RESULT
              runtimeParameters[i] =
                  ((OSQLFilterItemVariable) configuredParameters[i])
                      .getValue((OIdentifiable) iCurrentResult, iCurrentResult, iContext);
          } else if (configuredParameters[i] instanceof OCommandSQL) {
            try {
              runtimeParameters[i] =
                  ((OCommandSQL) configuredParameters[i]).setContext(iContext).execute();
            } catch (OCommandExecutorNotFoundException ignore) {
              // TRY WITH SIMPLE CONDITION
              final String text = ((OCommandSQL) configuredParameters[i]).getText();
              final OSQLPredicate pred = new OSQLPredicate(text);
              runtimeParameters[i] =
                  pred.evaluate(
                      iCurrentRecord instanceof ORecord ? (ORecord) iCurrentRecord : null,
                      (ODocument) iCurrentResult,
                      iContext);
              // REPLACE ORIGINAL PARAM
              configuredParameters[i] = pred;
            }
          } else if (configuredParameters[i] instanceof OSQLPredicate)
            runtimeParameters[i] =
                ((OSQLPredicate) configuredParameters[i])
                    .evaluate(
                        iCurrentRecord.getRecord(),
                        (iCurrentRecord instanceof ODocument ? (ODocument) iCurrentResult : null),
                        iContext);
          else if (configuredParameters[i] instanceof String) {
            if (configuredParameters[i].toString().startsWith("\"")
                || configuredParameters[i].toString().startsWith("'"))
              runtimeParameters[i] = OIOUtils.getStringContent(configuredParameters[i]);
          }
        }
      }

      if (method.getMaxParams() == -1 || method.getMaxParams() > 0) {
        if (runtimeParameters.length < method.getMinParams()
            || (method.getMaxParams() > -1 && runtimeParameters.length > method.getMaxParams())) {
          String params;
          if (method.getMinParams() == method.getMaxParams()) {
            params = "" + method.getMinParams();
          } else {
            params = method.getMinParams() + "-" + method.getMaxParams();
          }
          throw new OCommandExecutionException(
              "Syntax error: function '"
                  + method.getName()
                  + "' needs "
                  + (params)
                  + " argument(s) while has been received "
                  + runtimeParameters.length);
        }
      }
    }

    final Object functionResult =
        method.execute(iThis, iCurrentRecord, iContext, iCurrentResult, runtimeParameters);

    return transformValue(iCurrentRecord, iContext, functionResult);
  }

  @Override
  public Object getValue(
      final OIdentifiable iRecord, Object iCurrentResult, OCommandContext iContext) {
    final ODocument current = iRecord != null ? (ODocument) iRecord.getRecord() : null;
    return execute(current, current, null, iContext);
  }

  @Override
  public String getRoot() {
    return method.getName();
  }

  @Override
  protected void setRoot(final OBaseParser iQueryToParse, final String iText) {
    final int beginParenthesis = iText.indexOf('(');

    // SEARCH FOR THE FUNCTION
    final String funcName = iText.substring(0, beginParenthesis);

    final List<String> funcParamsText = OStringSerializerHelper.getParameters(iText);

    method = OSQLEngine.getMethod(funcName);
    if (method == null) throw new OCommandSQLParsingException("Unknown method " + funcName + "()");

    // PARSE PARAMETERS
    this.configuredParameters = new Object[funcParamsText.size()];
    for (int i = 0; i < funcParamsText.size(); ++i)
      this.configuredParameters[i] = funcParamsText.get(i);

    setParameters(configuredParameters, true);
  }

  public OSQLMethodRuntime setParameters(final Object[] iParameters, final boolean iEvaluate) {
    if (iParameters != null) {
      this.configuredParameters = new Object[iParameters.length];
      for (int i = 0; i < iParameters.length; ++i) {
        this.configuredParameters[i] = iParameters[i];

        if (iParameters[i] != null) {
          if (iParameters[i] instanceof String && !iParameters[i].toString().startsWith("[")) {
            final Object v = OSQLHelper.parseValue(null, null, iParameters[i].toString(), null);
            if (v == OSQLHelper.VALUE_NOT_PARSED
                || (v != null
                    && OMultiValue.isMultiValue(v)
                    && OMultiValue.getFirstValue(v) == OSQLHelper.VALUE_NOT_PARSED)) continue;

            configuredParameters[i] = v;
          }
        } else this.configuredParameters[i] = null;
      }

      // COPY STATIC VALUES
      this.runtimeParameters = new Object[configuredParameters.length];
      for (int i = 0; i < configuredParameters.length; ++i) {
        if (!(configuredParameters[i] instanceof OSQLFilterItemField)
            && !(configuredParameters[i] instanceof OSQLMethodRuntime))
          runtimeParameters[i] = configuredParameters[i];
      }
    }

    return this;
  }

  public OSQLMethod getMethod() {
    return method;
  }

  public Object[] getConfiguredParameters() {
    return configuredParameters;
  }

  public Object[] getRuntimeParameters() {
    return runtimeParameters;
  }

  @Override
  public int compareTo(final OSQLMethodRuntime o) {
    return method.compareTo(o.getMethod());
  }
}
