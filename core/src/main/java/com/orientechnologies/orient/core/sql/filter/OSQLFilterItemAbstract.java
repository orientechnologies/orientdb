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
package com.orientechnologies.orient.core.sql.filter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFunctionDelegate;

/**
 * Represents an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OSQLFilterItemAbstract implements OSQLFilterItem {

  protected List<OPair<OSQLMethod, Object[]>> operationsChain = null;

  public OSQLFilterItemAbstract(final OBaseParser iQueryToParse, final String iText) {
    final List<String> parts = OStringSerializerHelper.smartSplit(iText, '.');

    setRoot(iQueryToParse, parts.get(0));

    if (parts.size() > 1) {
      operationsChain = new ArrayList<OPair<OSQLMethod, Object[]>>();

      // GET ALL SPECIAL OPERATIONS
      for (int i = 1; i < parts.size(); ++i) {
        final String part = parts.get(i);

        final int pindex = part.indexOf('(');
        if (pindex > -1) {
          final String methodName = part.substring(0, pindex).trim().toLowerCase(Locale.ENGLISH);

          OSQLMethod method = OSQLHelper.getMethodByName(methodName);
          final Object[] arguments;
          if (method != null) {
            if (method.getMaxParams() == -1 || method.getMaxParams() > 0) {
              arguments = OStringSerializerHelper.getParameters(part).toArray();
              if (arguments.length < method.getMinParams()
                  || (method.getMaxParams() > -1 && arguments.length > method.getMaxParams()))
                throw new OQueryParsingException(iQueryToParse.parserText, "Syntax error: field operator '"
                    + method.getName()
                    + "' needs "
                    + (method.getMinParams() == method.getMaxParams() ? method.getMinParams() : method.getMinParams() + "-"
                        + method.getMaxParams()) + " argument(s) while has been received " + arguments.length, 0);
            } else
              arguments = null;

          } else {
            // LOOK FOR FUNCTION
            final OSQLFunction f = OSQLEngine.getInstance().getFunction(methodName);

            if (f == null)
              // ERROR: METHOD/FUNCTION NOT FOUND OR MISPELLED
              throw new OQueryParsingException(iQueryToParse.parserText,
                  "Syntax error: function or field operator not recognized between the supported ones: "
                      + Arrays.toString(OSQLHelper.getAllMethodNames()), 0);

            if (f.getMaxParams() == -1 || f.getMaxParams() > 0) {
              arguments = OStringSerializerHelper.getParameters(part).toArray();
              if (arguments.length < f.getMinParams() || (f.getMaxParams() > -1 && arguments.length > f.getMaxParams()))
                throw new OQueryParsingException(iQueryToParse.parserText, "Syntax error: function '" + f.getName() + "' needs "
                    + (f.getMinParams() == f.getMaxParams() ? f.getMinParams() : f.getMinParams() + "-" + f.getMaxParams())
                    + " argument(s) while has been received " + arguments.length, 0);
            } else
              arguments = null;

            method = new OSQLMethodFunctionDelegate(f);
          }

          // SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
          operationsChain.add(new OPair<OSQLMethod, Object[]>(method, arguments));

        } else {
          operationsChain.add(new OPair<OSQLMethod, Object[]>(OSQLHelper.getMethodByName(OSQLMethodField.NAME), new Object[] { part }));
        }
      }
    }
  }

  public abstract String getRoot();

  protected abstract void setRoot(OBaseParser iQueryToParse, final String iRoot);

  public Object transformValue(final OIdentifiable iRecord, final OCommandContext iContext, Object ioResult) {
    if (ioResult != null && operationsChain != null) {
      // APPLY OPERATIONS FOLLOWING THE STACK ORDER
      OSQLMethod operator = null;

      try {
        for (OPair<OSQLMethod, Object[]> op : operationsChain) {
          operator = op.getKey();

          // DON'T PASS THE CURRENT RECORD TO FORCE EVALUATING TEMPORARY RESULT
          ioResult = operator.execute(iRecord, iContext, ioResult, op.getValue());
        }
      } catch (ParseException e) {
        OLogManager.instance().exception("Error on conversion of value '%s' using field operator %s", e,
            OCommandExecutionException.class, ioResult, operator.getName());
      }
    }

    return ioResult;
  }

  public boolean hasChainOperators() {
    return operationsChain != null;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    final String root = getRoot();
    if (root != null)
      buffer.append(root);
    if (operationsChain != null) {
      for (OPair<OSQLMethod, Object[]> op : operationsChain) {
        buffer.append('.');
        buffer.append(op.getKey());
        if (op.getValue() != null) {
          final Object[] values = op.getValue();
          buffer.append('(');
          int i = 0;
          for (Object v : values) {
            if (i++ > 0)
              buffer.append(',');
            buffer.append(v);
          }
          buffer.append(')');
        }
      }
    }
    return buffer.toString();
  }
}
