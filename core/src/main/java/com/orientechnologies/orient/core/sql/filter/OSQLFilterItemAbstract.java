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

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Represents an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OSQLFilterItemAbstract implements OSQLFilterItem {
  protected List<OPair<Integer, List<String>>> operationsChain = null;

  public OSQLFilterItemAbstract(final OBaseParser iQueryToParse, final String iText) {
    final List<String> parts = OStringSerializerHelper.smartSplit(iText, '.');

    setRoot(iQueryToParse, parts.get(0));

    if (parts.size() > 1) {
      operationsChain = new ArrayList<OPair<Integer, List<String>>>();

      // GET ALL SPECIAL OPERATIONS
      for (int i = 1; i < parts.size(); ++i) {
        String part = parts.get(i);
        String partUpperCase = part.toUpperCase(Locale.ENGLISH);

        if (part.indexOf('(') > -1) {
          boolean operatorFound = false;
          for (OSQLFilterFieldOperator op : OSQLFilterFieldOperator.OPERATORS)
            if (partUpperCase.startsWith(op.keyword + "(")) {
              // OPERATOR MATCH
              final List<String> arguments;

              if (op.minArguments > 0) {
                arguments = OStringSerializerHelper.getParameters(part);
                if (arguments.size() < op.minArguments || arguments.size() > op.maxArguments)
                  throw new OQueryParsingException(iQueryToParse.parserText, "Syntax error: field operator '" + op.keyword
                      + "' needs "
                      + (op.minArguments == op.maxArguments ? op.minArguments : op.minArguments + "-" + op.maxArguments)
                      + " argument(s) while has been received " + arguments.size(), 0);
              } else
                arguments = null;

              // SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
              operationsChain.add(new OPair<Integer, List<String>>(op.id, arguments));
              operatorFound = true;
              break;
            }

          if (!operatorFound)
            // ERROR: OPERATOR NOT FOUND OR MISPELLED
            throw new OQueryParsingException(iQueryToParse.parserText,
                "Syntax error: field operator not recognized between the supported ones: "
                    + Arrays.toString(OSQLFilterFieldOperator.OPERATORS), 0);
        } else {
          final List<String> list = new ArrayList<String>();
          list.add(part);
          operationsChain.add(new OPair<Integer, List<String>>(OSQLFilterFieldOperator.FIELD.id, list));
        }
      }
    }
  }

  public abstract String getRoot();

  protected abstract void setRoot(OBaseParser iQueryToParse, final String iRoot);

  public Object transformValue(final OIdentifiable iRecord, Object ioResult) {
    if (ioResult != null && operationsChain != null) {
      // APPLY OPERATIONS FOLLOWING THE STACK ORDER
      int operator = -2;

      try {
        for (OPair<Integer, List<String>> op : operationsChain) {
          operator = op.key.intValue();

          // NO ARGS OPERATORS
          if (operator == OSQLFilterFieldOperator.SIZE.id) {
            if (ioResult != null)
              if (ioResult instanceof ORecord<?>)
                ioResult = 1;
              else
                ioResult = OMultiValue.getSize(ioResult);
            else
              ioResult = 0;

          } else if (operator == OSQLFilterFieldOperator.LENGTH.id)
            ioResult = ioResult != null ? ioResult.toString().length() : 0;

          else if (operator == OSQLFilterFieldOperator.TOUPPERCASE.id)
            ioResult = ioResult != null ? ioResult.toString().toUpperCase() : null;

          else if (operator == OSQLFilterFieldOperator.TOLOWERCASE.id)
            ioResult = ioResult != null ? ioResult.toString().toLowerCase() : null;

          else if (operator == OSQLFilterFieldOperator.TRIM.id)
            ioResult = ioResult != null ? ioResult.toString().trim() : null;

          else if (operator == OSQLFilterFieldOperator.FIELD.id) {
            if (ioResult != null) {

              if (ioResult instanceof String) {
                try {
                  ioResult = new ODocument(new ORecordId((String) ioResult));
                } catch (Exception e) {
                  OLogManager.instance().error(this, "Error on reading rid with value '%s'", null, ioResult);
                  ioResult = null;
                }
              } else if (ioResult instanceof OIdentifiable)
                ioResult = ((OIdentifiable) ioResult).getRecord();

              if (ioResult != null)
                if (ioResult instanceof OCommandContext)
                  ioResult = ((OCommandContext) ioResult).getVariable(op.value.get(0));
                else
                  ioResult = ODocumentHelper.getFieldValue(ioResult, op.value.get(0));
            }

            // OTHER OPERATORS
          } else if (operator == OSQLFilterFieldOperator.CHARAT.id) {
            int index = Integer.parseInt(op.value.get(0));
            ioResult = ioResult != null ? ioResult.toString().substring(index, index + 1) : null;

          } else if (operator == OSQLFilterFieldOperator.INDEXOF.id && op.value.get(0).length() > 2) {
            String toFind = op.value.get(0).substring(1, op.value.get(0).length() - 1);
            int startIndex = op.value.size() > 1 ? Integer.parseInt(op.value.get(1)) : 0;
            ioResult = ioResult != null ? ioResult.toString().indexOf(toFind, startIndex) : null;

          } else if (operator == OSQLFilterFieldOperator.SUBSTRING.id) {
            if (op.value.size() > 1)
              ioResult = ioResult != null ? ioResult.toString().substring(Integer.parseInt(op.value.get(0)),
                  Integer.parseInt(op.value.get(1))) : null;
            else
              ioResult = ioResult != null ? ioResult.toString().substring(Integer.parseInt(op.value.get(0))) : null;

          } else if (operator == OSQLFilterFieldOperator.REPLACE.id) {
            ioResult = ioResult != null ? ioResult.toString().replace(op.value.get(0), op.value.get(1)) : null;

          } else if (operator == OSQLFilterFieldOperator.APPEND.id) {
            final Object v = getParameterValue(iRecord, op.value.get(0));
            if (v != null)
              ioResult = ioResult != null ? ioResult.toString() + v : null;

          } else if (operator == OSQLFilterFieldOperator.PREFIX.id) {
            final Object v = getParameterValue(iRecord, op.value.get(0));
            if (v != null)
              ioResult = ioResult != null ? v + ioResult.toString() : null;

          } else if (operator == OSQLFilterFieldOperator.FORMAT.id) {
            final Object v = getParameterValue(iRecord, op.value.get(0));
            if (v != null)
              if (ioResult instanceof Date)
                ioResult = new SimpleDateFormat(v.toString()).format(ioResult);
              else
                ioResult = ioResult != null ? String.format(v.toString(), ioResult) : null;
          } else if (operator == OSQLFilterFieldOperator.LEFT.id) {
            final int len = Integer.parseInt(op.value.get(0));
            ioResult = ioResult != null ? ioResult.toString().substring(0,
                len <= ioResult.toString().length() ? len : ioResult.toString().length()) : null;

          } else if (operator == OSQLFilterFieldOperator.RIGHT.id) {
            final int offset = Integer.parseInt(op.value.get(0));
            ioResult = ioResult != null ? ioResult.toString().substring(
                offset < ioResult.toString().length() ? ioResult.toString().length() - offset : 0) : null;

          } else if (operator == OSQLFilterFieldOperator.ASSTRING.id)
            ioResult = ioResult != null ? ioResult.toString() : null;
          else if (operator == OSQLFilterFieldOperator.ASINTEGER.id)
            ioResult = ioResult != null ? new Integer(ioResult.toString()) : null;
          else if (operator == OSQLFilterFieldOperator.ASLONG.id)
            if (ioResult instanceof Date)
              ioResult = ((Date) ioResult).getTime();
            else
              ioResult = ioResult != null ? new Long(ioResult.toString()) : null;
          else if (operator == OSQLFilterFieldOperator.ASFLOAT.id)
            ioResult = ioResult != null ? new Float(ioResult.toString()) : null;
          else if (operator == OSQLFilterFieldOperator.ASDECIMAL.id)
            ioResult = ioResult != null ? new BigDecimal(ioResult.toString()) : null;
          else if (operator == OSQLFilterFieldOperator.ASBOOLEAN.id) {
            if (ioResult != null) {
              if (ioResult instanceof String)
                ioResult = Boolean.valueOf((String) ioResult);
              else if (ioResult instanceof Number) {
                final int bValue = ((Number) ioResult).intValue();
                if (bValue == 0)
                  ioResult = Boolean.FALSE;
                else if (bValue == 1)
                  ioResult = Boolean.TRUE;
                else
                  // IGNORE OTHER VALUES
                  ioResult = null;
              }
            }
          } else if (operator == OSQLFilterFieldOperator.ASDATE.id) {
            if (ioResult != null) {
              if (ioResult instanceof Number)
                ioResult = new Date(((Number) ioResult).longValue());
              else if (!(ioResult instanceof Date))
                ioResult = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance()
                    .parse(ioResult.toString());
            }
          } else if (operator == OSQLFilterFieldOperator.ASDATETIME.id) {
            if (ioResult != null) {
              if (ioResult instanceof Number)
                ioResult = new Date(((Number) ioResult).longValue());
              else if (!(ioResult instanceof Date))
                ioResult = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateTimeFormatInstance()
                    .parse(ioResult.toString());
            }
          } else if (operator == OSQLFilterFieldOperator.TOJSON.id)
            ioResult = ioResult != null && ioResult instanceof ODocument ? ((ODocument) ioResult).toJSON() : null;

          else if (operator == OSQLFilterFieldOperator.KEYS.id)
            ioResult = ioResult != null && ioResult instanceof Map<?, ?> ? ((Map<?, ?>) ioResult).keySet() : null;

          else if (operator == OSQLFilterFieldOperator.VALUES.id)
            ioResult = ioResult != null && ioResult instanceof Map<?, ?> ? ((Map<?, ?>) ioResult).values() : null;
        }
      } catch (ParseException e) {
        OLogManager.instance().exception("Error on conversion of value '%s' using field operator %s", e,
            OCommandExecutionException.class, ioResult, OSQLFilterFieldOperator.getById(operator));
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
      for (OPair<Integer, List<String>> op : operationsChain) {
        buffer.append('.');
        buffer.append(OSQLFilterFieldOperator.getById(op.getKey()));
        if (op.getValue() != null) {
          final List<String> values = op.getValue();
          buffer.append('(');
          int i = 0;
          for (String v : values) {
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

  public Object getParameterValue(final OIdentifiable iRecord, final String iValue) {
    if (iValue == null)
      return null;

    if (iValue.charAt(0) == '\'' || iValue.charAt(0) == '"')
      // GET THE VALUE AS STRING
      return iValue.substring(1, iValue.length() - 1);

    // SEARCH FOR FIELD
    return ((ODocument) iRecord.getRecord()).field(iValue);
  }
}
