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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Run-time query condition evaluator.
 *
 * @author Luca Garulli
 */
public class OSQLFilterCondition {
  private static final String NULL_VALUE = "null";
  protected Object            left;
  protected OQueryOperator    operator;
  protected Object            right;
  protected boolean           inBraces   = false;

  public OSQLFilterCondition(final Object iLeft, final OQueryOperator iOperator) {
    this.left = iLeft;
    this.operator = iOperator;
  }

  public OSQLFilterCondition(final Object iLeft, final OQueryOperator iOperator, final Object iRight) {
    this.left = iLeft;
    this.operator = iOperator;
    this.right = iRight;
  }

  public Object evaluate(final OIdentifiable iCurrentRecord, final ODocument iCurrentResult, final OCommandContext iContext) {
    // EXECUTE SUB QUERIES ONCE
    if (left instanceof OSQLQuery<?>) {
      left = ((OSQLQuery<?>) left).setContext(iContext).execute();
    }
    Object l = evaluate(iCurrentRecord, iCurrentResult, left, iContext);

    if (operator != null && operator.canShortCircuit(l)) {
      return l;
    }

    if (right instanceof OSQLQuery<?>) {
      right = ((OSQLQuery<?>) right).setContext(iContext).execute();
    }

    Object r = evaluate(iCurrentRecord, iCurrentResult, right, iContext);

    final OCollate collate = getCollate();

    final Object[] convertedValues = checkForConversion(iCurrentRecord, l, r, collate);
    if (convertedValues != null) {
      l = convertedValues[0];
      r = convertedValues[1];
    }

    if (operator == null) {
      if (l == null)
      // THE LEFT RETURNED NULL
      {
        return Boolean.FALSE;
      }

      // UNITARY OPERATOR: JUST RETURN LEFT RESULT
      return l;
    }

    Object result;
    try {
      result = operator.evaluateRecord(iCurrentRecord, iCurrentResult, this, l, r, iContext);
    } catch (OCommandExecutionException e) {
      throw e;
    } catch (Exception e) {
      result = Boolean.FALSE;
    }

    return result;
  }

  public OCollate getCollate() {
    if (left instanceof OSQLFilterItemField) {
      return ((OSQLFilterItemField) left).getCollate();
    } else if (right instanceof OSQLFilterItemField) {
      return ((OSQLFilterItemField) right).getCollate();
    }
    return null;
  }

  public ORID getBeginRidRange() {
    if (operator == null) {
      if (left instanceof OSQLFilterCondition) {
        return ((OSQLFilterCondition) left).getBeginRidRange();
      } else {
        return null;
      }
    }

    return operator.getBeginRidRange(left, right);
  }

  public ORID getEndRidRange() {
    if (operator == null) {
      if (left instanceof OSQLFilterCondition) {
        return ((OSQLFilterCondition) left).getEndRidRange();
      } else {
        return null;
      }
    }

    return operator.getEndRidRange(left, right);
  }

  public List<String> getInvolvedFields(final List<String> list) {
    extractInvolvedFields(getLeft(), list);
    extractInvolvedFields(getRight(), list);

    return list;
  }

  private void extractInvolvedFields(Object left, List<String> list) {
    if (left != null) {
      if (left instanceof OSQLFilterItemField) {
        if (((OSQLFilterItemField) left).isFieldChain()) {
          list.add(((OSQLFilterItemField) left).getFieldChain().getItemName(
              ((OSQLFilterItemField) left).getFieldChain().getItemCount() - 1));
        }
      } else if (left instanceof OSQLFilterCondition) {
        ((OSQLFilterCondition) left).getInvolvedFields(list);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder(128);

    buffer.append('(');
    buffer.append(left);
    if (operator != null) {
      buffer.append(' ');
      buffer.append(operator);
      buffer.append(' ');
      if (right instanceof String) {
        buffer.append('\'');
      }
      buffer.append(right);
      if (right instanceof String) {
        buffer.append('\'');
      }
      buffer.append(')');
    }

    return buffer.toString();
  }

  public Object getLeft() {
    return left;
  }

  public void setLeft(final Object iValue) {
    left = iValue;
  }

  public Object getRight() {
    return right;
  }

  public void setRight(final Object iValue) {
    right = iValue;
  }

  public OQueryOperator getOperator() {
    return operator;
  }

  protected Integer getInteger(Object iValue) {
    if (iValue == null) {
      return null;
    }

    final String stringValue = iValue.toString();

    if (NULL_VALUE.equals(stringValue)) {
      return null;
    }
    if (OSQLHelper.DEFINED.equals(stringValue)) {
      return null;
    }

    if (OStringSerializerHelper.contains(stringValue, '.') || OStringSerializerHelper.contains(stringValue, ',')) {
      return (int) Float.parseFloat(stringValue);
    } else {
      return stringValue.length() > 0 ? new Integer(stringValue) : new Integer(0);
    }
  }

  protected Float getFloat(final Object iValue) {
    if (iValue == null) {
      return null;
    }

    final String stringValue = iValue.toString();

    if (NULL_VALUE.equals(stringValue)) {
      return null;
    }

    return stringValue.length() > 0 ? new Float(stringValue) : new Float(0);
  }

  protected Date getDate(final Object value) {
    if (value == null) {
      return null;
    }

    final OStorageConfiguration config = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration();

    if (value instanceof Long) {
      Calendar calendar = Calendar.getInstance(config.getTimeZone());
      calendar.setTimeInMillis(((Long) value));
      return calendar.getTime();
    }

    String stringValue = value.toString();

    if (NULL_VALUE.equals(stringValue)) {
      return null;
    }

    if (stringValue.length() <= 0) {
      return null;
    }

    if (Pattern.matches("^\\d+$", stringValue)) {
      return new Date(Long.valueOf(stringValue).longValue());
    }

    SimpleDateFormat formatter = config.getDateFormatInstance();

    if (stringValue.length() > config.dateFormat.length())
    // ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
    {
      formatter = config.getDateTimeFormatInstance();
    }

    try {
      return formatter.parse(stringValue);
    } catch (ParseException pe) {
      try {
        return new Date(new Double(stringValue).longValue());
      } catch (Exception pe2) {
        throw new OQueryParsingException("Error on conversion of date '" + stringValue + "' using the format: "
            + formatter.toPattern(), pe2);
      }
    }
  }

  protected Object evaluate(OIdentifiable iCurrentRecord, final ODocument iCurrentResult, final Object iValue,
      final OCommandContext iContext) {
    if (iValue == null)
      return null;

    if (iCurrentRecord != null) {
      iCurrentRecord = iCurrentRecord.getRecord();
      if (iCurrentRecord!=null && ((ODocument)iCurrentRecord).getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
        try {
          iCurrentRecord = iCurrentRecord.getRecord().load();
        } catch (ORecordNotFoundException e) {
          return null;
        }
      }
    }

    if (iValue instanceof OSQLFilterItem) {
      return ((OSQLFilterItem) iValue).getValue(iCurrentRecord, iCurrentResult, iContext);
    }

    if (iValue instanceof OSQLFilterCondition) {
      // NESTED CONDITION: EVALUATE IT RECURSIVELY
      return ((OSQLFilterCondition) iValue).evaluate(iCurrentRecord, iCurrentResult, iContext);
    }

    if (iValue instanceof OSQLFunctionRuntime) {
      // STATELESS FUNCTION: EXECUTE IT
      final OSQLFunctionRuntime f = (OSQLFunctionRuntime) iValue;
      return f.execute(iCurrentRecord, iCurrentRecord, iCurrentResult, iContext);
    }

    if (OMultiValue.isMultiValue(iValue)) {
      final Iterable<?> multiValue = OMultiValue.getMultiValueIterable(iValue);

      // MULTI VALUE: RETURN A COPY
      final ArrayList<Object> result = new ArrayList<Object>(OMultiValue.getSize(iValue));

      for (final Object value : multiValue) {
        if (value instanceof OSQLFilterItem) {
          result.add(((OSQLFilterItem) value).getValue(iCurrentRecord, iCurrentResult, iContext));
        } else {
          result.add(value);
        }
      }
      return result;
    }

    // SIMPLE VALUE: JUST RETURN IT
    return iValue;
  }

  private Object[] checkForConversion(final OIdentifiable o, Object l, Object r, final OCollate collate) {
    Object[] result = null;

    final Object oldL = l;
    final Object oldR = r;
    if (collate != null) {

      l = collate.transform(l);
      r = collate.transform(r);

      if (l != oldL || r != oldR)
      // CHANGED
      {
        result = new Object[] { l, r };
      }
    }

    try {
      // DEFINED OPERATOR
      if ((oldR instanceof String && oldR.equals(OSQLHelper.DEFINED))
          || (oldL instanceof String && oldL.equals(OSQLHelper.DEFINED))) {
        result = new Object[] { ((OSQLFilterItemAbstract) this.left).getRoot(), r };
      }

      // NOT_NULL OPERATOR
      else if ((oldR instanceof String && oldR.equals(OSQLHelper.NOT_NULL))
          || (oldL instanceof String && oldL.equals(OSQLHelper.NOT_NULL))) {
        result = null;
      } else if (l != null && r != null && !l.getClass().isAssignableFrom(r.getClass())
          && !r.getClass().isAssignableFrom(l.getClass()))
      // INTEGERS
      {
        if (r instanceof Integer && !(l instanceof Number || l instanceof Collection)) {
          if (l instanceof String && ((String) l).indexOf('.') > -1) {
            result = new Object[] { new Float((String) l).intValue(), r };
          } else if (l instanceof Date) {
            result = new Object[] { ((Date) l).getTime(), r };
          } else if (!(l instanceof OQueryRuntimeValueMulti) && !(l instanceof Collection<?>) && !l.getClass().isArray()
              && !(l instanceof Map)) {
            result = new Object[] { getInteger(l), r };
          }
        } else if (l instanceof Integer && !(r instanceof Number || r instanceof Collection)) {
          if (r instanceof String && ((String) r).indexOf('.') > -1) {
            result = new Object[] { l, new Float((String) r).intValue() };
          } else if (r instanceof Date) {
            result = new Object[] { l, ((Date) r).getTime() };
          } else if (!(r instanceof OQueryRuntimeValueMulti) && !(r instanceof Collection<?>) && !r.getClass().isArray()
              && !(r instanceof Map)) {
            result = new Object[] { l, getInteger(r) };
          }
        }

        // DATES
        else if (r instanceof Date && !(l instanceof Collection || l instanceof Date)) {
          result = new Object[] { getDate(l), r };
        } else if (l instanceof Date && !(r instanceof Collection || r instanceof Date)) {
          result = new Object[] { l, getDate(r) };
        }

        // FLOATS
        else if (r instanceof Float && !(l instanceof Float || l instanceof Collection)) {
          result = new Object[] { getFloat(l), r };
        } else if (l instanceof Float && !(r instanceof Float || r instanceof Collection)) {
          result = new Object[] { l, getFloat(r) };
        }

        // RIDS
        else if (r instanceof ORID && l instanceof String && !oldL.equals(OSQLHelper.NOT_NULL)) {
          result = new Object[] { new ORecordId((String) l), r };
        } else if (l instanceof ORID && r instanceof String && !oldR.equals(OSQLHelper.NOT_NULL)) {
          result = new Object[] { l, new ORecordId((String) r) };
        }
      }
    } catch (Exception e) {
      // JUST IGNORE CONVERSION ERRORS
    }

    return result;
  }
}
