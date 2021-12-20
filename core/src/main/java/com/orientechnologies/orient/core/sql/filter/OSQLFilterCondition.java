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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
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
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMatches;
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
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFilterCondition {
  private static final String NULL_VALUE = "null";
  protected Object left;
  protected OQueryOperator operator;
  protected Object right;
  protected boolean inBraces = false;

  public OSQLFilterCondition(final Object iLeft, final OQueryOperator iOperator) {
    this.left = iLeft;
    this.operator = iOperator;
  }

  public OSQLFilterCondition(
      final Object iLeft, final OQueryOperator iOperator, final Object iRight) {
    this.left = iLeft;
    this.operator = iOperator;
    this.right = iRight;
  }

  public Object evaluate(
      final OIdentifiable iCurrentRecord,
      final ODocument iCurrentResult,
      final OCommandContext iContext) {
    boolean binaryEvaluation =
        operator != null
            && operator.isSupportingBinaryEvaluate()
            && iCurrentRecord != null
            && iCurrentRecord.getIdentity().isPersistent();

    if (left instanceof OSQLQuery<?>)
      // EXECUTE SUB QUERIES ONLY ONCE
      left = ((OSQLQuery<?>) left).setContext(iContext).execute();

    Object l = evaluate(iCurrentRecord, iCurrentResult, left, iContext, binaryEvaluation);

    if (operator == null || operator.canShortCircuit(l)) return l;

    if (right instanceof OSQLQuery<?>)
      // EXECUTE SUB QUERIES ONLY ONCE
      right = ((OSQLQuery<?>) right).setContext(iContext).execute();

    Object r = evaluate(iCurrentRecord, iCurrentResult, right, iContext, binaryEvaluation);
    OImmutableSchema schema =
        ODatabaseRecordThreadLocal.instance().get().getMetadata().getImmutableSchemaSnapshot();

    if (binaryEvaluation && l instanceof OBinaryField) {
      if (r != null && !(r instanceof OBinaryField)) {
        final OType type = OType.getTypeByValue(r);

        if (ORecordSerializerBinary.INSTANCE
            .getCurrentSerializer()
            .getComparator()
            .isBinaryComparable(type)) {
          final BytesContainer bytes = new BytesContainer();
          ORecordSerializerBinary.INSTANCE
              .getCurrentSerializer()
              .serializeValue(bytes, r, type, null, schema, null);
          bytes.offset = 0;
          final OCollate collate =
              r instanceof OSQLFilterItemField
                  ? ((OSQLFilterItemField) r).getCollate(iCurrentRecord)
                  : null;
          r = new OBinaryField(null, type, bytes, collate);
          if (!(right instanceof OSQLFilterItem || right instanceof OSQLFilterCondition))
            // FIXED VALUE, REPLACE IT
            right = r;
        }
      } else if (r instanceof OBinaryField)
        // GET THE COPY OR MT REASONS
        r = ((OBinaryField) r).copy();
    }

    if (binaryEvaluation && r instanceof OBinaryField) {
      if (l != null && !(l instanceof OBinaryField)) {
        final OType type = OType.getTypeByValue(l);
        if (ORecordSerializerBinary.INSTANCE
            .getCurrentSerializer()
            .getComparator()
            .isBinaryComparable(type)) {
          final BytesContainer bytes = new BytesContainer();
          ORecordSerializerBinary.INSTANCE
              .getCurrentSerializer()
              .serializeValue(bytes, l, type, null, schema, null);
          bytes.offset = 0;
          final OCollate collate =
              l instanceof OSQLFilterItemField
                  ? ((OSQLFilterItemField) l).getCollate(iCurrentRecord)
                  : null;
          l = new OBinaryField(null, type, bytes, collate);
          if (!(left instanceof OSQLFilterItem || left instanceof OSQLFilterCondition))
            // FIXED VALUE, REPLACE IT
            left = l;
        }
      } else if (l instanceof OBinaryField)
        // GET THE COPY OR MT REASONS
        l = ((OBinaryField) l).copy();
    }

    if (binaryEvaluation) binaryEvaluation = l instanceof OBinaryField && r instanceof OBinaryField;

    if (!binaryEvaluation) {
      // no collate for regular expressions, otherwise quotes will result in no match
      final OCollate collate =
          operator instanceof OQueryOperatorMatches ? null : getCollate(iCurrentRecord);
      final Object[] convertedValues = checkForConversion(iCurrentRecord, l, r, collate);
      if (convertedValues != null) {
        l = convertedValues[0];
        r = convertedValues[1];
      }
    }

    Object result;
    try {
      result =
          operator.evaluateRecord(
              iCurrentRecord,
              iCurrentResult,
              this,
              l,
              r,
              iContext,
              ORecordSerializerBinary.INSTANCE.getCurrentSerializer());
    } catch (OCommandExecutionException e) {
      throw e;
    } catch (Exception e) {
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Error on evaluating expression (%s)", e, toString());
      result = Boolean.FALSE;
    }

    return result;
  }

  @Deprecated
  public OCollate getCollate() {
    if (left instanceof OSQLFilterItemField) {
      return ((OSQLFilterItemField) left).getCollate();
    } else if (right instanceof OSQLFilterItemField) {
      return ((OSQLFilterItemField) right).getCollate();
    }
    return null;
  }

  public OCollate getCollate(OIdentifiable doc) {
    if (left instanceof OSQLFilterItemField) {
      return ((OSQLFilterItemField) left).getCollate(doc);
    } else if (right instanceof OSQLFilterItemField) {
      return ((OSQLFilterItemField) right).getCollate(doc);
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
          list.add(
              ((OSQLFilterItemField) left)
                  .getFieldChain()
                  .getItemName(((OSQLFilterItemField) left).getFieldChain().getItemCount() - 1));
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

    if (OStringSerializerHelper.contains(stringValue, '.')
        || OStringSerializerHelper.contains(stringValue, ',')) {
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

    final OStorageConfiguration config =
        ODatabaseRecordThreadLocal.instance().get().getStorageInfo().getConfiguration();

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

    if (stringValue.length() > config.getDateFormat().length())
    // ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
    {
      formatter = config.getDateTimeFormatInstance();
    }

    try {
      return formatter.parse(stringValue);
    } catch (ParseException ignore) {
      try {
        return new Date(new Double(stringValue).longValue());
      } catch (Exception pe2) {
        throw OException.wrapException(
            new OQueryParsingException(
                "Error on conversion of date '"
                    + stringValue
                    + "' using the format: "
                    + formatter.toPattern()),
            pe2);
      }
    }
  }

  protected Object evaluate(
      OIdentifiable iCurrentRecord,
      final ODocument iCurrentResult,
      final Object iValue,
      final OCommandContext iContext,
      final boolean binaryEvaluation) {
    if (iValue == null) return null;

    if (iValue instanceof BytesContainer) return iValue;

    if (iCurrentRecord != null) {
      iCurrentRecord = iCurrentRecord.getRecord();
      if (iCurrentRecord != null
          && ((ORecord) iCurrentRecord).getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
        try {
          iCurrentRecord = iCurrentRecord.getRecord().load();
        } catch (ORecordNotFoundException ignore) {
          return null;
        }
      }
    }

    if (binaryEvaluation
        && iValue instanceof OSQLFilterItemField
        && iCurrentRecord != null
        && !((ODocument) iCurrentRecord).isDirty()
        && !iCurrentRecord.getIdentity().isTemporary()) {
      final OBinaryField bField = ((OSQLFilterItemField) iValue).getBinaryField(iCurrentRecord);
      if (bField != null) return bField;
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

    if (OMultiValue.isMultiValue(iValue) && !Map.class.isAssignableFrom(iValue.getClass())) {
      final Iterable<?> multiValue = OMultiValue.getMultiValueIterable(iValue, false);

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

  private Object[] checkForConversion(
      final OIdentifiable o, Object l, Object r, final OCollate collate) {
    Object[] result = null;

    final Object oldL = l;
    final Object oldR = r;
    if (collate != null) {

      l = collate.transform(l);
      r = collate.transform(r);

      if (l != oldL || r != oldR)
      // CHANGED
      {
        result = new Object[] {l, r};
      }
    }

    try {
      // DEFINED OPERATOR
      if ((oldR instanceof String && oldR.equals(OSQLHelper.DEFINED))
          || (oldL instanceof String && oldL.equals(OSQLHelper.DEFINED))) {
        result = new Object[] {((OSQLFilterItemAbstract) this.left).getRoot(), r};
      } else if ((oldR instanceof String && oldR.equals(OSQLHelper.NOT_NULL))
          || (oldL instanceof String && oldL.equals(OSQLHelper.NOT_NULL))) {
        // NOT_NULL OPERATOR
        result = null;
      } else if (l != null
          && r != null
          && !l.getClass().isAssignableFrom(r.getClass())
          && !r.getClass().isAssignableFrom(l.getClass()))
      // INTEGERS
      {
        if (r instanceof Integer && !(l instanceof Number || l instanceof Collection)) {
          if (l instanceof String && ((String) l).indexOf('.') > -1) {
            result = new Object[] {new Float((String) l).intValue(), r};
          } else if (l instanceof Date) {
            result = new Object[] {((Date) l).getTime(), r};
          } else if (!(l instanceof OQueryRuntimeValueMulti)
              && !(l instanceof Collection<?>)
              && !l.getClass().isArray()
              && !(l instanceof Map)) {
            result = new Object[] {getInteger(l), r};
          }
        } else if (l instanceof Integer && !(r instanceof Number || r instanceof Collection)) {
          if (r instanceof String && ((String) r).indexOf('.') > -1) {
            result = new Object[] {l, new Float((String) r).intValue()};
          } else if (r instanceof Date) {
            result = new Object[] {l, ((Date) r).getTime()};
          } else if (!(r instanceof OQueryRuntimeValueMulti)
              && !(r instanceof Collection<?>)
              && !r.getClass().isArray()
              && !(r instanceof Map)) {
            result = new Object[] {l, getInteger(r)};
          }
        } else if (r instanceof Date && !(l instanceof Collection || l instanceof Date)) {
          // DATES
          result = new Object[] {getDate(l), r};
        } else if (l instanceof Date && !(r instanceof Collection || r instanceof Date)) {
          // DATES
          result = new Object[] {l, getDate(r)};
        } else if (r instanceof Float && !(l instanceof Float || l instanceof Collection)) {
          // FLOATS
          result = new Object[] {getFloat(l), r};
        } else if (l instanceof Float && !(r instanceof Float || r instanceof Collection)) {
          // FLOATS
          result = new Object[] {l, getFloat(r)};
        } else if (r instanceof ORID && l instanceof String && !oldL.equals(OSQLHelper.NOT_NULL)) {
          // RIDS
          result = new Object[] {new ORecordId((String) l), r};
        } else if (l instanceof ORID && r instanceof String && !oldR.equals(OSQLHelper.NOT_NULL)) {
          // RIDS
          result = new Object[] {l, new ORecordId((String) r)};
        }
      }
    } catch (Exception ignore) {
      // JUST IGNORE CONVERSION ERRORS
    }

    return result;
  }
}
