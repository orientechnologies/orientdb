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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement.STATUS;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ORecordTrackedList;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Helper class to manage documents.
 *
 * @author Luca Garulli
 */
public class ODocumentHelper {
  public static final String ATTRIBUTE_THIS    = "@this";
  public static final String ATTRIBUTE_RID     = "@rid";
  public static final String ATTRIBUTE_RID_ID  = "@rid_id";
  public static final String ATTRIBUTE_RID_POS = "@rid_pos";
  public static final String ATTRIBUTE_VERSION = "@version";
  public static final String ATTRIBUTE_CLASS   = "@class";
  public static final String ATTRIBUTE_TYPE    = "@type";
  public static final String ATTRIBUTE_SIZE    = "@size";
  public static final String ATTRIBUTE_FIELDS  = "@fields";
  public static final String ATTRIBUTE_RAW     = "@raw";

  public static interface ODbRelatedCall<T> {
    public T call();
  }

  public static interface RIDMapper {
    ORID map(ORID rid);
  }

  public static void sort(List<? extends OIdentifiable> ioResultSet, List<OPair<String, String>> iOrderCriteria,
      OCommandContext context) {
    if (ioResultSet != null)
      Collections.sort(ioResultSet, new ODocumentComparator(iOrderCriteria, context));
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET convertField(final ODocument iDocument, final String iFieldName, final Class<?> iFieldType,
      Object iValue) {
    if (iFieldType == null)
      return (RET) iValue;

    if (ORID.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof ORID) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new ORecordId((String) iValue);
      } else if (iValue instanceof ORecord) {
        return (RET) ((ORecord) iValue).getIdentity();
      }
    } else if (ORecord.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof ORID || iValue instanceof ORecord) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new ORecordId((String) iValue);
      }
    } else if (Set.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof Set)) {
        // CONVERT IT TO SET
        final Collection<?> newValue;

        if (iValue instanceof ORecordLazyList || iValue instanceof ORecordLazyMap)
          newValue = new ORecordLazySet(iDocument);
        else
          newValue = new OTrackedSet<Object>(iDocument);

        if (iValue instanceof Collection<?>) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
          return (RET) newValue;
        } else if (iValue instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
          return (RET) newValue;
        } else if (iValue instanceof String) {
          final String stringValue = (String) iValue;

          if (stringValue != null && !stringValue.isEmpty()) {
            final String[] items = stringValue.split(",");
            for (String s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
          return (RET) newValue;
        } else if (OMultiValue.isMultiValue(iValue)) {
          // GENERIC MULTI VALUE
          for (Object s : OMultiValue.getMultiValueIterable(iValue)) {
            ((Collection<Object>) newValue).add(s);
          }
          return (RET) newValue;
        }
      } else {
        return (RET) iValue;
      }
    } else if (List.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof List)) {
        // CONVERT IT TO LIST
        final Collection<?> newValue;

        if (iValue instanceof OMVRBTreeRIDSet || iValue instanceof ORecordLazyMap || iValue instanceof ORecordLazySet)
          newValue = new ORecordLazyList(iDocument);
        else
          newValue = new OTrackedList<Object>(iDocument);

        if (iValue instanceof Collection) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
          return (RET) newValue;
        } else if (iValue instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
          return (RET) newValue;
        } else if (iValue instanceof String) {
          final String stringValue = (String) iValue;

          if (stringValue != null && !stringValue.isEmpty()) {
            final String[] items = stringValue.split(",");
            for (String s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
          return (RET) newValue;
        } else if (OMultiValue.isMultiValue(iValue)) {
          // GENERIC MULTI VALUE
          for (Object s : OMultiValue.getMultiValueIterable(iValue)) {
            ((Collection<Object>) newValue).add(s);
          }
          return (RET) newValue;
        }
      } else {
        return (RET) iValue;
      }
    } else if (iValue instanceof Enum) {
      // ENUM
      if (Number.class.isAssignableFrom(iFieldType))
        iValue = ((Enum<?>) iValue).ordinal();
      else
        iValue = iValue.toString();
      if (!(iValue instanceof String) && !iFieldType.isAssignableFrom(iValue.getClass()))
        throw new IllegalArgumentException("Property '" + iFieldName + "' of type '" + iFieldType
            + "' cannot accept value of type: " + iValue.getClass());
    } else if (Date.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof String && ODatabaseRecordThreadLocal.INSTANCE.isDefined()) {
        final OStorageConfiguration config = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration();

        DateFormat formatter = config.getDateFormatInstance();

        if (((String) iValue).length() > config.dateFormat.length()) {
          // ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
          formatter = config.getDateTimeFormatInstance();
        }

        try {
          Date newValue = formatter.parse((String) iValue);
          // _fieldValues.put(iFieldName, newValue);
          return (RET) newValue;
        } catch (ParseException pe) {
          final String dateFormat = ((String) iValue).length() > config.dateFormat.length() ? config.dateTimeFormat
              : config.dateFormat;
          throw new OQueryParsingException("Error on conversion of date '" + iValue + "' using the format: " + dateFormat, pe);
        }
      }
    }

    iValue = OType.convert(iValue, iFieldType);

    return (RET) iValue;
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET getFieldValue(Object value, final String iFieldName) {
    return (RET) getFieldValue(value, iFieldName, null);
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET getFieldValue(Object value, final String iFieldName, final OCommandContext iContext) {
    if (value == null)
      return null;

    final int fieldNameLength = iFieldName.length();
    if (fieldNameLength == 0)
      return (RET) value;

    OIdentifiable currentRecord = value instanceof OIdentifiable ? (OIdentifiable) value : null;

    int beginPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    int nextSeparatorPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    boolean firstInChain = true;
    do {
      char nextSeparator = ' ';
      for (; nextSeparatorPos < fieldNameLength; ++nextSeparatorPos) {
        nextSeparator = iFieldName.charAt(nextSeparatorPos);
        if (nextSeparator == '.' || nextSeparator == '[')
          break;
      }

      final String fieldName;
      if (nextSeparatorPos < fieldNameLength)
        fieldName = iFieldName.substring(beginPos, nextSeparatorPos);
      else {
        nextSeparator = ' ';
        if (beginPos > 0)
          fieldName = iFieldName.substring(beginPos);
        else
          fieldName = iFieldName;
      }

      if (nextSeparator == '[') {
        if (fieldName != null && fieldName.length() > 0) {
          if (currentRecord != null)
            value = getIdentifiableValue(currentRecord, fieldName);
          else if (value instanceof Map<?, ?>)
            value = getMapEntry((Map<String, ?>) value, fieldName);
          else if (OMultiValue.isMultiValue(value)) {
            final HashSet<Object> temp = new HashSet<Object>();
            for (Object o : OMultiValue.getMultiValueIterable(value)) {
              if (o instanceof OIdentifiable) {
                Object r = getFieldValue(o, iFieldName);
                if (r != null)
                  OMultiValue.add(temp, r);
              }
            }
            value = temp;
          }
        }

        if (value == null)
          return null;
        else if (value instanceof OIdentifiable)
          currentRecord = (OIdentifiable) value;

        final int end = iFieldName.indexOf(']', nextSeparatorPos);
        if (end == -1)
          throw new IllegalArgumentException("Missed closed ']'");

        String indexPart = iFieldName.substring(nextSeparatorPos + 1, end);
        if (indexPart.length() == 0)
          return null;

        nextSeparatorPos = end;

        if (value instanceof OCommandContext)
          value = ((OCommandContext) value).getVariables();

        if (value instanceof OIdentifiable) {
          final ORecord record = currentRecord != null && currentRecord instanceof OIdentifiable ? ((OIdentifiable) currentRecord)
              .getRecord() : null;

          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts = OStringSerializerHelper.smartSplit(indexAsString, ',',
              OStringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final List<String> indexRanges = OStringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
            // SINGLE VALUE
            value = ((ODocument) record).field(indexAsString);
          else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = ((ODocument) record).field(OStringSerializerHelper.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final ODocument doc = (ODocument) record;

            final String[] fieldNames = doc.fieldNames();
            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo = to != null && !to.isEmpty() ? Math.min(Integer.parseInt(to), fieldNames.length - 1)
                : fieldNames.length - 1;

            final Object[] values = new Object[rangeTo - rangeFrom + 1];

            for (int i = rangeFrom; i <= rangeTo; ++i)
              values[i - rangeFrom] = doc.field(fieldNames[i]);

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue = ORecordSerializerStringAbstract.getTypeValue(indexCondition.get(1));

            if (conditionFieldValue instanceof String)
              conditionFieldValue = OStringSerializerHelper.getStringContent(conditionFieldValue);

            final Object fieldValue = getFieldValue(currentRecord, conditionFieldName);

            if (conditionFieldValue != null && fieldValue != null)
              conditionFieldValue = OType.convert(conditionFieldValue, fieldValue.getClass());

            if (fieldValue == null && !conditionFieldValue.equals("null") || fieldValue != null
                && !fieldValue.equals(conditionFieldValue))
              value = null;
          }
        } else if (value instanceof Map<?, ?>) {
          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts = OStringSerializerHelper.smartSplit(indexAsString, ',',
              OStringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final List<String> indexRanges = OStringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          final Map<String, ?> map = (Map<String, ?>) value;
          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
            // SINGLE VALUE
            value = map.get(index);
          else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = map.get(OStringSerializerHelper.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final List<String> fieldNames = new ArrayList<String>(map.keySet());
            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo = to != null && !to.isEmpty() ? Math.min(Integer.parseInt(to), fieldNames.size() - 1) : fieldNames
                .size() - 1;

            final Object[] values = new Object[rangeTo - rangeFrom + 1];

            for (int i = rangeFrom; i <= rangeTo; ++i)
              values[i - rangeFrom] = map.get(fieldNames.get(i));

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue = ORecordSerializerStringAbstract.getTypeValue(indexCondition.get(1));

            if (conditionFieldValue instanceof String)
              conditionFieldValue = OStringSerializerHelper.getStringContent(conditionFieldValue);

            final Object fieldValue = map.get(conditionFieldName);

            if (conditionFieldValue != null && fieldValue != null)
              conditionFieldValue = OType.convert(conditionFieldValue, fieldValue.getClass());

            if (fieldValue == null && !conditionFieldValue.equals("null") || fieldValue != null
                && !fieldValue.equals(conditionFieldValue))
              value = null;
          }

        } else if (OMultiValue.isMultiValue(value)) {
          // MULTI VALUE
            final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts = OStringSerializerHelper.smartSplit(indexAsString, ',');
          final List<String> indexRanges = OStringSerializerHelper.smartSplit(indexAsString, '-');
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (isFieldName(indexAsString)) {
            // SINGLE VALUE
            if (value instanceof Map<?, ?>)
              value = getMapEntry((Map<String, ?>) value, index);
            else if (Character.isDigit(indexAsString.charAt(0)))
              value = OMultiValue.getValue(value, Integer.parseInt(indexAsString));
            else
              // FILTER BY FIELD
              value = getFieldValue(value, indexAsString, iContext);

          } else if (isListOfNumbers(indexParts)) {

            // MULTI VALUES
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i)
              values[i] = OMultiValue.getValue(value, Integer.parseInt(indexParts.get(i)));
            if(indexParts.size() > 1){
              value = values;
            }else{
              value = values[0];
            }

          } else if (isListOfNumbers(indexRanges)) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo = to != null && !to.isEmpty() ? Math.min(Integer.parseInt(to), OMultiValue.getSize(value) - 1)
                : OMultiValue.getSize(value) - 1;

            final Object[] values = new Object[rangeTo - rangeFrom + 1];
            for (int i = rangeFrom; i <= rangeTo; ++i)
              values[i - rangeFrom] = OMultiValue.getValue(value, i);
            value = values;

          } else {
            // CONDITION
            OSQLPredicate pred = new OSQLPredicate(indexAsString);
            final HashSet<Object> values = new HashSet<Object>();

            for (Object v : OMultiValue.getMultiValueIterable(value)) {
              if (v instanceof OIdentifiable) {
                Object result = pred.evaluate((OIdentifiable) v, (ODocument) ((OIdentifiable) v).getRecord(), iContext);
                if (Boolean.TRUE.equals(result)) {
                  values.add(v);
                }
              }
            }

            if (values.isEmpty())
              // RETURNS NULL
              value = values;
            else if (values.size() == 1)
              // RETURNS THE SINGLE ODOCUMENT
              value = values.iterator().next();
            else
              // RETURNS THE FILTERED COLLECTION
              value = values;
          }
        }
      } else {
        if (fieldName.length() == 0) {
          // NO FIELD NAME: THIS IS THE CASE OF NOT USEFUL . AFTER A ] OR .
          beginPos = ++nextSeparatorPos;
          continue;
        }

        if (fieldName.startsWith("$"))
          value = iContext.getVariable(fieldName);
        else if (fieldName.contains("(")) {
          boolean executedMethod = false;
          if (!firstInChain && fieldName.endsWith("()")) {
            OSQLMethod method = OSQLEngine.getInstance().getMethod(fieldName.substring(0, fieldName.length() - 2));
            if (method != null) {
              value = method.execute(value, currentRecord, iContext, value, new Object[] {});
              executedMethod = true;
            }
          }
          if (!executedMethod) {
            value = evaluateFunction(value, fieldName, iContext);
          }
        } else {
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(fieldName, '=', ' ');

          if (indexCondition.size() == 2) {
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue = ORecordSerializerStringAbstract.getTypeValue(indexCondition.get(1));

            if (conditionFieldValue instanceof String)
              conditionFieldValue = OStringSerializerHelper.getStringContent(conditionFieldValue);

            value = filterItem(conditionFieldName, conditionFieldValue, value);

          } else if (currentRecord != null) {
            // GET THE LINKED OBJECT IF ANY
            value = getIdentifiableValue(currentRecord, fieldName);
            if (value != null && value instanceof ORecord && ((ORecord) value).getInternalStatus() == STATUS.NOT_LOADED)
              // RELOAD IT
              ((ORecord) value).reload();
          } else if (value instanceof Map<?, ?>)
            value = getMapEntry((Map<String, ?>) value, fieldName);
          else if (OMultiValue.isMultiValue(value)) {
            final Set<Object> values = new HashSet<Object>();
            for (Object v : OMultiValue.getMultiValueIterable(value)) {
              final Object item;

              if (v instanceof OIdentifiable)
                item = getIdentifiableValue((OIdentifiable) v, fieldName);
              else if (v instanceof Map)
                item = ((Map<?, ?>) v).get(fieldName);
              else
                item = null;

              if (item != null)
                if (item instanceof Collection<?>)
                  values.addAll((Collection<? extends Object>) item);
                else
                  values.add(item);
            }

            if (values.isEmpty())
              value = null;
            else
              value = values;
          } else
            return null;
        }
      }

      if (value instanceof OIdentifiable)
        currentRecord = (OIdentifiable) value;
      else
        currentRecord = null;

      beginPos = ++nextSeparatorPos;
      firstInChain = false;
    } while (nextSeparatorPos < fieldNameLength && value != null);

    return (RET) value;
  }

  private static boolean isFieldName(String indexAsString) {
    indexAsString = indexAsString.trim();
    if (indexAsString.startsWith("`") && indexAsString.endsWith("`")) {
      //quoted identifier
      return !indexAsString.substring(1, indexAsString.length() - 1).contains("`");
    }
    boolean firstChar = true;
    for (char c : indexAsString.toCharArray()) {
      if (isLetter(c) || (isNumber(c) && !firstChar)) {
        firstChar = false;
        continue;
      }
      return false;
    }
    return true;
  }

  private static boolean isNumber(char c) {
    return c >= '0' && c <= '9';
  }

  private static boolean isLetter(char c) {
    if (c == '$' || c == '_' || c == '@') {
      return true;
    }
    if (c >= 'a' && c <= 'z') {
      return true;
    }
    if (c >= 'A' && c <= 'Z') {
      return true;
    }

    return false;
  }

  private static boolean isListOfNumbers(List<String> list) {
    for (String s : list) {
      try {
        Integer.parseInt(s);
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return true;
  }

  protected static Object getIndexPart(final OCommandContext iContext, final String indexPart) {
    Object index = indexPart;
    if (indexPart.indexOf(',') == -1 && (indexPart.charAt(0) == '"' || indexPart.charAt(0) == '\''))
      index = OStringSerializerHelper.getStringContent(indexPart);
    else if (indexPart.charAt(0) == '$') {
      final Object ctxValue = iContext.getVariable(indexPart);
      if (ctxValue == null)
        return null;
      index = ctxValue;
    } else if (!Character.isDigit(indexPart.charAt(0)))
      // GET FROM CURRENT VALUE
      index = indexPart;
    return index;
  }

  @SuppressWarnings("unchecked")
  protected static Object filterItem(final String iConditionFieldName, final Object iConditionFieldValue, final Object iValue) {
    if (iValue instanceof OIdentifiable) {
      final ORecord rec = ((OIdentifiable) iValue).getRecord();
      if (rec instanceof ODocument) {
        final ODocument doc = (ODocument) rec;

        Object fieldValue = doc.field(iConditionFieldName);

        if (iConditionFieldValue == null)
          return fieldValue == null ? doc : null;

        fieldValue = OType.convert(fieldValue, iConditionFieldValue.getClass());
        if (fieldValue != null && fieldValue.equals(iConditionFieldValue))
          return doc;
      }
    } else if (iValue instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iValue;
      Object fieldValue = getMapEntry(map, iConditionFieldName);

      fieldValue = OType.convert(fieldValue, iConditionFieldValue.getClass());
      if (fieldValue != null && fieldValue.equals(iConditionFieldValue))
        return map;
    }
    return null;
  }

  /**
   * Retrieves the value crossing the map with the dotted notation
   *
   * @param iKey Field(s) to retrieve. If are multiple fields, then the dot must be used as separator
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Object getMapEntry(final Map<String, ?> iMap, final Object iKey) {
    if (iMap == null || iKey == null)
      return null;

    if (iKey instanceof String) {
      String iName = (String) iKey;
      int pos = iName.indexOf('.');
      if (pos > -1)
        iName = iName.substring(0, pos);

      final Object value = iMap.get(iName);
      if (value == null)
        return null;

      if (pos > -1) {
        final String restFieldName = iName.substring(pos + 1);
        if (value instanceof ODocument)
          return getFieldValue(value, restFieldName);
        else if (value instanceof Map<?, ?>)
          return getMapEntry((Map<String, ?>) value, restFieldName);
      }

      return value;
    } else
      return iMap.get(iKey);
  }

  public static Object getIdentifiableValue(final OIdentifiable iCurrent, final String iFieldName) {
    if (iFieldName == null || iFieldName.length() == 0)
      return null;

    final char begin = iFieldName.charAt(0);
    if (begin == '@') {
      // RETURN AN ATTRIBUTE
      if (iFieldName.equalsIgnoreCase(ATTRIBUTE_THIS))
        return iCurrent.getRecord();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID))
        return iCurrent.getIdentity();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_ID))
        return iCurrent.getIdentity().getClusterId();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_POS))
        return iCurrent.getIdentity().getClusterPosition();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_VERSION))
        return iCurrent.getRecord().getRecordVersion().getCounter();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_CLASS))
        return ((ODocument) iCurrent.getRecord()).getClassName();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_TYPE))
        return Orient.instance().getRecordFactoryManager().getRecordTypeName(ORecordInternal.getRecordType(iCurrent.getRecord()));
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_SIZE)) {
        final byte[] stream = iCurrent.getRecord().toStream();
        return stream != null ? stream.length : 0;
      } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_FIELDS))
        return ((ODocument) iCurrent.getRecord()).fieldNames();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RAW))
        return new String(iCurrent.getRecord().toStream());
    }

    if (iCurrent == null)
      return null;

    final ODocument doc = ((ODocument) iCurrent.getRecord());
    doc.checkForFields(iFieldName);
    ODocumentEntry entry = doc._fields.get(iFieldName);
    return entry != null ? entry.value : null;
  }

  public static Object evaluateFunction(final Object currentValue, final String iFunction, final OCommandContext iContext) {
    if (currentValue == null)
      return null;

    Object result = null;

    final String function = iFunction.toUpperCase();

    if (function.startsWith("SIZE("))
      result = currentValue instanceof ORecord ? 1 : OMultiValue.getSize(currentValue);
    else if (function.startsWith("LENGTH("))
      result = currentValue.toString().length();
    else if (function.startsWith("TOUPPERCASE("))
      result = currentValue.toString().toUpperCase();
    else if (function.startsWith("TOLOWERCASE("))
      result = currentValue.toString().toLowerCase();
    else if (function.startsWith("TRIM("))
      result = currentValue.toString().trim();
    else if (function.startsWith("TOJSON("))
      result = currentValue instanceof ODocument ? ((ODocument) currentValue).toJSON() : null;
    else if (function.startsWith("KEYS("))
      result = currentValue instanceof Map<?, ?> ? ((Map<?, ?>) currentValue).keySet() : null;
    else if (function.startsWith("VALUES("))
      result = currentValue instanceof Map<?, ?> ? ((Map<?, ?>) currentValue).values() : null;
    else if (function.startsWith("ASSTRING("))
      result = currentValue.toString();
    else if (function.startsWith("ASINTEGER("))
      result = new Integer(currentValue.toString());
    else if (function.startsWith("ASFLOAT("))
      result = new Float(currentValue.toString());
    else if (function.startsWith("ASBOOLEAN(")) {
      if (currentValue instanceof String)
        result = new Boolean((String) currentValue);
      else if (currentValue instanceof Number) {
        final int bValue = ((Number) currentValue).intValue();
        if (bValue == 0)
          result = Boolean.FALSE;
        else if (bValue == 1)
          result = Boolean.TRUE;
      }
    } else if (function.startsWith("ASDATE("))
      if (currentValue instanceof Date)
        result = currentValue;
      else if (currentValue instanceof Number)
        result = new Date(((Number) currentValue).longValue());
      else
        try {
          result = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance()
              .parse(currentValue.toString());
        } catch (ParseException e) {
        }
    else if (function.startsWith("ASDATETIME("))
      if (currentValue instanceof Date)
        result = currentValue;
      else if (currentValue instanceof Number)
        result = new Date(((Number) currentValue).longValue());
      else
        try {
          result = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateTimeFormatInstance()
              .parse(currentValue.toString());
        } catch (ParseException e) {
        }
    else {
      // EXTRACT ARGUMENTS
      final List<String> args = OStringSerializerHelper.getParameters(iFunction.substring(iFunction.indexOf('(')));

      final ORecord currentRecord = iContext != null ? (ORecord) iContext.getVariable("$current") : null;
      for (int i = 0; i < args.size(); ++i) {
        final String arg = args.get(i);
        final Object o = OSQLHelper.getValue(arg, currentRecord, iContext);
        if (o != null)
          args.set(i, o.toString());
      }

      if (function.startsWith("CHARAT("))
        result = currentValue.toString().charAt(Integer.parseInt(args.get(0)));
      else if (function.startsWith("INDEXOF("))
        if (args.size() == 1)
          result = currentValue.toString().indexOf(OStringSerializerHelper.getStringContent(args.get(0)));
        else
          result = currentValue.toString().indexOf(OStringSerializerHelper.getStringContent(args.get(0)),
              Integer.parseInt(args.get(1)));
      else if (function.startsWith("SUBSTRING("))
        if (args.size() == 1)
          result = currentValue.toString().substring(Integer.parseInt(args.get(0)));
        else
          result = currentValue.toString().substring(Integer.parseInt(args.get(0)), Integer.parseInt(args.get(1)));
      else if (function.startsWith("APPEND("))
        result = currentValue.toString() + OStringSerializerHelper.getStringContent(args.get(0));
      else if (function.startsWith("PREFIX("))
        result = OStringSerializerHelper.getStringContent(args.get(0)) + currentValue.toString();
      else if (function.startsWith("FORMAT("))
        if (currentValue instanceof Date)
          result = new SimpleDateFormat(OStringSerializerHelper.getStringContent(args.get(0))).format(currentValue);
        else
          result = String.format(OStringSerializerHelper.getStringContent(args.get(0)), currentValue.toString());
      else if (function.startsWith("LEFT(")) {
        final int len = Integer.parseInt(args.get(0));
        final String stringValue = currentValue.toString();
        result = stringValue.substring(0, len <= stringValue.length() ? len : stringValue.length());
      } else if (function.startsWith("RIGHT(")) {
        final int offset = Integer.parseInt(args.get(0));
        final String stringValue = currentValue.toString();
        result = stringValue.substring(offset < stringValue.length() ? stringValue.length() - offset : 0);
      } else {
        final OSQLFunctionRuntime f = OSQLHelper.getFunction(null, iFunction);
        if (f != null)
          result = f.execute(currentRecord, currentRecord, null, iContext);
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public static Object cloneValue(ODocument iCloned, final Object fieldValue) {

    if (fieldValue != null) {
      if (fieldValue instanceof ODocument && !((ODocument) fieldValue).getIdentity().isValid()) {
        // EMBEDDED DOCUMENT
        return ((ODocument) fieldValue).copy();

      } else if (fieldValue instanceof ORidBag) {
        return ((ORidBag) fieldValue).copy();

      } else if (fieldValue instanceof ORecordLazyList) {
        return ((ORecordLazyList) fieldValue).copy(iCloned);

      } else if (fieldValue instanceof ORecordTrackedList) {
        final ORecordTrackedList newList = new ORecordTrackedList(iCloned);
        newList.addAll((ORecordTrackedList) fieldValue);
        return newList;

      } else if (fieldValue instanceof OTrackedList<?>) {
        final OTrackedList<Object> newList = new OTrackedList<Object>(iCloned);
        newList.addAll((OTrackedList<Object>) fieldValue);
        return newList;

      } else if (fieldValue instanceof List<?>) {
        return new ArrayList<Object>((List<Object>) fieldValue);

        // SETS
      } else if (fieldValue instanceof OMVRBTreeRIDSet) {
        return ((OMVRBTreeRIDSet) fieldValue).copy(iCloned);

      } else if (fieldValue instanceof ORecordLazySet) {
        final ORecordLazySet newList = new ORecordLazySet(iCloned);
        newList.addAll((ORecordLazySet) fieldValue);
        return newList;

      } else if (fieldValue instanceof ORecordTrackedSet) {
        final ORecordTrackedSet newList = new ORecordTrackedSet(iCloned);
        newList.addAll((ORecordTrackedSet) fieldValue);
        return newList;

      } else if (fieldValue instanceof OTrackedSet<?>) {
        final OTrackedSet<Object> newList = new OTrackedSet<Object>(iCloned);
        newList.addAll((OTrackedSet<Object>) fieldValue);
        return newList;

      } else if (fieldValue instanceof Set<?>) {
        return new HashSet<Object>((Set<Object>) fieldValue);
        // MAPS
      } else if (fieldValue instanceof ORecordLazyMap) {
        final ORecordLazyMap newMap = new ORecordLazyMap(iCloned, ((ORecordLazyMap) fieldValue).getRecordType());
        newMap.putAll((ORecordLazyMap) fieldValue);
        return newMap;

      } else if (fieldValue instanceof OTrackedMap) {
        final OTrackedMap<Object> newMap = new OTrackedMap<Object>(iCloned);
        newMap.putAll((OTrackedMap<Object>) fieldValue);
        return newMap;

      } else if (fieldValue instanceof Map<?, ?>) {
        return new LinkedHashMap<String, Object>((Map<String, Object>) fieldValue);
      } else
        return fieldValue;
    }
    // else if (iCloned.getImmutableSchemaClass() != null) {
    // final OProperty prop = iCloned.getImmutableSchemaClass().getProperty(iEntry.getKey());
    // if (prop != null && prop.isMandatory())
    // return fieldValue;
    // }
    return null;
  }

  public static boolean hasSameContentItem(final Object iCurrent, ODatabaseDocumentInternal iMyDb, final Object iOther,
      final ODatabaseDocumentInternal iOtherDb, RIDMapper ridMapper) {
    if (iCurrent instanceof ODocument) {
      final ODocument current = (ODocument) iCurrent;
      if (iOther instanceof ORID) {
        if (!current.isDirty()) {
          if (!current.getIdentity().equals(iOther))
            return false;
        } else {
          final ODocument otherDoc = iOtherDb.load((ORID) iOther);
          if (!ODocumentHelper.hasSameContentOf(current, iMyDb, otherDoc, iOtherDb, ridMapper))
            return false;
        }
      } else if (!ODocumentHelper.hasSameContentOf(current, iMyDb, (ODocument) iOther, iOtherDb, ridMapper))
        return false;
    } else if (!compareScalarValues(iCurrent, iOther, ridMapper))
      return false;
    return true;
  }

  /**
   * Makes a deep comparison field by field to check if the passed ODocument instance is identical as identity and content to the
   * current one. Instead equals() just checks if the RID are the same.
   *
   * @param iOther ODocument instance
   * @return true if the two document are identical, otherwise false
   * @see #equals(Object)
   */
  @SuppressWarnings("unchecked")
  public static boolean hasSameContentOf(final ODocument iCurrent, final ODatabaseDocumentInternal iMyDb, final ODocument iOther,
      final ODatabaseDocumentInternal iOtherDb, RIDMapper ridMapper) {
    return hasSameContentOf(iCurrent, iMyDb, iOther, iOtherDb, ridMapper, true);
  }

  /**
   * Makes a deep comparison field by field to check if the passed ODocument instance is identical in the content to the current
   * one. Instead equals() just checks if the RID are the same.
   *
   * @param iOther ODocument instance
   * @return true if the two document are identical, otherwise false
   * @see #equals(Object)
   */
  @SuppressWarnings("unchecked")
  public static boolean hasSameContentOf(final ODocument iCurrent, final ODatabaseDocumentInternal iMyDb, final ODocument iOther,
      final ODatabaseDocumentInternal iOtherDb, RIDMapper ridMapper, final boolean iCheckAlsoIdentity) {
    if (iOther == null)
      return false;

    if (iCheckAlsoIdentity && !iCurrent.equals(iOther) && iCurrent.getIdentity().isValid())
      return false;

    if (iMyDb != null)
      makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
        public Object call() {
          if (iCurrent.getInternalStatus() == STATUS.NOT_LOADED)
            iCurrent.reload();
          return null;
        }
      });

    if (iOtherDb != null)
      makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
        public Object call() {
          if (iOther.getInternalStatus() == STATUS.NOT_LOADED)
            iOther.reload();
          return null;
        }
      });

    if (iMyDb != null)
      makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
        public Object call() {
          iCurrent.checkForFields();
          return null;
        }
      });
    else
      iCurrent.checkForFields();

    if (iOtherDb != null)
      makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
        public Object call() {
          iOther.checkForFields();
          return null;
        }
      });
    else
      iOther.checkForFields();

    if (iCurrent.fields() != iOther.fields())
      return false;

    // CHECK FIELD-BY-FIELD
    Object myFieldValue;
    Object otherFieldValue;
    for (Entry<String, Object> f : iCurrent) {
      myFieldValue = f.getValue();
      otherFieldValue = iOther._fields.get(f.getKey()).value;

      if (myFieldValue == otherFieldValue)
        continue;

      // CHECK FOR NULLS
      if (myFieldValue == null) {
        if (otherFieldValue != null)
          return false;
      } else if (otherFieldValue == null)
        return false;

      if (myFieldValue != null)
        if (myFieldValue instanceof Set && otherFieldValue instanceof Set) {
          if (!compareSets(iMyDb, (Set<?>) myFieldValue, iOtherDb, (Set<?>) otherFieldValue, ridMapper))
            return false;
        } else if (myFieldValue instanceof Collection && otherFieldValue instanceof Collection) {
          if (!compareCollections(iMyDb, (Collection<?>) myFieldValue, iOtherDb, (Collection<?>) otherFieldValue, ridMapper))
            return false;
        } else if (myFieldValue instanceof ORidBag && otherFieldValue instanceof ORidBag) {
          if (!compareBags(iMyDb, (ORidBag) myFieldValue, iOtherDb, (ORidBag) otherFieldValue, ridMapper))
            return false;
        } else if (myFieldValue instanceof Map && otherFieldValue instanceof Map) {
          if (!compareMaps(iMyDb, (Map<Object, Object>) myFieldValue, iOtherDb, (Map<Object, Object>) otherFieldValue, ridMapper))
            return false;
        } else if (myFieldValue instanceof ODocument && otherFieldValue instanceof ODocument) {
          if (!hasSameContentOf((ODocument) myFieldValue, iMyDb, (ODocument) otherFieldValue, iOtherDb, ridMapper))
            return false;
        } else {
          if (!compareScalarValues(myFieldValue, otherFieldValue, ridMapper))
            return false;
        }
    }

    return true;
  }

  public static boolean compareMaps(ODatabaseDocumentInternal iMyDb, Map<Object, Object> myFieldValue,
      ODatabaseDocumentInternal iOtherDb, Map<Object, Object> otherFieldValue, RIDMapper ridMapper) {
    // CHECK IF THE ORDER IS RESPECTED
    final Map<Object, Object> myMap = myFieldValue;
    final Map<Object, Object> otherMap = otherFieldValue;

    if (myMap.size() != otherMap.size())
      return false;

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    if (myMap instanceof ORecordLazyMultiValue) {
      oldMyAutoConvert = ((ORecordLazyMultiValue) myMap).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) myMap).setAutoConvertToRecord(false);
    }

    if (otherMap instanceof ORecordLazyMultiValue) {
      oldOtherAutoConvert = ((ORecordLazyMultiValue) otherMap).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) otherMap).setAutoConvertToRecord(false);
    }

    try {
      final Iterator<Entry<Object, Object>> myEntryIterator = makeDbCall(iMyDb,
          new ODbRelatedCall<Iterator<Entry<Object, Object>>>() {
            public Iterator<Entry<Object, Object>> call() {
              return myMap.entrySet().iterator();
            }
          });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myEntryIterator.hasNext();
        }
      })) {
        final Entry<Object, Object> myEntry = makeDbCall(iMyDb, new ODbRelatedCall<Entry<Object, Object>>() {
          public Entry<Object, Object> call() {
            return myEntryIterator.next();
          }
        });
        final Object myKey = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return myEntry.getKey();
          }
        });

        if (makeDbCall(iOtherDb, new ODbRelatedCall<Boolean>() {
          public Boolean call() {
            return !otherMap.containsKey(myKey);
          }
        }))
          return false;

        if (myEntry.getValue() instanceof ODocument) {
          if (!hasSameContentOf(makeDbCall(iMyDb, new ODbRelatedCall<ODocument>() {
            public ODocument call() {
              return (ODocument) myEntry.getValue();
            }
          }), iMyDb, makeDbCall(iOtherDb, new ODbRelatedCall<ODocument>() {
            public ODocument call() {
              return (ODocument) otherMap.get(myEntry.getKey());
            }
          }), iOtherDb, ridMapper))
            return false;
        } else {
          final Object myValue = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
            public Object call() {
              return myEntry.getValue();
            }
          });

          final Object otherValue = makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
            public Object call() {
              return otherMap.get(myEntry.getKey());
            }
          });

          if (!compareScalarValues(myValue, otherValue, ridMapper))
            return false;
        }
      }
      return true;
    } finally {
      if (myMap instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) myMap).setAutoConvertToRecord(oldMyAutoConvert);

      if (otherMap instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) otherMap).setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  public static boolean compareCollections(ODatabaseDocumentInternal iMyDb, Collection<?> myFieldValue,
      ODatabaseDocumentInternal iOtherDb, Collection<?> otherFieldValue, RIDMapper ridMapper) {
    final Collection<?> myCollection = myFieldValue;
    final Collection<?> otherCollection = otherFieldValue;

    if (myCollection.size() != otherCollection.size())
      return false;

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    if (myCollection instanceof ORecordLazyMultiValue) {
      oldMyAutoConvert = ((ORecordLazyMultiValue) myCollection).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) myCollection).setAutoConvertToRecord(false);
    }

    if (otherCollection instanceof ORecordLazyMultiValue) {
      oldOtherAutoConvert = ((ORecordLazyMultiValue) otherCollection).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) otherCollection).setAutoConvertToRecord(false);
    }

    try {
      final Iterator<?> myIterator = makeDbCall(iMyDb, new ODbRelatedCall<Iterator<?>>() {
        public Iterator<?> call() {
          return myCollection.iterator();
        }
      });

      final Iterator<?> otherIterator = makeDbCall(iOtherDb, new ODbRelatedCall<Iterator<?>>() {
        public Iterator<?> call() {
          return otherCollection.iterator();
        }
      });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myIterator.hasNext();
        }
      })) {
        final Object myNextVal = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return myIterator.next();
          }
        });

        final Object otherNextVal = makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return otherIterator.next();
          }
        });

        if (!hasSameContentItem(myNextVal, iMyDb, otherNextVal, iOtherDb, ridMapper))
          return false;
      }
      return true;
    } finally {
      if (myCollection instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) myCollection).setAutoConvertToRecord(oldMyAutoConvert);

      if (otherCollection instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) otherCollection).setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  public static boolean compareSets(ODatabaseDocumentInternal iMyDb, Set<?> myFieldValue, ODatabaseDocumentInternal iOtherDb,
      Set<?> otherFieldValue, RIDMapper ridMapper) {
    final Set<?> mySet = myFieldValue;
    final Set<?> otherSet = otherFieldValue;

    final int mySize = makeDbCall(iMyDb, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return mySet.size();
      }
    });

    final int otherSize = makeDbCall(iOtherDb, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return otherSet.size();
      }
    });

    if (mySize != otherSize)
      return false;

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    if (mySet instanceof ORecordLazyMultiValue) {
      oldMyAutoConvert = ((ORecordLazyMultiValue) mySet).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) mySet).setAutoConvertToRecord(false);
    }

    if (otherSet instanceof ORecordLazyMultiValue) {
      oldOtherAutoConvert = ((ORecordLazyMultiValue) otherSet).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) otherSet).setAutoConvertToRecord(false);
    }

    try {
      final Iterator<?> myIterator = makeDbCall(iMyDb, new ODbRelatedCall<Iterator<?>>() {
        public Iterator<?> call() {
          return mySet.iterator();
        }
      });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myIterator.hasNext();
        }
      })) {

        final Iterator<?> otherIterator = makeDbCall(iOtherDb, new ODbRelatedCall<Iterator<?>>() {
          public Iterator<?> call() {
            return otherSet.iterator();
          }
        });

        final Object myNextVal = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return myIterator.next();
          }
        });

        boolean found = false;
        while (!found && makeDbCall(iOtherDb, new ODbRelatedCall<Boolean>() {
          public Boolean call() {
            return otherIterator.hasNext();
          }
        })) {
          final Object otherNextVal = makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
            public Object call() {
              return otherIterator.next();
            }
          });

          found = hasSameContentItem(myNextVal, iMyDb, otherNextVal, iOtherDb, ridMapper);
        }

        if (!found)
          return false;
      }
      return true;
    } finally {
      if (mySet instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) mySet).setAutoConvertToRecord(oldMyAutoConvert);

      if (otherSet instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) otherSet).setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  public static boolean compareBags(ODatabaseDocumentInternal iMyDb, ORidBag myFieldValue, ODatabaseDocumentInternal iOtherDb,
      ORidBag otherFieldValue, RIDMapper ridMapper) {
    final ORidBag myBag = myFieldValue;
    final ORidBag otherBag = otherFieldValue;

    final int mySize = makeDbCall(iMyDb, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return myBag.size();
      }
    });

    final int otherSize = makeDbCall(iOtherDb, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return otherBag.size();
      }
    });

    if (mySize != otherSize)
      return false;

    boolean oldMyAutoConvert;
    boolean oldOtherAutoConvert;

    oldMyAutoConvert = myBag.isAutoConvertToRecord();
    myBag.setAutoConvertToRecord(false);

    oldOtherAutoConvert = otherBag.isAutoConvertToRecord();
    otherBag.setAutoConvertToRecord(false);

    final ORidBag otherBagCopy = makeDbCall(iOtherDb, new ODbRelatedCall<ORidBag>() {
      @Override
      public ORidBag call() {
        final ORidBag otherRidBag = new ORidBag();
        otherRidBag.setAutoConvertToRecord(false);

        for (OIdentifiable identifiable : otherBag)
          otherRidBag.add(identifiable);

        return otherRidBag;
      }
    });

    try {
      final Iterator<OIdentifiable> myIterator = makeDbCall(iMyDb, new ODbRelatedCall<Iterator<OIdentifiable>>() {
        public Iterator<OIdentifiable> call() {
          return myBag.iterator();
        }
      });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myIterator.hasNext();
        }
      })) {
        final OIdentifiable myIdentifiable = makeDbCall(iMyDb, new ODbRelatedCall<OIdentifiable>() {
          @Override
          public OIdentifiable call() {
            return myIterator.next();
          }
        });

        final ORID otherRid;
        if (ridMapper != null) {
          ORID convertedRid = ridMapper.map(myIdentifiable.getIdentity());
          if (convertedRid != null)
            otherRid = convertedRid;
          else
            otherRid = myIdentifiable.getIdentity();
        } else
          otherRid = myIdentifiable.getIdentity();

        makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
          @Override
          public Object call() {
            otherBagCopy.remove(otherRid);
            return null;
          }
        });

      }

      return makeDbCall(iOtherDb, new ODbRelatedCall<Boolean>() {
        @Override
        public Boolean call() {
          return otherBagCopy.isEmpty();
        }
      });
    } finally {
      myBag.setAutoConvertToRecord(oldMyAutoConvert);
      otherBag.setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  private static boolean compareScalarValues(Object myValue, Object otherValue, RIDMapper ridMapper) {
    if (myValue == null && otherValue != null || myValue != null && otherValue == null)
      return false;

    if (myValue == null)
      return true;

    if (myValue.getClass().isArray() && !otherValue.getClass().isArray() || !myValue.getClass().isArray()
        && otherValue.getClass().isArray())
      return false;

    if (myValue.getClass().isArray() && otherValue.getClass().isArray()) {
      final int myArraySize = Array.getLength(myValue);
      final int otherArraySize = Array.getLength(otherValue);

      if (myArraySize != otherArraySize)
        return false;

      for (int i = 0; i < myArraySize; i++) {
        final Object first = Array.get(myValue, i);
        final Object second = Array.get(otherValue, i);
        if (first == null && second != null || (first != null && !first.equals(second)))
          return false;
      }

      return true;
    }

    if (myValue instanceof Number && otherValue instanceof Number) {
      final Number myNumberValue = (Number) myValue;
      final Number otherNumberValue = (Number) otherValue;

      if (isInteger(myNumberValue) && isInteger(otherNumberValue))
        return myNumberValue.longValue() == otherNumberValue.longValue();
      else if (isFloat(myNumberValue) && isFloat(otherNumberValue))
        return myNumberValue.doubleValue() == otherNumberValue.doubleValue();
    }

    if (ridMapper != null && myValue instanceof ORID && otherValue instanceof ORID && ((ORID) myValue).isPersistent()) {
      ORID convertedValue = ridMapper.map((ORID) myValue);
      if (convertedValue != null)
        myValue = convertedValue;
    }

    return myValue.equals(otherValue);
  }

  private static boolean isInteger(Number value) {
    return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;

  }

  private static boolean isFloat(Number value) {
    return value instanceof Float || value instanceof Double;
  }

  public static void deleteCrossRefs(final ORID iRid, final ODocument iContent) {
    for (String fieldName : iContent.fieldNames()) {
      final Object fieldValue = iContent.field(fieldName);
      if (fieldValue != null) {
        if (fieldValue.equals(iRid)) {
          // REMOVE THE LINK
          iContent.field(fieldName, (ORID) null);
          iContent.save();
        } else if (fieldValue instanceof ODocument && ((ODocument) fieldValue).isEmbedded()) {
          // EMBEDDED DOCUMENT: GO RECURSIVELY
          deleteCrossRefs(iRid, (ODocument) fieldValue);
        } else if (OMultiValue.isMultiValue(fieldValue)) {
          // MULTI-VALUE (COLLECTION, ARRAY OR MAP), CHECK THE CONTENT
          for (final Iterator<?> it = OMultiValue.getMultiValueIterator(fieldValue); it.hasNext(); ) {
            final Object item = it.next();

            if (fieldValue.equals(iRid)) {
              // DELETE ITEM
              it.remove();
            } else if (item instanceof ODocument && ((ODocument) item).isEmbedded()) {
              // EMBEDDED DOCUMENT: GO RECURSIVELY
              deleteCrossRefs(iRid, (ODocument) item);
            }
          }
        }
      }
    }
  }

  public static <T> T makeDbCall(final ODatabaseDocumentInternal databaseRecord, final ODbRelatedCall<T> function) {
    databaseRecord.activateOnCurrentThread();
    return function.call();
  }
}
