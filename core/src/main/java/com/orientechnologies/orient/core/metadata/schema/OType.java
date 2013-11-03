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
package com.orientechnologies.orient.core.metadata.schema;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OBinary;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Generic representation of a type.<br/>
 * allowAssignmentFrom accepts any class, but Array.class means that the type accepts generic Arrays.
 * 
 * @author Luca Garulli
 * 
 */
public enum OType {
  BOOLEAN("Boolean", 0, new Class<?>[] { Boolean.class, Boolean.TYPE }, new Class<?>[] { Boolean.class, Number.class }) {
  },
  INTEGER("Integer", 1, new Class<?>[] { Integer.class, Integer.TYPE }, new Class<?>[] { Integer.class, Number.class }) {
  },
  SHORT("Short", 2, new Class<?>[] { Short.class, Short.TYPE }, new Class<?>[] { Short.class, Number.class }) {
  },
  LONG("Long", 3, new Class<?>[] { Long.class, Long.TYPE }, new Class<?>[] { Long.class, Number.class, }) {
  },
  FLOAT("Float", 4, new Class<?>[] { Float.class, Float.TYPE }, new Class<?>[] { Float.class, Number.class }) {
  },
  DOUBLE("Double", 5, new Class<?>[] { Double.class, Double.TYPE }, new Class<?>[] { Double.class, Number.class }) {
  },
  DATETIME("Datetime", 6, new Class<?>[] { Date.class }, new Class<?>[] { Date.class, Number.class }) {
  },
  STRING("String", 7, new Class<?>[] { String.class }, new Class<?>[] { String.class }) {
  },
  BINARY("Binary", 8, new Class<?>[] { byte[].class }, new Class<?>[] { byte[].class }) {
  },
  EMBEDDED("Embedded", 9, new Class<?>[] { Object.class }, new Class<?>[] { OSerializableStream.class }) {
  },
  EMBEDDEDLIST("EmbeddedList", 10, new Class<?>[] { List.class }, new Class<?>[] { List.class }) {
  },
  EMBEDDEDSET("EmbeddedSet", 11, new Class<?>[] { Set.class }, new Class<?>[] { Set.class }) {
  },
  EMBEDDEDMAP("EmbeddedMap", 12, new Class<?>[] { Map.class }, new Class<?>[] { Map.class }) {
  },
  LINK("Link", 13, new Class<?>[] { Object.class, ORecordId.class }, new Class<?>[] { ORecord.class, ORID.class }) {
  },
  LINKLIST("LinkList", 14, new Class<?>[] { List.class }, new Class<?>[] { List.class }) {
  },
  LINKSET("LinkSet", 15, new Class<?>[] { Set.class }, new Class<?>[] { Set.class }) {
  },
  LINKMAP("LinkMap", 16, new Class<?>[] { Map.class }, new Class<?>[] { Map.class }) {
  },
  BYTE("Byte", 17, new Class<?>[] { Byte.class, Byte.TYPE }, new Class<?>[] { Byte.class, Number.class }) {
  },
  TRANSIENT("Transient", 18, new Class<?>[] {}, new Class<?>[] {}) {
  },
  DATE("Date", 19, new Class<?>[] { Date.class }, new Class<?>[] { Date.class, Number.class }) {
  },
  CUSTOM("Custom", 20, new Class<?>[] { OSerializableStream.class }, new Class<?>[] { OSerializableStream.class }) {
  },
  DECIMAL("Decimal", 21, new Class<?>[] { BigDecimal.class }, new Class<?>[] { BigDecimal.class, Number.class }) {
  };

  protected static final OType[] TYPES = new OType[] { STRING, BOOLEAN, BYTE, INTEGER, SHORT, LONG, FLOAT, DOUBLE, DATETIME, DATE,
      BINARY, EMBEDDEDLIST, EMBEDDEDSET, EMBEDDEDMAP, LINK, LINKLIST, LINKSET, LINKMAP, EMBEDDED, CUSTOM, TRANSIENT, DECIMAL };

  protected String               name;
  protected int                  id;
  protected Class<?>[]           javaTypes;
  protected Class<?>[]           allowAssignmentFrom;

  private OType(final String iName, final int iId, final Class<?>[] iJavaTypes, final Class<?>[] iAllowAssignmentBy) {
    name = iName;
    id = iId;
    javaTypes = iJavaTypes;
    allowAssignmentFrom = iAllowAssignmentBy;
  }

  /**
   * Return the type by ID.
   * 
   * @param iId
   *          The id to search
   * @return The type if any, otherwise null
   */
  public static OType getById(final byte iId) {
    for (OType t : TYPES) {
      if (iId == t.id)
        return t;
    }
    return null;
  }

  /**
   * Return the correspondent type by checking the "assignability" of the class received as parameter.
   * 
   * @param iClass
   *          Class to check
   * @return OType instance if found, otherwise null
   */
  public static OType getTypeByClass(final Class<?> iClass) {
    if (iClass == null)
      return null;

    for (final OType type : TYPES)
      for (int i = 0; i < type.javaTypes.length; ++i) {
        if (type.javaTypes[i] == iClass)
          return type;
        if (type.javaTypes[i] == Array.class && iClass.isArray())
          return type;
      }

    int priority = 0;
    boolean comparedAtLeastOnce;
    do {
      comparedAtLeastOnce = false;
      for (final OType type : TYPES) {
        if (type.allowAssignmentFrom.length > priority) {
          if (type.allowAssignmentFrom[priority].isAssignableFrom(iClass))
            return type;
          if (type.allowAssignmentFrom[priority].isArray() && iClass.isArray())
            return type;
          comparedAtLeastOnce = true;
        }
      }

      priority++;
    } while (comparedAtLeastOnce);

    return null;
  }

  /**
   * Convert the input object to an integer.
   * 
   * @param iValue
   *          Any type supported
   * @return The integer value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public int asInt(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).intValue();
    else if (iValue instanceof String)
      return Integer.valueOf((String) iValue);
    else if (iValue instanceof Boolean)
      return ((Boolean) iValue) ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to int for type: " + name);
  }

  /**
   * Convert the input object to a long.
   * 
   * @param iValue
   *          Any type supported
   * @return The long value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public long asLong(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).longValue();
    else if (iValue instanceof String)
      return Long.valueOf((String) iValue);
    else if (iValue instanceof Boolean)
      return ((Boolean) iValue) ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to long for type: " + name);
  }

  /**
   * Convert the input object to a float.
   * 
   * @param iValue
   *          Any type supported
   * @return The float value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public float asFloat(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).floatValue();
    else if (iValue instanceof String)
      return Float.valueOf((String) iValue);

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to float for type: " + name);
  }

  /**
   * Convert the input object to a double.
   * 
   * @param iValue
   *          Any type supported
   * @return The double value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public double asDouble(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).doubleValue();
    else if (iValue instanceof String)
      return Double.valueOf((String) iValue);

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to double for type: " + name);
  }

  /**
   * Convert the input object to a string.
   * 
   * @param iValue
   *          Any type supported
   * @return The string if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public String asString(final Object iValue) {
    return iValue.toString();
  }

  public boolean isMultiValue() {
    return this == EMBEDDEDLIST || this == EMBEDDEDMAP || this == EMBEDDEDSET || this == LINKLIST || this == LINKMAP
        || this == LINKSET;
  }

  public boolean isLink() {
    return this == LINK || this == LINKSET || this == LINKLIST || this == LINKMAP;
  }

  public static boolean isSimpleType(final Object iObject) {
    if (iObject == null)
      return false;

    final Class<? extends Object> iType = iObject.getClass();

    if (iType.isPrimitive()
        || Number.class.isAssignableFrom(iType)
        || String.class.isAssignableFrom(iType)
        || Boolean.class.isAssignableFrom(iType)
        || Date.class.isAssignableFrom(iType)
        || (iType.isArray() && (iType.equals(byte[].class) || iType.equals(char[].class) || iType.equals(int[].class)
            || iType.equals(long[].class) || iType.equals(double[].class) || iType.equals(float[].class)
            || iType.equals(short[].class) || iType.equals(Integer[].class) || iType.equals(String[].class)
            || iType.equals(Long[].class) || iType.equals(Short[].class) || iType.equals(Double[].class))))
      return true;

    return false;
  }

  /**
   * Convert types between numbers based on the iTargetClass parameter.
   * 
   * @param iValue
   *          Value to convert
   * @param iTargetClass
   *          Expected class
   * @return The converted value or the original if no conversion was applied
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Object convert(final Object iValue, final Class<?> iTargetClass) {
    if (iValue == null)
      return null;

    if (iValue.getClass().equals(iTargetClass))
      // SAME TYPE: DON'T CONVERT IT
      return iValue;

    if (iTargetClass.isAssignableFrom(iValue.getClass()))
      // COMPATIBLE TYPES: DON'T CONVERT IT
      return iValue;

    try {
      if (iValue instanceof OBinary && iTargetClass.isAssignableFrom(byte[].class))
        return ((OBinary) iValue).toByteArray();
      else if (byte[].class.isAssignableFrom(iTargetClass)) {
        return OStringSerializerHelper.getBinaryContent(iValue);
      } else if (byte[].class.isAssignableFrom(iValue.getClass())) {
        return iValue;
      } else if (iTargetClass.isEnum()) {
        if (iValue instanceof Number)
          return ((Class<Enum>) iTargetClass).getEnumConstants()[((Number) iValue).intValue()];
        return Enum.valueOf((Class<Enum>) iTargetClass, iValue.toString());
      } else if (iTargetClass.equals(Byte.TYPE) || iTargetClass.equals(Byte.class)) {
        if (iValue instanceof Byte)
          return iValue;
        else if (iValue instanceof String)
          return Byte.parseByte((String) iValue);
        else
          return ((Number) iValue).byteValue();

      } else if (iTargetClass.equals(Short.TYPE) || iTargetClass.equals(Short.class)) {
        if (iValue instanceof Short)
          return iValue;
        else if (iValue instanceof String)
          return Short.parseShort((String) iValue);
        else
          return ((Number) iValue).shortValue();

      } else if (iTargetClass.equals(Integer.TYPE) || iTargetClass.equals(Integer.class)) {
        if (iValue instanceof Integer)
          return iValue;
        else if (iValue instanceof String)
          return Integer.parseInt((String) iValue);
        else
          return ((Number) iValue).intValue();

      } else if (iTargetClass.equals(Long.TYPE) || iTargetClass.equals(Long.class)) {
        if (iValue instanceof Long)
          return iValue;
        else if (iValue instanceof String)
          return Long.parseLong((String) iValue);
        else if (iValue instanceof Date)
          return ((Date) iValue).getTime();
        else
          return ((Number) iValue).longValue();

      } else if (iTargetClass.equals(Float.TYPE) || iTargetClass.equals(Float.class)) {
        if (iValue instanceof Float)
          return iValue;
        else if (iValue instanceof String)
          return Float.parseFloat((String) iValue);
        else
          return ((Number) iValue).floatValue();

      } else if (iTargetClass.equals(BigDecimal.class)) {
        if (iValue instanceof BigDecimal)
          return iValue;
        else if (iValue instanceof String)
          return new BigDecimal((String) iValue);
        else if (iValue instanceof Number)
          return new BigDecimal(iValue.toString());

      } else if (iTargetClass.equals(Double.TYPE) || iTargetClass.equals(Double.class)) {
        if (iValue instanceof Double)
          return iValue;
        else if (iValue instanceof String)
          return Double.parseDouble((String) iValue);
        else if (iValue instanceof Float)
          // THIS IS NECESSARY DUE TO A BUG/STRANGE BEHAVIOR OF JAVA BY LOSSING PRECISION
          return Double.parseDouble((String) iValue.toString());
        else
          return ((Number) iValue).doubleValue();

      } else if (iTargetClass.equals(Boolean.TYPE) || iTargetClass.equals(Boolean.class)) {
        if (iValue instanceof Boolean)
          return ((Boolean) iValue).booleanValue();
        else if (iValue instanceof String) {
          if (((String) iValue).equalsIgnoreCase("true"))
            return Boolean.TRUE;
          else if (((String) iValue).equalsIgnoreCase("false"))
            return Boolean.FALSE;
          throw new IllegalArgumentException("Value is not boolean. Expected true or false but received '" + iValue + "'");
        } else if (iValue instanceof Number)
          return ((Number) iValue).intValue() != 0;

      } else if (iValue instanceof Collection<?> && !(iValue instanceof Set<?>) && Set.class.isAssignableFrom(iTargetClass)) {
        final Set<Object> set = new HashSet<Object>();
        set.addAll((Collection<? extends Object>) iValue);
        return set;

      } else if (!(iValue instanceof Collection<?>) && Collection.class.isAssignableFrom(iTargetClass)) {
        final Set<Object> set = new HashSet<Object>();
        set.add(iValue);
        return set;

      } else if (iTargetClass.equals(Date.class)) {
        if (iValue instanceof Number)
          return new Date(((Number) iValue).longValue());
        if (iValue instanceof String) {
          try {
            return ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateTimeFormatInstance()
                .parse((String) iValue);
          } catch (ParseException e) {
            return ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance()
                .parse((String) iValue);
          }
        }
      } else if (iTargetClass.equals(String.class))
        return iValue.toString();
    } catch (IllegalArgumentException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      OLogManager.instance().debug(OType.class, "Error in conversion of value '%s' to type '%s'", iValue, iTargetClass);
      return null;
    }

    return iValue;
  }

  public Class<?> getDefaultJavaType() {
    return javaTypes.length > 0 ? javaTypes[0] : null;
  }

  public Class<?>[] getJavaTypes() {
    return javaTypes;
  }

  public static Number increment(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot increment a null value");

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() + b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return new Long(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Long)
        return new Long(a.intValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.intValue() + b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return new Long(a.intValue() + b.shortValue());
        return sum;
      } else if (b instanceof Float)
        return new Float(a.intValue() + b.floatValue());
      else if (b instanceof Double)
        return new Double(a.intValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.intValue()).add((BigDecimal) b);

    } else if (a instanceof Long) {
      if (b instanceof Integer)
        return new Long(a.longValue() + b.intValue());
      else if (b instanceof Long)
        return new Long(a.longValue() + b.longValue());
      else if (b instanceof Short)
        return new Long(a.longValue() + b.shortValue());
      else if (b instanceof Float)
        return new Float(a.longValue() + b.floatValue());
      else if (b instanceof Double)
        return new Double(a.longValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.longValue()).add((BigDecimal) b);

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() + b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return new Long(a.shortValue() + b.intValue());
        return sum;
      } else if (b instanceof Long)
        return new Long(a.shortValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.shortValue() + b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO INTEGER
          return new Integer(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Float)
        return new Float(a.shortValue() + b.floatValue());
      else if (b instanceof Double)
        return new Double(a.shortValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.shortValue()).add((BigDecimal) b);

    } else if (a instanceof Float) {
      if (b instanceof Integer)
        return new Float(a.floatValue() + b.intValue());
      else if (b instanceof Long)
        return new Float(a.floatValue() + b.longValue());
      else if (b instanceof Short)
        return new Float(a.floatValue() + b.shortValue());
      else if (b instanceof Float)
        return new Float(a.floatValue() + b.floatValue());
      else if (b instanceof Double)
        return new Double(a.floatValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.floatValue()).add((BigDecimal) b);

    } else if (a instanceof Double) {
      if (b instanceof Integer)
        return new Double(a.doubleValue() + b.intValue());
      else if (b instanceof Long)
        return new Double(a.doubleValue() + b.longValue());
      else if (b instanceof Short)
        return new Double(a.doubleValue() + b.shortValue());
      else if (b instanceof Float)
        return new Double(a.doubleValue() + b.floatValue());
      else if (b instanceof Double)
        return new Double(a.doubleValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.doubleValue()).add((BigDecimal) b);

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer)
        return ((BigDecimal) a).add(new BigDecimal(b.intValue()));
      else if (b instanceof Long)
        return ((BigDecimal) a).add(new BigDecimal(b.longValue()));
      else if (b instanceof Short)
        return ((BigDecimal) a).add(new BigDecimal(b.shortValue()));
      else if (b instanceof Float)
        return ((BigDecimal) a).add(new BigDecimal(b.floatValue()));
      else if (b instanceof Double)
        return ((BigDecimal) a).add(new BigDecimal(b.doubleValue()));
      else if (b instanceof BigDecimal)
        return ((BigDecimal) a).add((BigDecimal) b);

    }

    throw new IllegalArgumentException("Cannot increment value '" + a + "' (" + a.getClass() + ") with '" + b + "' ("
        + b.getClass() + ")");
  }

  public static Number[] castComparableNumber(Number context, Number max) {
    // CHECK FOR CONVERSION
    if (context instanceof Integer) {
      // SHORT
      if (max instanceof Integer)
        context = context.intValue();
      else if (max instanceof Long)
        context = context.longValue();
      else if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.intValue());

    } else if (context instanceof Integer) {
      // INTEGER
      if (max instanceof Long)
        context = context.longValue();
      else if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.intValue());
      else if (max instanceof Short)
        max = max.intValue();

    } else if (context instanceof Long) {
      // LONG
      if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.longValue());
      else if (max instanceof Integer || max instanceof Short)
        max = max.longValue();

    } else if (context instanceof Float) {
      // FLOAT
      if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.floatValue());
      else if (max instanceof Short || max instanceof Integer || max instanceof Long)
        max = max.floatValue();

    } else if (context instanceof Double) {
      // DOUBLE
      if (max instanceof BigDecimal)
        context = new BigDecimal(context.doubleValue());
      else if (max instanceof Short || max instanceof Integer || max instanceof Long || max instanceof Float)
        max = max.doubleValue();

    } else if (context instanceof BigDecimal) {
      // DOUBLE
      if (max instanceof Integer)
        max = new BigDecimal((Integer) max);
      else if (max instanceof Float)
        max = new BigDecimal((Float) max);
      else if (max instanceof Double)
        max = new BigDecimal((Double) max);
      else if (max instanceof Short)
        max = new BigDecimal((Short) max);
    }

    return new Number[] { context, max };
  }
}
