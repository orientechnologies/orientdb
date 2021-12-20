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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OBinary;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generic representation of a type.<br>
 * allowAssignmentFrom accepts any class, but Array.class means that the type accepts generic
 * Arrays.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public enum OType {
  BOOLEAN("Boolean", 0, Boolean.class, new Class<?>[] {Number.class}),

  INTEGER("Integer", 1, Integer.class, new Class<?>[] {Number.class}),

  SHORT("Short", 2, Short.class, new Class<?>[] {Number.class}),

  LONG(
      "Long",
      3,
      Long.class,
      new Class<?>[] {
        Number.class,
      }),

  FLOAT("Float", 4, Float.class, new Class<?>[] {Number.class}),

  DOUBLE("Double", 5, Double.class, new Class<?>[] {Number.class}),

  DATETIME("Datetime", 6, Date.class, new Class<?>[] {Date.class, Number.class}),

  STRING("String", 7, String.class, new Class<?>[] {Enum.class}),

  BINARY("Binary", 8, byte[].class, new Class<?>[] {byte[].class}),

  EMBEDDED(
      "Embedded",
      9,
      Object.class,
      new Class<?>[] {ODocumentSerializable.class, OSerializableStream.class}),

  EMBEDDEDLIST(
      "EmbeddedList", 10, List.class, new Class<?>[] {List.class, OMultiCollectionIterator.class}),

  EMBEDDEDSET("EmbeddedSet", 11, Set.class, new Class<?>[] {Set.class}),

  EMBEDDEDMAP("EmbeddedMap", 12, Map.class, new Class<?>[] {Map.class}),

  LINK("Link", 13, OIdentifiable.class, new Class<?>[] {OIdentifiable.class, ORID.class}),

  LINKLIST("LinkList", 14, List.class, new Class<?>[] {List.class}),

  LINKSET("LinkSet", 15, Set.class, new Class<?>[] {Set.class}),

  LINKMAP("LinkMap", 16, Map.class, new Class<?>[] {Map.class}),

  BYTE("Byte", 17, Byte.class, new Class<?>[] {Number.class}),

  TRANSIENT("Transient", 18, null, new Class<?>[] {}),

  DATE("Date", 19, Date.class, new Class<?>[] {Number.class}),

  CUSTOM(
      "Custom",
      20,
      OSerializableStream.class,
      new Class<?>[] {OSerializableStream.class, Serializable.class}),

  DECIMAL("Decimal", 21, BigDecimal.class, new Class<?>[] {BigDecimal.class, Number.class}),

  LINKBAG("LinkBag", 22, ORidBag.class, new Class<?>[] {ORidBag.class}),

  ANY("Any", 23, null, new Class<?>[] {});

  // Don't change the order, the type discover get broken if you change the order.
  protected static final OType[] TYPES =
      new OType[] {
        EMBEDDEDLIST, EMBEDDEDSET, EMBEDDEDMAP, LINK, CUSTOM, EMBEDDED, STRING, DATETIME
      };

  protected static final OType[] TYPES_BY_ID = new OType[24];
  // Values previosly stored in javaTypes
  protected static final Map<Class<?>, OType> TYPES_BY_CLASS = new HashMap<Class<?>, OType>();

  static {
    for (OType oType : values()) {
      TYPES_BY_ID[oType.id] = oType;
    }
    // This is made by hand because not all types should be add.
    TYPES_BY_CLASS.put(Boolean.class, BOOLEAN);
    TYPES_BY_CLASS.put(Boolean.TYPE, BOOLEAN);
    TYPES_BY_CLASS.put(Integer.TYPE, INTEGER);
    TYPES_BY_CLASS.put(Integer.class, INTEGER);
    TYPES_BY_CLASS.put(BigInteger.class, INTEGER);
    TYPES_BY_CLASS.put(Short.class, SHORT);
    TYPES_BY_CLASS.put(Short.TYPE, SHORT);
    TYPES_BY_CLASS.put(Long.class, LONG);
    TYPES_BY_CLASS.put(Long.TYPE, LONG);
    TYPES_BY_CLASS.put(Float.TYPE, FLOAT);
    TYPES_BY_CLASS.put(Float.class, FLOAT);
    TYPES_BY_CLASS.put(Double.TYPE, DOUBLE);
    TYPES_BY_CLASS.put(Double.class, DOUBLE);
    TYPES_BY_CLASS.put(Date.class, DATETIME);
    TYPES_BY_CLASS.put(String.class, STRING);
    TYPES_BY_CLASS.put(Enum.class, STRING);
    TYPES_BY_CLASS.put(byte[].class, BINARY);
    TYPES_BY_CLASS.put(Byte.class, BYTE);
    TYPES_BY_CLASS.put(Byte.TYPE, BYTE);
    TYPES_BY_CLASS.put(Character.class, STRING);
    TYPES_BY_CLASS.put(Character.TYPE, STRING);
    TYPES_BY_CLASS.put(ORecordId.class, LINK);
    TYPES_BY_CLASS.put(BigDecimal.class, DECIMAL);
    TYPES_BY_CLASS.put(ORidBag.class, LINKBAG);
    TYPES_BY_CLASS.put(OTrackedSet.class, EMBEDDEDSET);
    TYPES_BY_CLASS.put(ORecordLazySet.class, LINKSET);
    TYPES_BY_CLASS.put(OTrackedList.class, EMBEDDEDLIST);
    TYPES_BY_CLASS.put(ORecordLazyList.class, LINKLIST);
    TYPES_BY_CLASS.put(OTrackedMap.class, EMBEDDEDMAP);
    TYPES_BY_CLASS.put(ORecordLazyMap.class, LINKMAP);
    BYTE.castable.add(BOOLEAN);
    SHORT.castable.addAll(Arrays.asList(new OType[] {BOOLEAN, BYTE}));
    INTEGER.castable.addAll(Arrays.asList(new OType[] {BOOLEAN, BYTE, SHORT}));
    LONG.castable.addAll(Arrays.asList(new OType[] {BOOLEAN, BYTE, SHORT, INTEGER}));
    FLOAT.castable.addAll(Arrays.asList(new OType[] {BOOLEAN, BYTE, SHORT, INTEGER}));
    DOUBLE.castable.addAll(Arrays.asList(new OType[] {BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT}));
    DECIMAL.castable.addAll(
        Arrays.asList(new OType[] {BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE}));
    LINKLIST.castable.add(LINKSET);
    EMBEDDEDLIST.castable.add(EMBEDDEDSET);
  }

  protected final String name;
  protected final int id;
  protected final Class<?> javaDefaultType;
  protected final Class<?>[] allowAssignmentFrom;
  protected final Set<OType> castable;

  private OType(
      final String iName,
      final int iId,
      final Class<?> iJavaDefaultType,
      final Class<?>[] iAllowAssignmentBy) {
    name = iName;
    id = iId;
    javaDefaultType = iJavaDefaultType;
    allowAssignmentFrom = iAllowAssignmentBy;
    castable = new HashSet<OType>();
    castable.add(this);
  }

  /**
   * Return the type by ID.
   *
   * @param iId The id to search
   * @return The type if any, otherwise null
   */
  public static OType getById(final byte iId) {
    if (iId >= 0 && iId < TYPES_BY_ID.length) return TYPES_BY_ID[iId];
    OLogManager.instance().warn(OType.class, "Invalid type index: " + iId, (Object[]) null);
    return null;
  }

  /**
   * Get the identifier of the type. use this instead of {@link Enum#ordinal()} for guarantee a
   * cross code version identifier.
   *
   * @return the identifier of the type.
   */
  public final int getId() {
    return id;
  }

  /**
   * Return the correspondent type by checking the "assignability" of the class received as
   * parameter.
   *
   * @param iClass Class to check
   * @return OType instance if found, otherwise null
   */
  public static OType getTypeByClass(final Class<?> iClass) {
    if (iClass == null) return null;

    OType type = TYPES_BY_CLASS.get(iClass);
    if (type != null) return type;
    type = getTypeByClassInherit(iClass);

    return type;
  }

  private static OType getTypeByClassInherit(final Class<?> iClass) {
    if (iClass.isArray()) return EMBEDDEDLIST;
    int priority = 0;
    boolean comparedAtLeastOnce;
    do {
      comparedAtLeastOnce = false;
      for (final OType type : TYPES) {
        if (type.allowAssignmentFrom.length > priority) {
          if (type.allowAssignmentFrom[priority].isAssignableFrom(iClass)) return type;
          comparedAtLeastOnce = true;
        }
      }

      priority++;
    } while (comparedAtLeastOnce);
    return null;
  }

  public static OType getTypeByValue(Object value) {
    if (value == null) return null;
    Class<?> clazz = value.getClass();
    OType type = TYPES_BY_CLASS.get(clazz);
    if (type != null) return type;

    OType byType = getTypeByClassInherit(clazz);
    if (LINK == byType) {
      if (value instanceof ODocument && ((ODocument) value).isEmbedded()) return EMBEDDED;
    } else if (EMBEDDEDSET == byType) {
      if (checkLinkCollection(((Collection<?>) value))) return LINKSET;
    } else if (EMBEDDEDLIST == byType && !clazz.isArray()) {
      if (value instanceof OMultiCollectionIterator<?>)
        type =
            ((OMultiCollectionIterator<?>) value).isEmbedded()
                ? OType.EMBEDDEDLIST
                : OType.LINKLIST;
      else if (checkLinkCollection(((Collection<?>) value))) return LINKLIST;

    } else if (EMBEDDEDMAP == byType) {
      if (checkLinkCollection(((Map<?, ?>) value).values())) return LINKMAP;
    }
    return byType;
  }

  private static boolean checkLinkCollection(Collection<?> toCheck) {
    boolean empty = true;
    for (Object object : toCheck) {
      if (object != null
          && (!(object instanceof OIdentifiable)
              || (object instanceof ODocument && ((ODocument) object).isEmbedded()))) return false;
      else if (object != null) empty = false;
    }
    if (!empty) return true;
    return false;
  }

  public static boolean isSimpleType(final Object iObject) {
    if (iObject == null) return false;

    final Class<? extends Object> iType = iObject.getClass();

    if (iType.isPrimitive()
        || Number.class.isAssignableFrom(iType)
        || String.class.isAssignableFrom(iType)
        || Boolean.class.isAssignableFrom(iType)
        || Date.class.isAssignableFrom(iType)
        || (iType.isArray()
            && (iType.equals(byte[].class)
                || iType.equals(char[].class)
                || iType.equals(int[].class)
                || iType.equals(long[].class)
                || iType.equals(double[].class)
                || iType.equals(float[].class)
                || iType.equals(short[].class)
                || iType.equals(Integer[].class)
                || iType.equals(String[].class)
                || iType.equals(Long[].class)
                || iType.equals(Short[].class)
                || iType.equals(Double[].class)))) return true;

    return false;
  }

  /**
   * Convert types based on the iTargetClass parameter.
   *
   * @param iValue Value to convert
   * @param iTargetClass Expected class
   * @return The converted value or the original if no conversion was applied
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Object convert(Object iValue, final Class<?> iTargetClass) {
    if (iValue == null) return null;

    if (iTargetClass == null) return iValue;

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
        if (iValue instanceof Byte) return iValue;
        else if (iValue instanceof String) return Byte.parseByte((String) iValue);
        else return ((Number) iValue).byteValue();

      } else if (iTargetClass.equals(Short.TYPE) || iTargetClass.equals(Short.class)) {
        if (iValue instanceof Short) return iValue;
        else if (iValue instanceof String) return Short.parseShort((String) iValue);
        else return ((Number) iValue).shortValue();

      } else if (iTargetClass.equals(Integer.TYPE) || iTargetClass.equals(Integer.class)) {
        if (iValue instanceof Integer) return iValue;
        else if (iValue instanceof String) {
          if (iValue.toString().equals("")) {
            return null;
          }
          return Integer.parseInt((String) iValue);
        } else return ((Number) iValue).intValue();

      } else if (iTargetClass.equals(Long.TYPE) || iTargetClass.equals(Long.class)) {
        if (iValue instanceof Long) return iValue;
        else if (iValue instanceof String) return Long.parseLong((String) iValue);
        else if (iValue instanceof Date) return ((Date) iValue).getTime();
        else return ((Number) iValue).longValue();

      } else if (iTargetClass.equals(Float.TYPE) || iTargetClass.equals(Float.class)) {
        if (iValue instanceof Float) return iValue;
        else if (iValue instanceof String) return Float.parseFloat((String) iValue);
        else return ((Number) iValue).floatValue();

      } else if (iTargetClass.equals(BigDecimal.class)) {
        if (iValue instanceof String) return new BigDecimal((String) iValue);
        else if (iValue instanceof Number) return new BigDecimal(iValue.toString());

      } else if (iTargetClass.equals(Double.TYPE) || iTargetClass.equals(Double.class)) {
        if (iValue instanceof Double) return iValue;
        else if (iValue instanceof String) return Double.parseDouble((String) iValue);
        else if (iValue instanceof Float)
          // THIS IS NECESSARY DUE TO A BUG/STRANGE BEHAVIOR OF JAVA BY LOSSING PRECISION
          return Double.parseDouble(iValue.toString());
        else return ((Number) iValue).doubleValue();

      } else if (iTargetClass.equals(Boolean.TYPE) || iTargetClass.equals(Boolean.class)) {
        if (iValue instanceof Boolean) return iValue;
        else if (iValue instanceof String) {
          if (((String) iValue).equalsIgnoreCase("true")) return Boolean.TRUE;
          else if (((String) iValue).equalsIgnoreCase("false")) return Boolean.FALSE;
          throw new IllegalArgumentException(
              "Value is not boolean. Expected true or false but received '" + iValue + "'");
        } else if (iValue instanceof Number) return ((Number) iValue).intValue() != 0;

      } else if (Set.class.isAssignableFrom(iTargetClass)) {
        // The caller specifically wants a Set.  If the value is a collection
        // we will add all of the items in the collection to a set.  Otherwise
        // we will create a singleton set with only the value in it.
        if (iValue instanceof Collection<?>) {
          final Set<Object> set = new HashSet<Object>();
          set.addAll((Collection<? extends Object>) iValue);
          return set;
        } else {
          return Collections.singleton(iValue);
        }

      } else if (List.class.isAssignableFrom(iTargetClass)) {
        // The caller specifically wants a List.  If the value is a collection
        // we will add all of the items in the collection to a List.  Otherwise
        // we will create a singleton List with only the value in it.
        if (iValue instanceof Collection<?>) {
          final List<Object> list = new ArrayList<Object>();
          list.addAll((Collection<? extends Object>) iValue);
          return list;
        } else {
          return Collections.singletonList(iValue);
        }

      } else if (Collection.class.equals(iTargetClass)) {
        // The caller specifically wants a Collection of any type.
        // we will return a list if the value is a collection or
        // a singleton set if the value is not a collection.
        if (iValue instanceof Collection<?>) {
          final List<Object> set = new ArrayList<Object>();
          set.addAll((Collection<? extends Object>) iValue);
          return set;
        } else {
          return Collections.singleton(iValue);
        }

      } else if (iTargetClass.equals(Date.class)) {
        if (iValue instanceof Number) return new Date(((Number) iValue).longValue());
        if (iValue instanceof String) {
          if (OIOUtils.isLong(iValue.toString()))
            return new Date(Long.parseLong(iValue.toString()));
          try {
            return ODateHelper.getDateTimeFormatInstance(
                    ODatabaseRecordThreadLocal.instance().get())
                .parse((String) iValue);
          } catch (ParseException ignore) {
            return ODateHelper.getDateFormatInstance(ODatabaseRecordThreadLocal.instance().get())
                .parse((String) iValue);
          }
        }
      } else if (iTargetClass.equals(String.class)) {
        if (iValue instanceof Collection
            && ((Collection) iValue).size() == 1
            && ((Collection) iValue).iterator().next() instanceof String) {
          return ((Collection) iValue).iterator().next();
        }
        return iValue.toString();
      } else if (OIdentifiable.class.isAssignableFrom(iTargetClass)) {
        if (OMultiValue.isMultiValue(iValue)) {
          List<OIdentifiable> result = new ArrayList<OIdentifiable>();
          for (Object o : OMultiValue.getMultiValueIterable(iValue)) {
            if (o instanceof OIdentifiable) {
              result.add((OIdentifiable) o);
            } else if (o instanceof String) {
              try {
                result.add(new ORecordId(iValue.toString()));
              } catch (Exception e) {
                OLogManager.instance()
                    .debug(
                        OType.class,
                        "Error in conversion of value '%s' to type '%s'",
                        e,
                        iValue,
                        iTargetClass);
              }
            }
          }
          return result;
        } else if (iValue instanceof String) {
          try {
            return new ORecordId((String) iValue);
          } catch (Exception e) {
            OLogManager.instance()
                .debug(
                    OType.class,
                    "Error in conversion of value '%s' to type '%s'",
                    e,
                    iValue,
                    iTargetClass);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      if (iValue instanceof Collection
          && ((Collection) iValue).size() == 1
          && !Collection.class.isAssignableFrom(iTargetClass)) {
        // this must be a comparison with the result of a subquery, try to unbox the collection
        return convert(((Collection) iValue).iterator().next(), iTargetClass);
      } else if (iValue instanceof OResult
          && ((OResult) iValue).getPropertyNames().size() == 1
          && !OResult.class.isAssignableFrom(iTargetClass)) {
        // try to unbox OResult with a single property, for subqueries
        return convert(
            ((OResult) iValue).getProperty(((OResult) iValue).getPropertyNames().iterator().next()),
            iTargetClass);
      } else if (iValue instanceof OElement
          && ((OElement) iValue).getPropertyNames().size() == 1
          && !OElement.class.isAssignableFrom(iTargetClass)) {
        // try to unbox OResult with a single property, for subqueries
        return convert(
            ((OElement) iValue)
                .getProperty(((OElement) iValue).getPropertyNames().iterator().next()),
            iTargetClass);
      }

      OLogManager.instance()
          .debug(
              OType.class,
              "Error in conversion of value '%s' to type '%s'",
              e,
              iValue,
              iTargetClass);
      return null;
    }

    return iValue;
  }

  public static Number increment(final Number a, final Number b) {
    if (a == null || b == null) throw new IllegalArgumentException("Cannot increment a null value");

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() + b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return new Long(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Long) return new Long(a.intValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.intValue() + b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return new Long(a.intValue() + b.shortValue());
        return sum;
      } else if (b instanceof Float) return new Float(a.intValue() + b.floatValue());
      else if (b instanceof Double) return new Double(a.intValue() + b.doubleValue());
      else if (b instanceof BigDecimal) return new BigDecimal(a.intValue()).add((BigDecimal) b);

    } else if (a instanceof Long) {
      if (b instanceof Integer) return new Long(a.longValue() + b.intValue());
      else if (b instanceof Long) return new Long(a.longValue() + b.longValue());
      else if (b instanceof Short) return new Long(a.longValue() + b.shortValue());
      else if (b instanceof Float) return new Float(a.longValue() + b.floatValue());
      else if (b instanceof Double) return new Double(a.longValue() + b.doubleValue());
      else if (b instanceof BigDecimal) return new BigDecimal(a.longValue()).add((BigDecimal) b);

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() + b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return new Long(a.shortValue() + b.intValue());
        return sum;
      } else if (b instanceof Long) return new Long(a.shortValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.shortValue() + b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO INTEGER
          return new Integer(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Float) return new Float(a.shortValue() + b.floatValue());
      else if (b instanceof Double) return new Double(a.shortValue() + b.doubleValue());
      else if (b instanceof BigDecimal) return new BigDecimal(a.shortValue()).add((BigDecimal) b);

    } else if (a instanceof Float) {
      if (b instanceof Integer) return new Float(a.floatValue() + b.intValue());
      else if (b instanceof Long) return new Float(a.floatValue() + b.longValue());
      else if (b instanceof Short) return new Float(a.floatValue() + b.shortValue());
      else if (b instanceof Float) return new Float(a.floatValue() + b.floatValue());
      else if (b instanceof Double) return new Double(a.floatValue() + b.doubleValue());
      else if (b instanceof BigDecimal) return new BigDecimal(a.floatValue()).add((BigDecimal) b);

    } else if (a instanceof Double) {
      if (b instanceof Integer) return new Double(a.doubleValue() + b.intValue());
      else if (b instanceof Long) return new Double(a.doubleValue() + b.longValue());
      else if (b instanceof Short) return new Double(a.doubleValue() + b.shortValue());
      else if (b instanceof Float) return new Double(a.doubleValue() + b.floatValue());
      else if (b instanceof Double) return new Double(a.doubleValue() + b.doubleValue());
      else if (b instanceof BigDecimal) return new BigDecimal(a.doubleValue()).add((BigDecimal) b);

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer) return ((BigDecimal) a).add(new BigDecimal(b.intValue()));
      else if (b instanceof Long) return ((BigDecimal) a).add(new BigDecimal(b.longValue()));
      else if (b instanceof Short) return ((BigDecimal) a).add(new BigDecimal(b.shortValue()));
      else if (b instanceof Float) return ((BigDecimal) a).add(new BigDecimal(b.floatValue()));
      else if (b instanceof Double) return ((BigDecimal) a).add(new BigDecimal(b.doubleValue()));
      else if (b instanceof BigDecimal) return ((BigDecimal) a).add((BigDecimal) b);
    }

    throw new IllegalArgumentException(
        "Cannot increment value '"
            + a
            + "' ("
            + a.getClass()
            + ") with '"
            + b
            + "' ("
            + b.getClass()
            + ")");
  }

  public static Number[] castComparableNumber(Number context, Number max) {
    // CHECK FOR CONVERSION
    if (context instanceof Short) {
      // SHORT
      if (max instanceof Integer) context = context.intValue();
      else if (max instanceof Long) context = context.longValue();
      else if (max instanceof Float) context = context.floatValue();
      else if (max instanceof Double) context = context.doubleValue();
      else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.intValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
        max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
      } else if (max instanceof Byte) context = context.byteValue();

    } else if (context instanceof Integer) {
      // INTEGER
      if (max instanceof Long) context = context.longValue();
      else if (max instanceof Float) context = context.floatValue();
      else if (max instanceof Double) context = context.doubleValue();
      else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.intValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
        max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
      } else if (max instanceof Short) max = max.intValue();
      else if (max instanceof Byte) max = max.intValue();

    } else if (context instanceof Long) {
      // LONG
      if (max instanceof Float) context = context.floatValue();
      else if (max instanceof Double) context = context.doubleValue();
      else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.longValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
        max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
      } else if (max instanceof Integer || max instanceof Byte || max instanceof Short)
        max = max.longValue();

    } else if (context instanceof Float) {
      // FLOAT
      if (max instanceof Double) context = context.doubleValue();
      else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.floatValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
        max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
      } else if (max instanceof Byte
          || max instanceof Short
          || max instanceof Integer
          || max instanceof Long) max = max.floatValue();

    } else if (context instanceof Double) {
      // DOUBLE
      if (max instanceof BigDecimal) {
        context = new BigDecimal(context.doubleValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
        max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
      } else if (max instanceof Byte
          || max instanceof Short
          || max instanceof Integer
          || max instanceof Long
          || max instanceof Float) max = max.doubleValue();

    } else if (context instanceof BigDecimal) {
      // DOUBLE
      if (max instanceof Integer) max = new BigDecimal((Integer) max);
      else if (max instanceof Float) max = new BigDecimal((Float) max);
      else if (max instanceof Double) max = new BigDecimal((Double) max);
      else if (max instanceof Short) max = new BigDecimal((Short) max);
      else if (max instanceof Byte) {
        max = new BigDecimal((Byte) max);
      }

      int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
      context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
      max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
    } else if (context instanceof Byte) {
      if (max instanceof Short) context = context.shortValue();
      else if (max instanceof Integer) context = context.intValue();
      else if (max instanceof Long) context = context.longValue();
      else if (max instanceof Float) context = context.floatValue();
      else if (max instanceof Double) context = context.doubleValue();
      else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.intValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, BigDecimal.ROUND_DOWN);
        max = ((BigDecimal) max).setScale(maxScale, BigDecimal.ROUND_DOWN);
      }
    }

    return new Number[] {context, max};
  }

  /**
   * Convert the input object to an integer.
   *
   * @param iValue Any type supported
   * @return The integer value if the conversion succeed, otherwise the IllegalArgumentException
   *     exception
   */
  public int asInt(final Object iValue) {
    if (iValue instanceof Number) return ((Number) iValue).intValue();
    else if (iValue instanceof String) return Integer.valueOf((String) iValue);
    else if (iValue instanceof Boolean) return ((Boolean) iValue) ? 1 : 0;

    throw new IllegalArgumentException(
        "Cannot convert value " + iValue + " to int for type: " + name);
  }

  /**
   * Convert the input object to a long.
   *
   * @param iValue Any type supported
   * @return The long value if the conversion succeed, otherwise the IllegalArgumentException
   *     exception
   */
  public long asLong(final Object iValue) {
    if (iValue instanceof Number) return ((Number) iValue).longValue();
    else if (iValue instanceof String) return Long.valueOf((String) iValue);
    else if (iValue instanceof Boolean) return ((Boolean) iValue) ? 1 : 0;

    throw new IllegalArgumentException(
        "Cannot convert value " + iValue + " to long for type: " + name);
  }

  /**
   * Convert the input object to a float.
   *
   * @param iValue Any type supported
   * @return The float value if the conversion succeed, otherwise the IllegalArgumentException
   *     exception
   */
  public float asFloat(final Object iValue) {
    if (iValue instanceof Number) return ((Number) iValue).floatValue();
    else if (iValue instanceof String) return Float.valueOf((String) iValue);

    throw new IllegalArgumentException(
        "Cannot convert value " + iValue + " to float for type: " + name);
  }

  /**
   * Convert the input object to a double.
   *
   * @param iValue Any type supported
   * @return The double value if the conversion succeed, otherwise the IllegalArgumentException
   *     exception
   */
  public double asDouble(final Object iValue) {
    if (iValue instanceof Number) return ((Number) iValue).doubleValue();
    else if (iValue instanceof String) return Double.valueOf((String) iValue);

    throw new IllegalArgumentException(
        "Cannot convert value " + iValue + " to double for type: " + name);
  }

  /**
   * Convert the input object to a string.
   *
   * @param iValue Any type supported
   * @return The string if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  @Deprecated
  public String asString(final Object iValue) {
    return iValue.toString();
  }

  public boolean isMultiValue() {
    return this == EMBEDDEDLIST
        || this == EMBEDDEDMAP
        || this == EMBEDDEDSET
        || this == LINKLIST
        || this == LINKMAP
        || this == LINKSET
        || this == LINKBAG;
  }

  public boolean isList() {
    return this == EMBEDDEDLIST || this == LINKLIST;
  }

  public boolean isLink() {
    return this == LINK
        || this == LINKSET
        || this == LINKLIST
        || this == LINKMAP
        || this == LINKBAG;
  }

  public boolean isEmbedded() {
    return this == EMBEDDED || this == EMBEDDEDLIST || this == EMBEDDEDMAP || this == EMBEDDEDSET;
  }

  public Class<?> getDefaultJavaType() {
    return javaDefaultType;
  }

  public Set<OType> getCastable() {
    return castable;
  }

  @Deprecated
  public Class<?>[] getJavaTypes() {
    return null;
  }

  public String getName() {
    return name;
  }
}
