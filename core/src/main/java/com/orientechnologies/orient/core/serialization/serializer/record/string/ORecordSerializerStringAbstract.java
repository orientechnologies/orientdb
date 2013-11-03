/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationSetThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerEmbedded;

@SuppressWarnings("serial")
public abstract class ORecordSerializerStringAbstract implements ORecordSerializer, Serializable {
  protected static final OProfilerMBean PROFILER              = Orient.instance().getProfiler();
  private static final char             DECIMAL_SEPARATOR     = '.';
  private static final String           MAX_INTEGER_AS_STRING = String.valueOf(Integer.MAX_VALUE);
  private static final int              MAX_INTEGER_DIGITS    = MAX_INTEGER_AS_STRING.length();

  protected abstract StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat,
      final OUserObject2RecordHandler iObjHandler, final Set<ODocument> iMarshalledRecords, boolean iOnlyDelta,
      boolean autoDetectCollectionType);

  public abstract ORecordInternal<?> fromString(String iContent, ORecordInternal<?> iRecord, String[] iFields);

  public StringBuilder toString(final ORecordInternal<?> iRecord, final String iFormat) {
    return toString(iRecord, new StringBuilder(), iFormat, ODatabaseRecordThreadLocal.INSTANCE.get(),
        OSerializationSetThreadLocal.INSTANCE.get(), false, true);
  }

  public StringBuilder toString(final ORecordInternal<?> iRecord, final String iFormat, final boolean autoDetectCollectionType) {
    return toString(iRecord, new StringBuilder(), iFormat, ODatabaseRecordThreadLocal.INSTANCE.get(),
        OSerializationSetThreadLocal.INSTANCE.get(), false, autoDetectCollectionType);
  }

  public StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat) {
    return toString(iRecord, iOutput, iFormat, null, OSerializationSetThreadLocal.INSTANCE.get(), false, true);
  }

  public ORecordInternal<?> fromString(final String iSource) {
    return fromString(iSource, (ORecordInternal<?>) ODatabaseRecordThreadLocal.INSTANCE.get().newInstance(), null);
  }

  public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord, final String[] iFields) {
    final long timer = PROFILER.startChrono();

    try {
      return fromString(OBinaryProtocol.bytes2string(iSource), iRecord, iFields);
    } finally {

      PROFILER
          .stopChrono(PROFILER.getProcessMetric("serializer.record.string.fromStream"), "Deserialize record from stream", timer);
    }
  }

  public byte[] toStream(final ORecordInternal<?> iRecord, boolean iOnlyDelta) {
    final long timer = PROFILER.startChrono();

    try {
      return OBinaryProtocol.string2bytes(toString(iRecord, new StringBuilder(), null, null,
          OSerializationSetThreadLocal.INSTANCE.get(), iOnlyDelta, true).toString());
    } finally {

      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.toStream"), "Serialize record to stream", timer);
    }
  }

  public static Object fieldTypeFromStream(final ODocument iDocument, OType iType, final Object iValue) {
    if (iValue == null)
      return null;

    if (iType == null)
      iType = OType.EMBEDDED;

    switch (iType) {
    case STRING:
    case INTEGER:
    case BOOLEAN:
    case FLOAT:
    case DECIMAL:
    case LONG:
    case DOUBLE:
    case SHORT:
    case BYTE:
    case BINARY:
    case DATE:
    case DATETIME:
    case LINK:
      return simpleValueFromStream(iValue, iType);

    case EMBEDDED: {
      // EMBEDED RECORD
      final ODocument doc = ((ODocument) OStringSerializerEmbedded.INSTANCE.fromStream((String) iValue));
      if (doc != null)
        return doc.addOwner(iDocument);
      return null;
    }

    case CUSTOM:
      // RECORD
      final Object result = OStringSerializerAnyStreamable.INSTANCE.fromStream((String) iValue);
      if (result instanceof ODocument)
        ((ODocument) result).addOwner(iDocument);
      return result;

    case EMBEDDEDSET:
    case EMBEDDEDLIST: {
      final String value = (String) iValue;
      return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionFromStream(iDocument, iType, null, null, value);
    }

    case EMBEDDEDMAP: {
      final String value = (String) iValue;
      return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapFromStream(iDocument, null, value, null);
    }
    }

    throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
  }

  public static Object convertValue(final String iValue, final OType iExpectedType) {
    final Object v = getTypeValue((String) iValue);
    return OType.convert(v, iExpectedType.getDefaultJavaType());
  }

  public static void fieldTypeToString(final StringBuilder iBuffer, OType iType, final Object iValue) {
    if (iValue == null)
      return;

    final long timer = PROFILER.startChrono();

    if (iType == null) {
      if (iValue instanceof ORID)
        iType = OType.LINK;
      else
        iType = OType.EMBEDDED;
    }

    switch (iType) {
    case STRING:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.string2string"), "Serialize string to string", timer);
      break;

    case BOOLEAN:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.bool2string"), "Serialize boolean to string", timer);
      break;

    case INTEGER:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.int2string"), "Serialize integer to string", timer);
      break;

    case FLOAT:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.float2string"), "Serialize float to string", timer);
      break;

    case DECIMAL:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.decimal2string"), "Serialize decimal to string",
          timer);
      break;

    case LONG:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.long2string"), "Serialize long to string", timer);
      break;

    case DOUBLE:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.double2string"), "Serialize double to string", timer);
      break;

    case SHORT:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.short2string"), "Serialize short to string", timer);
      break;

    case BYTE:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.byte2string"), "Serialize byte to string", timer);
      break;

    case BINARY:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.binary2string"), "Serialize binary to string", timer);
      break;

    case DATE:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.date2string"), "Serialize date to string", timer);
      break;

    case DATETIME:
      simpleValueToStream(iBuffer, iType, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.datetime2string"), "Serialize datetime to string",
          timer);
      break;

    case LINK:
      if (iValue instanceof ORecordId)
        ((ORecordId) iValue).toString(iBuffer);
      else
        ((ORecord<?>) iValue).getIdentity().toString(iBuffer);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.link2string"), "Serialize link to string", timer);
      break;

    case EMBEDDEDSET:
      ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionToStream(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined(),
          null, iBuffer, null, null, iValue, null, true, true);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.embedSet2string"), "Serialize embeddedset to string",
          timer);
      break;

    case EMBEDDEDLIST:
      ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionToStream(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined(),
          null, iBuffer, null, null, iValue, null, true, false);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.embedList2string"),
          "Serialize embeddedlist to string", timer);
      break;

    case EMBEDDEDMAP:
      ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapToStream(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined(), null,
          iBuffer, null, null, iValue, null, true);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.embedMap2string"), "Serialize embeddedmap to string",
          timer);
      break;

    case EMBEDDED:
      OStringSerializerEmbedded.INSTANCE.toStream(iBuffer, iValue);
      PROFILER
          .stopChrono(PROFILER.getProcessMetric("serializer.record.string.embed2string"), "Serialize embedded to string", timer);
      break;

    case CUSTOM:
      OStringSerializerAnyStreamable.INSTANCE.toStream(iBuffer, iValue);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.custom2string"), "Serialize custom to string", timer);
      break;

    default:
      throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
    }
  }

  /**
   * Parses a string returning the closer type. Numbers by default are INTEGER if haven't decimal separator, otherwise FLOAT. To
   * treat all the number types numbers are postponed with a character that tells the type: b=byte, s=short, l=long, f=float,
   * d=double, t=date.
   * 
   * @param iUnusualSymbols
   *          Localized decimal number separators
   * @param iValue
   *          Value to parse
   * @return The closest type recognized
   */
  public static OType getType(final String iValue) {
    if (iValue.length() == 0)
      return null;

    final char firstChar = iValue.charAt(0);

    if (firstChar == ORID.PREFIX)
      // RID
      return OType.LINK;
    else if (firstChar == '\'' || firstChar == '"')
      return OType.STRING;
    else if (firstChar == OStringSerializerHelper.BINARY_BEGINEND)
      return OType.BINARY;
    else if (firstChar == OStringSerializerHelper.EMBEDDED_BEGIN)
      return OType.EMBEDDED;
    else if (firstChar == OStringSerializerHelper.LINK)
      return OType.LINK;
    else if (firstChar == OStringSerializerHelper.LIST_BEGIN)
      return OType.EMBEDDEDLIST;
    else if (firstChar == OStringSerializerHelper.SET_BEGIN)
      return OType.EMBEDDEDSET;
    else if (firstChar == OStringSerializerHelper.MAP_BEGIN)
      return OType.EMBEDDEDMAP;
    else if (firstChar == OStringSerializerHelper.CUSTOM_TYPE)
      return OType.CUSTOM;

    // BOOLEAN?
    if (iValue.equalsIgnoreCase("true") || iValue.equalsIgnoreCase("false"))
      return OType.BOOLEAN;

    // NUMBER OR STRING?
    boolean integer = true;
    for (int index = 0; index < iValue.length(); ++index) {
      final char c = iValue.charAt(index);
      if (c < '0' || c > '9')
        if ((index == 0 && (c == '+' || c == '-')))
          continue;
        else if (c == DECIMAL_SEPARATOR)
          integer = false;
        else {
          if (index > 0)
            if (!integer && c == 'E') {
              // CHECK FOR SCIENTIFIC NOTATION
              if (index < iValue.length()) {
                if (iValue.charAt(index + 1) == '-')
                  // JUMP THE DASH IF ANY (NOT MANDATORY)
                  index++;
                continue;
              }
            } else if (c == 'f')
              return OType.FLOAT;
            else if (c == 'c')
              return OType.DECIMAL;
            else if (c == 'l')
              return OType.LONG;
            else if (c == 'd')
              return OType.DOUBLE;
            else if (c == 'b')
              return OType.BYTE;
            else if (c == 'a')
              return OType.DATE;
            else if (c == 't')
              return OType.DATETIME;
            else if (c == 's')
              return OType.SHORT;

          return OType.STRING;
        }
    }

    if (integer) {
      // AUTO CONVERT TO LONG IF THE INTEGER IS TOO BIG
      final int numberLength = iValue.length();
      if (numberLength > MAX_INTEGER_DIGITS || (numberLength == MAX_INTEGER_DIGITS && iValue.compareTo(MAX_INTEGER_AS_STRING) > 0))
        return OType.LONG;
    }

    return integer ? OType.INTEGER : OType.FLOAT;
  }

  /**
   * Parses the field type char returning the closer type. Default is STRING. b=binary if iValue.lenght() >= 4 b=byte if
   * iValue.lenght() <= 3 s=short, l=long f=float d=double a=date t=datetime
   * 
   * @param iValue
   *          Value to parse
   * @param iCharType
   *          Char value indicating the type
   * @return The closest type recognized
   */
  public static OType getType(final String iValue, final char iCharType) {
    if (iCharType == 'f')
      return OType.FLOAT;
    else if (iCharType == 'c')
      return OType.DECIMAL;
    else if (iCharType == 'l')
      return OType.LONG;
    else if (iCharType == 'd')
      return OType.DOUBLE;
    else if (iCharType == 'b') {
      if (iValue.length() >= 1 && iValue.length() <= 3)
        return OType.BYTE;
      else
        return OType.BINARY;
    } else if (iCharType == 'a')
      return OType.DATE;
    else if (iCharType == 't')
      return OType.DATETIME;
    else if (iCharType == 's')
      return OType.SHORT;
    else if (iCharType == 'e')
      return OType.EMBEDDEDSET;

    return OType.STRING;
  }

  /**
   * Parses a string returning the value with the closer type. Numbers by default are INTEGER if haven't decimal separator,
   * otherwise FLOAT. To treat all the number types numbers are postponed with a character that tells the type: b=byte, s=short,
   * l=long, f=float, d=double, t=date. If starts with # it's a RecordID. Most of the code is equals to getType() but has been
   * copied to speed-up it.
   * 
   * @param iUnusualSymbols
   *          Localized decimal number separators
   * @param iValue
   *          Value to parse
   * @return The closest type recognized
   */
  public static Object getTypeValue(final String iValue) {
    if (iValue == null)
      return null;

    if (iValue.length() == 0)
      return "";

    if (iValue.length() > 1)
      if (iValue.charAt(0) == '"' && iValue.charAt(iValue.length() - 1) == '"')
        // STRING
        return OStringSerializerHelper.decode(iValue.substring(1, iValue.length() - 1));
      else if (iValue.charAt(0) == OStringSerializerHelper.BINARY_BEGINEND
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.BINARY_BEGINEND)
        // STRING
        return OStringSerializerHelper.getBinaryContent(iValue);
      else if (iValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.LIST_END) {
        // LIST
        final ArrayList<String> coll = new ArrayList<String>();
        OStringSerializerHelper.getCollection(iValue, 0, coll, OStringSerializerHelper.LIST_BEGIN,
            OStringSerializerHelper.LIST_END, OStringSerializerHelper.COLLECTION_SEPARATOR);
        return coll;
      } else if (iValue.charAt(0) == OStringSerializerHelper.SET_BEGIN
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.SET_END) {
        // SET
        final Set<String> coll = new HashSet<String>();
        OStringSerializerHelper.getCollection(iValue, 0, coll, OStringSerializerHelper.SET_BEGIN, OStringSerializerHelper.SET_END,
            OStringSerializerHelper.COLLECTION_SEPARATOR);
        return coll;
      } else if (iValue.charAt(0) == OStringSerializerHelper.MAP_BEGIN
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.MAP_END) {
        // MAP
        return OStringSerializerHelper.getMap(iValue);
      }

    if (iValue.charAt(0) == ORID.PREFIX)
      // RID
      return new ORecordId(iValue);

    boolean integer = true;
    char c;

    for (int index = 0; index < iValue.length(); ++index) {
      c = iValue.charAt(index);
      if (c < '0' || c > '9')
        if ((index == 0 && (c == '+' || c == '-')))
          continue;
        else if (c == DECIMAL_SEPARATOR)
          integer = false;
        else {
          if (index > 0) {
            if (!integer && c == 'E') {
              // CHECK FOR SCIENTIFIC NOTATION
              if (index < iValue.length())
                index++;
              if (iValue.charAt(index) == '-')
                continue;
            }

            final String v = iValue.substring(0, index);

            if (c == 'f')
              return new Float(v);
            else if (c == 'c')
              return new BigDecimal(v);
            else if (c == 'l')
              return new Long(v);
            else if (c == 'd')
              return new Double(v);
            else if (c == 'b')
              return new Byte(v);
            else if (c == 'a' || c == 't')
              return new Date(Long.parseLong(v));
            else if (c == 's')
              return new Short(v);
          }
          return iValue;
        }
    }

    if (integer) {
      try {
        return new Integer(iValue);
      } catch (NumberFormatException e) {
        return new Long(iValue);
      }
    } else
      return new BigDecimal(iValue);
  }

  public static Object simpleValueFromStream(final Object iValue, final OType iType) {
    switch (iType) {
    case STRING:
      if (iValue instanceof String) {
        final String s = OStringSerializerHelper.getStringContent(iValue);
        return OStringSerializerHelper.decode(s);
      }
      return iValue.toString();

    case INTEGER:
      if (iValue instanceof Integer)
        return iValue;
      return new Integer(iValue.toString());

    case BOOLEAN:
      if (iValue instanceof Boolean)
        return iValue;
      return new Boolean(iValue.toString());

    case FLOAT:
      if (iValue instanceof Float)
        return iValue;
      return convertValue((String) iValue, iType);

    case DECIMAL:
      if (iValue instanceof BigDecimal)
        return iValue;
      return convertValue((String) iValue, iType);

    case LONG:
      if (iValue instanceof Long)
        return iValue;
      return convertValue((String) iValue, iType);

    case DOUBLE:
      if (iValue instanceof Double)
        return iValue;
      return convertValue((String) iValue, iType);

    case SHORT:
      if (iValue instanceof Short)
        return iValue;
      return convertValue((String) iValue, iType);

    case BYTE:
      if (iValue instanceof Byte)
        return iValue;
      return convertValue((String) iValue, iType);

    case BINARY:
      return OStringSerializerHelper.getBinaryContent(iValue);

    case DATE:
    case DATETIME:
      if (iValue instanceof Date)
        return iValue;
      return convertValue((String) iValue, iType);

    case LINK:
      if (iValue instanceof ORID)
        return iValue.toString();
      else if (iValue instanceof String)
        return new ORecordId((String) iValue);
      else
        return ((ORecord<?>) iValue).getIdentity().toString();
    }

    throw new IllegalArgumentException("Type " + iType + " is not simple type.");
  }

  public static void simpleValueToStream(final StringBuilder iBuffer, final OType iType, final Object iValue) {
    if (iValue == null || iType == null)
      return;
    switch (iType) {
    case STRING:
      iBuffer.append('"');
      iBuffer.append(OStringSerializerHelper.encode(iValue.toString()));
      iBuffer.append('"');
      break;

    case BOOLEAN:
      iBuffer.append(String.valueOf(iValue));
      break;

    case INTEGER:
      iBuffer.append(String.valueOf(iValue));
      break;

    case FLOAT:
      iBuffer.append(String.valueOf(iValue));
      iBuffer.append('f');
      break;

    case DECIMAL:
      if (iValue instanceof BigDecimal)
        iBuffer.append(((BigDecimal) iValue).toPlainString());
      else
        iBuffer.append(String.valueOf(iValue));
      iBuffer.append('c');
      break;

    case LONG:
      iBuffer.append(String.valueOf(iValue));
      iBuffer.append('l');
      break;

    case DOUBLE:
      iBuffer.append(String.valueOf(iValue));
      iBuffer.append('d');
      break;

    case SHORT:
      iBuffer.append(String.valueOf(iValue));
      iBuffer.append('s');
      break;

    case BYTE:
      if (iValue instanceof Character)
        iBuffer.append((int) ((Character) iValue).charValue());
      else if (iValue instanceof String)
        iBuffer.append(String.valueOf((int) ((String) iValue).charAt(0)));
      else
        iBuffer.append(String.valueOf(iValue));
      iBuffer.append('b');
      break;

    case BINARY:
      iBuffer.append(OStringSerializerHelper.BINARY_BEGINEND);
      if (iValue instanceof Byte)
        iBuffer.append(OBase64Utils.encodeBytes(new byte[] { ((Byte) iValue).byteValue() }));
      else
        iBuffer.append(OBase64Utils.encodeBytes((byte[]) iValue));
      iBuffer.append(OStringSerializerHelper.BINARY_BEGINEND);
      break;

    case DATE:
      if (iValue instanceof Date) {
        // RESET HOURS, MINUTES, SECONDS AND MILLISECONDS
        Calendar calendar = Calendar.getInstance();
        calendar.setTime((Date) iValue);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        iBuffer.append(calendar.getTimeInMillis());
      } else
        iBuffer.append(iValue);
      iBuffer.append('a');
      break;

    case DATETIME:
      if (iValue instanceof Date)
        iBuffer.append(((Date) iValue).getTime());
      else
        iBuffer.append(iValue);
      iBuffer.append('t');
      break;
    }
  }
}
