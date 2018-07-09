/*
 * Copyright 2018 OrientDB.
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
package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author mdjurovi
 */
public class HelperClasses {
  protected static final String    CHARSET_UTF_8    = "UTF-8";
  protected static final ORecordId NULL_RECORD_ID   = new ORecordId(-2, ORID.CLUSTER_POS_INVALID);
  public static final long      MILLISEC_PER_DAY = 86400000;

  public static class Tuple<T1, T2> {

    private final T1 firstVal;
    private final T2 secondVal;

    Tuple(T1 firstVal, T2 secondVal) {
      this.firstVal = firstVal;
      this.secondVal = secondVal;
    }

    public T1 getFirstVal() {
      return firstVal;
    }

    public T2 getSecondVal() {
      return secondVal;
    }
  }

  public static class Triple<T1, T2, T3> {// extends Tuple<T1, T2>{
    private final T1 firstVal;
    private final T2 secondVal;
    private final T3 thirdVal;

    public Triple(T1 firstVal, T2 secondVal, T3 thirdVal) {
      this.firstVal = firstVal;
      this.secondVal = secondVal;
      this.thirdVal = thirdVal;
    }

    public T3 getThirdVal() {
      return thirdVal;
    }

    public T1 getFirstVal() {
      return firstVal;
    }

    public T2 getSecondVal() {
      return secondVal;
    }
  }

  protected static class RecordInfo {
    public int   fieldStartOffset;
    public int   fieldLength;
    public OType fieldType;
  }

  protected static class MapRecordInfo extends RecordInfo {
    public String key;
    public OType  keyType;
  }

  public static OType readOType(final BytesContainer bytes, boolean justRunThrough) {
    if (justRunThrough) {
      bytes.offset++;
      return null;
    }
    return OType.getById(readByte(bytes));
  }

  public static void writeOType(BytesContainer bytes, int pos, OType type) {
    bytes.bytes[pos] = (byte) type.getId();
  }
  
  public static void writeType(BytesContainer bytes, OType type){
    bytes.bytes[bytes.offset++] = (byte) type.getId();
  }
  
  public static OType readType(BytesContainer bytes){
    byte typeId = bytes.bytes[bytes.offset++];
    return OType.getById(typeId);
  }

  public static byte[] readBinary(final BytesContainer bytes) {
    final int n = OVarIntSerializer.readAsInteger(bytes);
    final byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  public static String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  public static int readInteger(final BytesContainer container) {
    final int value = OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  public static byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  public static long readLong(final BytesContainer container) {
    final long value = OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  public static ORecordId readOptimizedLink(final BytesContainer bytes, boolean justRunThrough) {
    int clusterId = OVarIntSerializer.readAsInteger(bytes);
    long clusterPos = OVarIntSerializer.readAsLong(bytes);
    if (justRunThrough)
      return null;
    else
      return new ORecordId(clusterId, clusterPos);
  }

  public static String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }

  public static byte[] bytesFromString(final String toWrite) {
    try {
      return toWrite.getBytes(CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string encoding"), e);
    }
  }

  public static long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
    Calendar fromCalendar = Calendar.getInstance(from);
    fromCalendar.setTimeInMillis(time);
    Calendar toCalendar = Calendar.getInstance(to);
    toCalendar.setTimeInMillis(0);
    toCalendar.set(Calendar.ERA, fromCalendar.get(Calendar.ERA));
    toCalendar.set(Calendar.YEAR, fromCalendar.get(Calendar.YEAR));
    toCalendar.set(Calendar.MONTH, fromCalendar.get(Calendar.MONTH));
    toCalendar.set(Calendar.DAY_OF_MONTH, fromCalendar.get(Calendar.DAY_OF_MONTH));
    toCalendar.set(Calendar.HOUR_OF_DAY, 0);
    toCalendar.set(Calendar.MINUTE, 0);
    toCalendar.set(Calendar.SECOND, 0);
    toCalendar.set(Calendar.MILLISECOND, 0);
    return toCalendar.getTimeInMillis();
  }
  
  public static OGlobalProperty getGlobalProperty(final ODocument document, final int len) {
    final int id = (len * -1) - 1;
    return ODocumentInternal.getGlobalPropertyById(document, id);
  }
  
  public static int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }
  
  public static int writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        final ORecord real = link.getRecord();
        if (real != null)
          link = real;
      } catch (ORecordNotFoundException ignored) {
        // IGNORE IT WILL FAIL THE ASSERT IN CASE
      }
    }
    if (link.getIdentity().getClusterId() < 0)
      throw new ODatabaseException("Impossible to serialize invalid link " + link.getIdentity());

    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }
  
  public static int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;
  }
  
  public static OType getTypeFromValueEmbedded(final Object fieldValue) {
    OType type = OType.getTypeByValue(fieldValue);
    if (type == OType.LINK && fieldValue instanceof ODocument && !((ODocument) fieldValue).getIdentity().isValid())
      type = OType.EMBEDDED;
    return type;
  }
  
  public static int writeLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> value) {
    final int pos = OVarIntSerializer.write(bytes, value.size());

    final boolean disabledAutoConversion =
        value instanceof ORecordLazyMultiValue && ((ORecordLazyMultiValue) value).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) value).setAutoConvertToRecord(false);

    try {
      for (OIdentifiable itemValue : value) {
        // TODO: handle the null links
        if (itemValue == null)
          writeNullLink(bytes);
        else
          writeOptimizedLink(bytes, itemValue);
      }

    } finally {
      if (disabledAutoConversion)
        ((ORecordLazyMultiValue) value).setAutoConvertToRecord(true);
    }

    return pos;
  }
  
  public static Collection<OIdentifiable> readLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> found,
      boolean justRunThrough) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      ORecordId id = readOptimizedLink(bytes, justRunThrough);
      if (!justRunThrough) {
        if (id.equals(NULL_RECORD_ID))
          found.add(null);
        else
          found.add(id);
      }
    }
    return found;
  }
  
  public static int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }
  
  public static int writeLinkMap(final BytesContainer bytes, final Map<Object, OIdentifiable> map) {
    final boolean disabledAutoConversion =
        map instanceof ORecordLazyMultiValue && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) map).setAutoConvertToRecord(false);

    try {
      final int fullPos = OVarIntSerializer.write(bytes, map.size());
      for (Map.Entry<Object, OIdentifiable> entry : map.entrySet()) {
        // TODO:check skip of complex types
        // FIXME: changed to support only string key on map
        final OType type = OType.STRING;
        writeOType(bytes, bytes.alloc(1), type);
        writeString(bytes, entry.getKey().toString());
        if (entry.getValue() == null)
          writeNullLink(bytes);
        else
          writeOptimizedLink(bytes, entry.getValue());
      }
      return fullPos;

    } finally {
      if (disabledAutoConversion)
        ((ORecordLazyMultiValue) map).setAutoConvertToRecord(true);
    }
  }
  
}
