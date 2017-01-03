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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  private static final String       CHARSET_UTF_8    = "UTF-8";
  private static final ORecordId    NULL_RECORD_ID   = new ORecordId(-2, ORID.CLUSTER_POS_INVALID);
  protected static final long       MILLISEC_PER_DAY = 86400000;

  private final OBinaryComparatorV0 comparator       = new OBinaryComparatorV0();

  public ORecordSerializerBinaryV0() {
  }

  public OBinaryComparator getComparator() {
    return comparator;
  }

  public void deserializePartial(final ODocument document, final BytesContainer bytes, final String[] iFields) {
    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i)
      fields[i] = iFields[i].getBytes();

    String fieldName = null;
    int valuePos;
    OType type;
    int unmarshalledFields = 0;

    while (true) {
      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len == 0) {
        // SCAN COMPLETED
        break;
      } else if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        boolean match = false;
        for (int i = 0; i < iFields.length; ++i) {
          if (iFields[i] != null && iFields[i].length() == len) {
            boolean matchField = true;
            for (int j = 0; j < len; ++j) {
              if (bytes.bytes[bytes.offset + j] != fields[i][j]) {
                matchField = false;
                break;
              }
            }
            if (matchField) {
              fieldName = iFields[i];
              bytes.skip(len);
              match = true;
              break;
            }
          }
        }

        if (!match) {
          // FIELD NOT INCLUDED: SKIP IT
          bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
          continue;
        }
        valuePos = readInteger(bytes);
        type = readOType(bytes);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final OGlobalProperty prop = getGlobalProperty(document, len);
        fieldName = prop.getName();

        boolean matchField = false;
        for (String f : iFields) {
          if (fieldName.equals(f)) {
            matchField = true;
            break;
          }
        }

        if (!matchField) {
          // FIELD NOT INCLUDED: SKIP IT
          bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
          continue;
        }

        valuePos = readInteger(bytes);
        if (prop.getType() != OType.ANY)
          type = prop.getType();
        else
          type = readOType(bytes);
      }

      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        final Object value = deserializeValue(bytes, type, document);
        bytes.offset = headerCursor;
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else
        ODocumentInternal.rawField(document, fieldName, null, null);

      if (++unmarshalledFields == iFields.length)
        // ALL REQUESTED FIELDS UNMARSHALLED: EXIT
        break;
    }
  }

  public OBinaryField deserializeField(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final byte[] field = iFieldName.getBytes();

    final OMetadataInternal metadata = (OMetadataInternal) ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata();
    final OImmutableSchema _schema = metadata.getImmutableSchemaSnapshot();

    while (true) {
      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len == 0) {
        // SCAN COMPLETED, NO FIELD FOUND
        return null;
      } else if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        if (iFieldName.length() == len) {
          boolean match = true;
          for (int j = 0; j < len; ++j)
            if (bytes.bytes[bytes.offset + j] != field[j]) {
              match = false;
              break;
            }

          bytes.skip(len);
          final int valuePos = readInteger(bytes);
          final OType type = readOType(bytes);

          if (valuePos == 0)
            return null;

          if (!match)
            continue;

          if (!ORecordSerializerBinary.INSTANCE.getCurrentSerializer().getComparator().isBinaryComparable(type))
            return null;

          bytes.offset = valuePos;
          return new OBinaryField(iFieldName, type, bytes, null);
        }

        // SKIP IT
        bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
        continue;

      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = _schema.getGlobalPropertyById(id);
        if (iFieldName.equals(prop.getName())) {
          final int valuePos = readInteger(bytes);
          final OType type;
          if (prop.getType() != OType.ANY)
            type = prop.getType();
          else
            type = readOType(bytes);

          if (valuePos == 0)
            return null;

          if (!ORecordSerializerBinary.INSTANCE.getCurrentSerializer().getComparator().isBinaryComparable(type))
            return null;

          bytes.offset = valuePos;

          final OProperty classProp = iClass.getProperty(iFieldName);
          return new OBinaryField(iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null);
        }
        bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
      }
    }
  }

  @Override
  public void deserialize(final ODocument document, final BytesContainer bytes) {
    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    int last = 0;
    String fieldName;
    int valuePos;
    OType type;
    while (true) {
      OGlobalProperty prop = null;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len == 0) {
        // SCAN COMPLETED
        break;
      } else if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
        bytes.skip(len);
        valuePos = readInteger(bytes);
        type = readOType(bytes);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        prop = getGlobalProperty(document, len);
        fieldName = prop.getName();
        valuePos = readInteger(bytes);
        if (prop.getType() != OType.ANY)
          type = prop.getType();
        else
          type = readOType(bytes);
      }

      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }

      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        final Object value = deserializeValue(bytes, type, document);
        if (bytes.offset > last)
          last = bytes.offset;
        bytes.offset = headerCursor;
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else
        ODocumentInternal.rawField(document, fieldName, null, null);
    }

    ORecordInternal.clearSource(document);

    if (last > bytes.offset)
      bytes.offset = last;
  }

  @Override
  public String[] getFieldNames(ODocument reference, final BytesContainer bytes) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final List<String> result = new ArrayList<String>();

    String fieldName;
    while (true) {
      OGlobalProperty prop = null;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len == 0) {
        // SCAN COMPLETED
        break;
      } else if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
        result.add(fieldName);

        // SKIP THE REST
        bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        prop = ODocumentInternal.getGlobalPropertyById(reference, id);
        if (prop == null) {
          throw new OSerializationException("Missing property definition for property id '" + id + "'");
        }
        result.add(prop.getName());

        // SKIP THE REST
        bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
      }
    }

    return result.toArray(new String[result.size()]);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(final ODocument document, final BytesContainer bytes, final boolean iClassOnly) {

    final OClass clazz = serializeClass(document, bytes);
    if (iClassOnly) {
      writeEmptyString(bytes);
      return;
    }

    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;

    final Set<Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    final int[] pos = new int[fields.size()];

    int i = 0;

    final Entry<String, ODocumentEntry> values[] = new Entry[fields.size()];
    for (Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      if (!docEntry.exist())
        continue;
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(entry.getKey());
        if (prop != null && docEntry.type == prop.getType())
          docEntry.property = prop;
      }

      if (docEntry.property != null) {
        OVarIntSerializer.write(bytes, (docEntry.property.getId() + 1) * -1);
        if (docEntry.property.getType() != OType.ANY)
          pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE);
        else
          pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
      } else {
        writeString(bytes, entry.getKey());
        pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
      }
      values[i] = entry;
      i++;
    }
    writeEmptyString(bytes);
    int size = i;

    for (i = 0; i < size; i++) {
      int pointer = 0;
      final Object value = values[i].getValue().value;
      if (value != null) {
        final OType type = getFieldType(values[i].getValue());
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }
        pointer = serializeValue(bytes, value, type, getLinkedType(document, type, values[i].getKey()));
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, bytes.bytes, pos[i]);
        if (values[i].getValue().property == null || values[i].getValue().property.getType() == OType.ANY)
          writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }

  }

  @Override
  public Object deserializeValue(final BytesContainer bytes, final OType type, final ODocument ownerDocument) {
    Object value = null;
    switch (type) {
    case INTEGER:
      value = OVarIntSerializer.readAsInteger(bytes);
      break;
    case LONG:
      value = OVarIntSerializer.readAsLong(bytes);
      break;
    case SHORT:
      value = OVarIntSerializer.readAsShort(bytes);
      break;
    case STRING:
      value = readString(bytes);
      break;
    case DOUBLE:
      value = Double.longBitsToDouble(readLong(bytes));
      break;
    case FLOAT:
      value = Float.intBitsToFloat(readInteger(bytes));
      break;
    case BYTE:
      value = readByte(bytes);
      break;
    case BOOLEAN:
      value = readByte(bytes) == 1;
      break;
    case DATETIME:
      value = new Date(OVarIntSerializer.readAsLong(bytes));
      break;
    case DATE:
      long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
      savedTime = convertDayToTimezone(TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
      value = new Date(savedTime);
      break;
    case EMBEDDED:
      value = new ODocument();
      deserialize((ODocument) value, bytes);
      if (((ODocument) value).containsField(ODocumentSerializable.CLASS_NAME)) {
        String className = ((ODocument) value).field(ODocumentSerializable.CLASS_NAME);
        try {
          Class<?> clazz = Class.forName(className);
          ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
          newValue.fromDocument((ODocument) value);
          value = newValue;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else
        ODocumentInternal.addOwner((ODocument) value, ownerDocument);

      break;
    case EMBEDDEDSET:
      value = readEmbeddedSet(bytes, ownerDocument);
      break;
    case EMBEDDEDLIST:
      value = readEmbeddedList(bytes, ownerDocument);
      break;
    case LINKSET:
      value = readLinkCollection(bytes, new ORecordLazySet(ownerDocument));
      break;
    case LINKLIST:
      value = readLinkCollection(bytes, new ORecordLazyList(ownerDocument));
      break;
    case BINARY:
      value = readBinary(bytes);
      break;
    case LINK:
      value = readOptimizedLink(bytes);
      break;
    case LINKMAP:
      value = readLinkMap(bytes, ownerDocument);
      break;
    case EMBEDDEDMAP:
      value = readEmbeddedMap(bytes, ownerDocument);
      break;
    case DECIMAL:
      value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
      break;
    case LINKBAG:
      ORidBag bag = new ORidBag();
      bag.fromStream(bytes);
      bag.setOwner(ownerDocument);
      value = bag;
      break;
    case TRANSIENT:
      break;
    case CUSTOM:
      try {
        String className = readString(bytes);
        Class<?> clazz = Class.forName(className);
        OSerializableStream stream = (OSerializableStream) clazz.newInstance();
        stream.fromStream(readBinary(bytes));
        if (stream instanceof OSerializableWrapper)
          value = ((OSerializableWrapper) stream).getSerializable();
        else
          value = stream;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      break;
    case ANY:
      break;

    }
    return value;
  }

  protected OClass serializeClass(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null)
      writeString(bytes, clazz.getName());
    else
      writeEmptyString(bytes);
    return clazz;
  }

  protected OGlobalProperty getGlobalProperty(final ODocument document, final int len) {
    final int id = (len * -1) - 1;
    return ODocumentInternal.getGlobalPropertyById(document, id);
  }

  protected OType readOType(final BytesContainer bytes) {
    return OType.getById(readByte(bytes));
  }

  private void writeOType(BytesContainer bytes, int pos, OType type) {
    bytes.bytes[pos] = (byte) type.getId();
  }

  private static byte[] readBinary(final BytesContainer bytes) {
    final int n = OVarIntSerializer.readAsInteger(bytes);
    final byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<Object, OIdentifiable> readLinkMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final ORecordLazyMap result = new ORecordLazyMap(document);

    result.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {

      while ((size--) > 0) {
        final OType keyType = readOType(bytes);
        final Object key = deserializeValue(bytes, keyType, document);
        final ORecordId value = readOptimizedLink(bytes);
        if (value.equals(NULL_RECORD_ID))
          result.put(key, null);
        else
          result.put(key, value);
      }
      return result;

    } finally {
      result.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  private Object readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap<Object> result = new OTrackedMap<Object>(document);

    result.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {

      int last = 0;
      while ((size--) > 0) {
        OType keyType = readOType(bytes);
        Object key = deserializeValue(bytes, keyType, document);
        final int valuePos = readInteger(bytes);
        final OType type = readOType(bytes);
        if (valuePos != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = valuePos;
          Object value = deserializeValue(bytes, type, document);
          if (bytes.offset > last)
            last = bytes.offset;
          bytes.offset = headerCursor;
          result.put(key, value);
        } else
          result.put(key, null);
      }
      if (last > bytes.offset)
        bytes.offset = last;
      return result;

    } finally {
      result.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  private Collection<OIdentifiable> readLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> found) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      ORecordId id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID))
        found.add(null);
      else
        found.add(id);
    }
    return found;
  }

  protected static ORecordId readOptimizedLink(final BytesContainer bytes) {
    return new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
  }

  private Collection<?> readEmbeddedSet(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes);

    if (type == OType.ANY) {
      final OTrackedSet found = new OTrackedSet<Object>(ownerDocument);

      found.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
      try {

        for (int i = 0; i < items; i++) {
          OType itemType = readOType(bytes);
          if (itemType == OType.ANY)
            found.add(null);
          else
            found.add(deserializeValue(bytes, itemType, ownerDocument));
        }
        return found;

      } finally {
        found.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
    }
    // TODO: manage case where type is known
    return null;
  }

  private Collection<?> readEmbeddedList(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes);

    if (type == OType.ANY) {
      final OTrackedList found = new OTrackedList<Object>(ownerDocument);

      found.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
      try {

        for (int i = 0; i < items; i++) {
          OType itemType = readOType(bytes);
          if (itemType == OType.ANY)
            found.add(null);
          else
            found.add(deserializeValue(bytes, itemType, ownerDocument));
        }
        return found;

      } finally {
        found.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
    }
    // TODO: manage case where type is known
    return null;
  }

  private OType getLinkedType(ODocument document, OType type, String key) {
    if (type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDMAP)
      return null;
    OClass immutableClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableClass != null) {
      OProperty prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public int serializeValue(final BytesContainer bytes, Object value, final OType type, final OType linkedType) {
    int pointer = 0;
    switch (type) {
    case INTEGER:
    case LONG:
    case SHORT:
      pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
      break;
    case STRING:
      pointer = writeString(bytes, value.toString());
      break;
    case DOUBLE:
      long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
      break;
    case FLOAT:
      int fg = Float.floatToIntBits(((Number) value).floatValue());
      pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
      break;
    case BYTE:
      pointer = bytes.alloc(1);
      bytes.bytes[pointer] = ((Number) value).byteValue();
      break;
    case BOOLEAN:
      pointer = bytes.alloc(1);
      bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
      break;
    case DATETIME:
      if (value instanceof Number) {
        pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
      } else
        pointer = OVarIntSerializer.write(bytes, ((Date) value).getTime());
      break;
    case DATE:
      long dateValue;
      if (value instanceof Number) {
        dateValue = ((Number) value).longValue();
      } else
        dateValue = ((Date) value).getTime();
      dateValue = convertDayToTimezone(ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
      pointer = OVarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
      break;
    case EMBEDDED:
      pointer = bytes.offset;
      if (value instanceof ODocumentSerializable) {
        ODocument cur = ((ODocumentSerializable) value).toDocument();
        cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
        serialize(cur, bytes, false);
      } else {
        serialize((ODocument) value, bytes, false);
      }
      break;
    case EMBEDDEDSET:
    case EMBEDDEDLIST:
      if (value.getClass().isArray())
        pointer = writeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), linkedType);
      else
        pointer = writeEmbeddedCollection(bytes, (Collection<?>) value, linkedType);
      break;
    case DECIMAL:
      BigDecimal decimalValue = (BigDecimal) value;
      pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
      ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
      break;
    case BINARY:
      pointer = writeBinary(bytes, (byte[]) (value));
      break;
    case LINKSET:
    case LINKLIST:
      Collection<OIdentifiable> ridCollection = (Collection<OIdentifiable>) value;
      pointer = writeLinkCollection(bytes, ridCollection);
      break;
    case LINK:
      if (!(value instanceof OIdentifiable))
        throw new OValidationException("Value '" + value + "' is not a OIdentifiable");

      pointer = writeOptimizedLink(bytes, (OIdentifiable) value);
      break;
    case LINKMAP:
      pointer = writeLinkMap(bytes, (Map<Object, OIdentifiable>) value);
      break;
    case EMBEDDEDMAP:
      pointer = writeEmbeddedMap(bytes, (Map<Object, Object>) value);
      break;
    case LINKBAG:
      pointer = ((ORidBag) value).toStream(bytes);
      break;
    case CUSTOM:
      if (!(value instanceof OSerializableStream))
        value = new OSerializableWrapper((Serializable) value);
      pointer = writeString(bytes, value.getClass().getName());
      writeBinary(bytes, ((OSerializableStream) value).toStream());
      break;
    case TRANSIENT:
      break;
    case ANY:
      break;
    }
    return pointer;
  }

  private int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  private int writeLinkMap(final BytesContainer bytes, final Map<Object, OIdentifiable> map) {
    final boolean disabledAutoConversion = map instanceof ORecordLazyMultiValue
        && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) map).setAutoConvertToRecord(false);

    try {
      final int fullPos = OVarIntSerializer.write(bytes, map.size());
      for (Entry<Object, OIdentifiable> entry : map.entrySet()) {
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

  @SuppressWarnings("unchecked")
  private int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int[] pos = new int[map.size()];
    int i = 0;
    Entry<Object, Object> values[] = new Entry[map.size()];
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      OType type = OType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
      values[i] = entry;
      i++;
    }

    for (i = 0; i < values.length; i++) {
      int pointer = 0;
      final Object value = values[i].getValue();
      if (value != null) {
        final OType type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }
        pointer = serializeValue(bytes, value, type, null);
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, bytes.bytes, pos[i]);
        writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }
    return fullPos;
  }

  private int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;

  }

  private int writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        final ORecord real = link.getRecord();
        if (real != null)
          link = real;
      } catch (ORecordNotFoundException ex) {
        // IGNORE IT WILL FAIL THE ASSERT IN CASE
      }
    }
    if (link.getIdentity().getClusterId() < 0 && ORecordSerializationContext.getContext() != null)
      throw new ODatabaseException("Impossible to serialize invalid link " + link.getIdentity());

    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  private int writeLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> value) {
    final int pos = OVarIntSerializer.write(bytes, value.size());

    final boolean disabledAutoConversion = value instanceof ORecordLazyMultiValue
        && ((ORecordLazyMultiValue) value).isAutoConvertToRecord();

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

  private int writeEmbeddedCollection(final BytesContainer bytes, final Collection<?> value, final OType linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    writeOType(bytes, bytes.alloc(1), OType.ANY);
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), OType.ANY);
        continue;
      }
      OType type;
      if (linkedType == null || linkedType == OType.ANY)
        type = getTypeFromValueEmbedded(itemValue);
      else
        type = linkedType;
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, itemValue, type, null);
      } else {
        throw new OSerializationException(
            "Impossible serialize value of type " + itemValue.getClass() + " with the ODocument binary serializer");
      }
    }
    return pos;
  }

  private OType getFieldType(final ODocumentEntry entry) {
    OType type = entry.type;
    if (type == null) {
      final OProperty prop = entry.property;
      if (prop != null)
        type = prop.getType();

    }
    if (type == null || OType.ANY == type)
      type = OType.getTypeByValue(entry.value);
    return type;
  }

  private OType getTypeFromValueEmbedded(final Object fieldValue) {
    OType type = OType.getTypeByValue(fieldValue);
    if (type == OType.LINK && fieldValue instanceof ODocument && !((ODocument) fieldValue).getIdentity().isValid())
      type = OType.EMBEDDED;
    return type;
  }

  protected static String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected static int readInteger(final BytesContainer container) {
    final int value = OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  protected static byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  protected static long readLong(final BytesContainer container) {
    final long value = OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  private int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

  private byte[] bytesFromString(final String toWrite) {
    try {
      return toWrite.getBytes(CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string encoding"), e);
    }
  }

  protected static String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }

  protected static long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
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

}
