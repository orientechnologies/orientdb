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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.ONetworkThreadLocalSerializer;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.core.util.ODateHelper;

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  private static final String    CHARSET_UTF_8    = "UTF-8";
  private static final ORecordId NULL_RECORD_ID   = new ORecordId(-2, ORID.CLUSTER_POS_INVALID);
  private static final long      MILLISEC_PER_DAY = 86400000;

  public ORecordSerializerBinaryV0() {
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
          if (iFields[i].length() == len) {
            boolean matchField = true;
            for (int j = 0; j < len; ++j) {
              if (bytes.bytes[bytes.offset + j] != fields[i][j]) {
                matchField = false;
                break;
              }
            }
            if (matchField) {
              fieldName = iFields[i];
              unmarshalledFields++;
              bytes.skip(len);
              match = true;
              break;
            }
          }
        }

        if (!match) {
          // SKIP IT
          bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
          continue;
        }
        valuePos = readInteger(bytes);
        type = readOType(bytes);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        OGlobalProperty prop = getGlobalProperty(document, len);
        fieldName = prop.getName();
        valuePos = readInteger(bytes);
        if (prop.getType() != OType.ANY)
          type = prop.getType();
        else
          type = readOType(bytes);

      }

      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        final Object value = readSingleValue(bytes, type, document);
        bytes.offset = headerCursor;
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else
        ODocumentInternal.rawField(document, fieldName, null, null);

      if (unmarshalledFields == iFields.length)
        // ALL REQUESTED FIELDS UNMARSHALLED: EXIT
        break;
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
        final Object value = readSingleValue(bytes, type, document);
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
      if (!entry.getValue().exist())
        continue;
      if (entry.getValue().property == null && props != null)
        entry.getValue().property = props.get(entry.getKey());

      if (entry.getValue().property != null) {
        OVarIntSerializer.write(bytes, (entry.getValue().property.getId() + 1) * -1);
        if (entry.getValue().property.getType() != OType.ANY)
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
          throw new OSerializationException("Impossible serialize value of type " + value.getClass()
              + " with the ODocument binary serializer");
        }
        pointer = writeSingleValue(bytes, value, type, getLinkedType(document, type, values[i].getKey()));
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, bytes.bytes, pos[i]);
        if (values[i].getValue().property == null || values[i].getValue().property.getType() == OType.ANY)
          writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }

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

  protected Object readSingleValue(BytesContainer bytes, OType type, ODocument document) {
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
      int offset = ODateHelper.getDatabaseTimeZone().getOffset(savedTime);
      value = new Date(savedTime - offset);
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
        ODocumentInternal.addOwner((ODocument) value, document);

      break;
    case EMBEDDEDSET:
      value = readEmbeddedCollection(bytes, new OTrackedSet<Object>(document), document);
      break;
    case EMBEDDEDLIST:
      value = readEmbeddedCollection(bytes, new OTrackedList<Object>(document), document);
      break;
    case LINKSET:
      value = readLinkCollection(bytes, new ORecordLazySet(document));
      break;
    case LINKLIST:
      value = readLinkCollection(bytes, new ORecordLazyList(document));
      break;
    case BINARY:
      value = readBinary(bytes);
      break;
    case LINK:
      value = readOptimizedLink(bytes);
      break;
    case LINKMAP:
      value = readLinkMap(bytes, document);
      break;
    case EMBEDDEDMAP:
      value = readEmbeddedMap(bytes, document);
      break;
    case DECIMAL:
      value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
      break;
    case LINKBAG:
      ORidBag bag = new ORidBag();
      bag.fromStream(bytes);
      bag.setOwner(document);
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

  private byte[] readBinary(BytesContainer bytes) {
    int n = OVarIntSerializer.readAsInteger(bytes);
    byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<Object, OIdentifiable> readLinkMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    Map<Object, OIdentifiable> result = new ORecordLazyMap(document);
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = readSingleValue(bytes, keyType, document);
      ORecordId value = readOptimizedLink(bytes);
      if (value.equals(NULL_RECORD_ID))
        result.put(key, null);
      else
        result.put(key, value);
    }
    return result;
  }

  private Object readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final Map<Object, Object> result = new OTrackedMap<Object>(document);
    int last = 0;
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = readSingleValue(bytes, keyType, document);
      final int valuePos = readInteger(bytes);
      final OType type = readOType(bytes);
      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        Object value = readSingleValue(bytes, type, document);
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
  }

  private Collection<OIdentifiable> readLinkCollection(BytesContainer bytes, Collection<OIdentifiable> found) {
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

  private ORecordId readOptimizedLink(final BytesContainer bytes) {
    return new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
  }

  private Collection<?> readEmbeddedCollection(final BytesContainer bytes, final Collection<Object> found, final ODocument document) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes);

    if (type == OType.ANY) {
      for (int i = 0; i < items; i++) {
        OType itemType = readOType(bytes);
        if (itemType == OType.ANY)
          found.add(null);
        else
          found.add(readSingleValue(bytes, itemType, document));
      }
      return found;
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
  private int writeSingleValue(final BytesContainer bytes, Object value, final OType type, final OType linkedType) {
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
      long dg = Double.doubleToLongBits((Double) value);
      pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
      break;
    case FLOAT:
      int fg = Float.floatToIntBits((Float) value);
      pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
      break;
    case BYTE:
      pointer = bytes.alloc(1);
      bytes.bytes[pointer] = (Byte) value;
      break;
    case BOOLEAN:
      pointer = bytes.alloc(1);
      bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
      break;
    case DATETIME:
      if (value instanceof Long) {
        pointer = OVarIntSerializer.write(bytes, (Long) value);
      } else
        pointer = OVarIntSerializer.write(bytes, ((Date) value).getTime());
      break;
    case DATE:
      long dateValue;
      if (value instanceof Long) {
        dateValue = (Long) value;
      } else
        dateValue = ((Date) value).getTime();
      int offset = ODateHelper.getDatabaseTimeZone().getOffset(dateValue);
      pointer = OVarIntSerializer.write(bytes, (dateValue + offset) / MILLISEC_PER_DAY);
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

  private int writeLinkMap(BytesContainer bytes, Map<Object, OIdentifiable> map) {
    int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, OIdentifiable> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      OType type = OType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      if (entry.getValue() == null)
        writeNullLink(bytes);
      else
        writeOptimizedLink(bytes, entry.getValue());
    }
    return fullPos;
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
          throw new OSerializationException("Impossible serialize value of type " + value.getClass()
              + " with the ODocument binary serializer");
        }
        pointer = writeSingleValue(bytes, value, type, null);
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, bytes.bytes, pos[i]);
        writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }
    return fullPos;
  }

  private OIdentifiable recursiveLinkSave(OIdentifiable link) {

    if (link instanceof ORID) {
      if (((ORID) link).isValid() && ((ORID) link).isNew()) {
        final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.get();
        ORecord record = link.getRecord();
        if (record != null) {
          if (ONetworkThreadLocalSerializer.getNetworkSerializer() != null)
            throw new ODatabaseException("Impossible save a record during network serialization");
          database.save(record);
          return record;
        }
      }
    } else if (link instanceof ORecord) {
      ORID rid = link.getIdentity();
      if (((ORecord) link).isDirty() || (rid.isTemporary())) {
        if (ONetworkThreadLocalSerializer.getNetworkSerializer() != null)
          throw new ODatabaseException("Impossible save a record during network serialization");

        ((ORecord) link).save();
      }
    }
    return link;
  }

  private int writeNullLink(BytesContainer bytes) {
    int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;

  }

  private int writeOptimizedLink(BytesContainer bytes, OIdentifiable link) {
    link = recursiveLinkSave(link);
    assert link.getIdentity().isValid() || (ODatabaseRecordThreadLocal.INSTANCE.get().getStorage() instanceof OStorageProxy) : "Impossible to serialize invalid link";
    int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  private int writeLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> value) {
    assert (!(value instanceof OMVRBTreeRIDSet));
    final int pos = OVarIntSerializer.write(bytes, value.size());

    final boolean disabledAutoConvertion = value instanceof ORecordLazyMultiValue
        && ((ORecordLazyMultiValue) value).isAutoConvertToRecord();

    if (disabledAutoConvertion)
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
      if (disabledAutoConvertion)
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
      if (linkedType == null)
        type = getTypeFromValueEmbedded(itemValue);
      else
        type = linkedType;
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        writeSingleValue(bytes, itemValue, type, null);
      } else {
        throw new OSerializationException("Impossible serialize value of type " + value.getClass()
            + " with the ODocument binary serializer");
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

  protected String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected int readInteger(final BytesContainer container) {
    final int value = OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  private byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  private long readLong(final BytesContainer container) {
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
      throw new OSerializationException("Error on string encoding", e);
    }
  }

  protected String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new OSerializationException("Error on string decoding", e);
    }
  }
}
