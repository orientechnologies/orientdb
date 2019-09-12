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
package com.orientechnologies.orient.core.delta;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OSerializableWrapper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author mdjurovi
 */
public class ODocumentDeltaSerializerV1 extends ODocumentDeltaSerializer {

  protected ODocumentDeltaSerializerV1() {

  }

  @Override
  public byte[] toStream(ODocumentDelta delta) {
    BytesContainer bytes = new BytesContainer();
    serialize(delta, bytes);
    return bytes.fitBytes();
  }

  private void serialize(ODocumentDelta deltaDoc, BytesContainer bytes) {
    for (Map.Entry<String, ValueType> entry : deltaDoc.fields.entrySet()) {
      String fieldName = entry.getKey();
      ValueType fieldValueType = entry.getValue();
      Object fieldValue = fieldValueType.getValue();

      //serialize field name
      serializeValue(bytes, fieldName, OType.STRING);

      //serialize field type
      OTypeInterface type = fieldValue != null ? fieldValueType.getType() : null;
      if (type == null) {
        //signal for null value
        HelperClasses.writeByte(bytes, (byte) -1);
        continue;
      }

      if (type == OType.LINK && fieldValue instanceof ODocument) {
        type = OType.EMBEDDED;
      }

      HelperClasses.writeType(bytes, type);
      //serialize field value
      serializeValue(bytes, fieldValue, type);
    }
    serializeValue(bytes, -1, OType.INTEGER);
  }

  private void deserialize(ODocumentDelta delta, BytesContainer bytes) {
    boolean endReached = false;
    while (!endReached) {
      int fieldNameLength = OVarIntSerializer.readAsInteger(bytes);
      if (fieldNameLength > -1) {
        if (fieldNameLength > 0) {
          String fieldName = HelperClasses.stringFromBytes(bytes.bytes, bytes.offset, fieldNameLength);
          bytes.offset += fieldNameLength;
          OTypeInterface type = HelperClasses.readDeltaDocumentType(bytes);
          if (type == null) {
            delta.field(fieldName, new ValueType(null, null));
            continue;
          }
          Object value = deserializeValue(bytes, type, null);
          delta.field(fieldName, new ValueType(value, type));
        }
      } else {
        endReached = true;
      }
    }
  }

  private ODocumentDelta deserialize(BytesContainer bytes) {
    ODocumentDelta ret = new ODocumentDelta();
    deserialize(ret, bytes);
    return ret;
  }

  protected int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  private int serializeEmbeddedCollection(final BytesContainer bytes, final Collection<?> value, final OTypeInterface linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.    
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        HelperClasses.writeType(bytes, OType.ANY);
        continue;
      }
      OTypeInterface type;
      if (linkedType == null || linkedType == OType.ANY) {
        type = HelperClasses.getDeltaTypeFromValueEmbedded(itemValue);
        if (type == OType.LINK && itemValue instanceof ODocument) {
          type = OType.EMBEDDED;
        }
      } else
        type = linkedType;
      if (type != null) {
        HelperClasses.writeType(bytes, type);
        serializeValue(bytes, itemValue, type);
      } else {
        throw new OSerializationException(
            "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
      }
    }
    return pos;
  }

  private Collection<?> deserializeEmbeddedCollection(final BytesContainer bytes, final ODocument ownerDocument, boolean isList) {
    final OTrackedMultiValue found;
    if (isList) {
      found = deserializeEmbeddedList(bytes, ownerDocument);
    } else {
      found = deserializeEmbeddedSet(bytes, ownerDocument);
    }
    return (Collection<?>) found;
  }

  protected OTrackedSet deserializeEmbeddedSet(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);

    final OTrackedSet found = new OTrackedSet<>(ownerDocument);

    for (int i = 0; i < items; i++) {
      OTypeInterface itemType = HelperClasses.readDeltaDocumentType(bytes);
      if (itemType == OType.ANY)
        found.addInternal(null);
      else
        found.addInternal(deserializeValue(bytes, itemType, ownerDocument));
    }
    return found;
  }

  private OTrackedList deserializeEmbeddedList(final BytesContainer bytes, final ODocument ownerDocument) {
    final int items = OVarIntSerializer.readAsInteger(bytes);

    final OTrackedList found = new OTrackedList<>(ownerDocument);
    for (int i = 0; i < items; i++) {
      OTypeInterface itemType = HelperClasses.readDeltaDocumentType(bytes);
      if (itemType == OType.ANY)
        found.addInternal(null);
      else
        found.addInternal(deserializeValue(bytes, itemType, ownerDocument));
    } return found;

  }

  private void serializeValue(final BytesContainer bytes, Object value, OTypeInterface type) {

    if (type == OType.INTEGER || type == OType.LONG || type == OType.SHORT) {
      OVarIntSerializer.write(bytes, ((Number) value).longValue());
    } else if (type == OType.STRING) {
      HelperClasses.writeString(bytes, value.toString());
    } else if (type == OType.DOUBLE) {
      long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      int pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
    } else if (type == OType.FLOAT) {
      int fg = Float.floatToIntBits(((Number) value).floatValue());
      int pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
    } else if (type == OType.BYTE) {
      HelperClasses.writeByte(bytes, ((Number) value).byteValue());
    } else if (type == OType.BOOLEAN) {
      byte val = ((Boolean) value) ? (byte) 1 : (byte) 0;
      HelperClasses.writeByte(bytes, val);
    } else if (type == OType.DATETIME) {
      if (value instanceof Number) {
        OVarIntSerializer.write(bytes, ((Number) value).longValue());
      } else
        OVarIntSerializer.write(bytes, ((Date) value).getTime());
    } else if (type == OType.DATE) {
      long dateValue;
      if (value instanceof Number) {
        dateValue = ((Number) value).longValue();
      } else
        dateValue = ((Date) value).getTime();
      dateValue = HelperClasses.convertDayToTimezone(ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
      OVarIntSerializer.write(bytes, dateValue / HelperClasses.MILLISEC_PER_DAY);
    } else if (type == OType.EMBEDDED) {
      if (value instanceof ODocumentSerializable) {
        ODocument cur = ((ODocumentSerializable) value).toDocument();
        cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
        serializeDoc(cur, bytes);
      } else {
        serializeDoc((ODocument) value, bytes);
      }
    } else if (type == OType.EMBEDDEDSET || type == OType.EMBEDDEDLIST) {
      if (value.getClass().isArray())
        serializeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), null);
      else
        serializeEmbeddedCollection(bytes, (Collection<?>) value, null);
    } else if (type == OType.DECIMAL) {
      BigDecimal decimalValue = (BigDecimal) value;
      int pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
      ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
    } else if (type == OType.BINARY) {
      HelperClasses.writeBinary(bytes, (byte[]) (value));
    } else if (type == OType.LINKSET || type == OType.LINKLIST) {
      Collection<OIdentifiable> ridCollection = (Collection<OIdentifiable>) value;
      HelperClasses.writeLinkCollection(bytes, ridCollection);
    } else if (type == OType.LINK) {
      if (!(value instanceof OIdentifiable))
        throw new OValidationException("Value '" + value + "' is not a OIdentifiable");

      HelperClasses.writeOptimizedLink(bytes, (OIdentifiable) value);
    } else if (type == OType.LINKMAP) {
      HelperClasses.writeLinkMap(bytes, (Map<Object, OIdentifiable>) value);
    } else if (type == OType.EMBEDDEDMAP) {
      writeEmbeddedMap(bytes, (Map<Object, Object>) value);
    } else if (type == OType.LINKBAG) {
      HelperClasses.writeRidBag(bytes, (ORidBag) value);
    } else if (type == OType.CUSTOM) {
      if (!(value instanceof OSerializableStream))
        value = new OSerializableWrapper((Serializable) value);
      HelperClasses.writeString(bytes, value.getClass().getName());
      HelperClasses.writeBinary(bytes, ((OSerializableStream) value).toStream());
    } else if (type == ODeltaDocumentFieldType.DELTA_RECORD) {
      ODocumentDelta deltaVal = (ODocumentDelta) value;
      serialize(deltaVal, bytes);
    }
  }

  protected OClass serializeClassName(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null)
      HelperClasses.writeString(bytes, clazz.getName());
    else
      writeEmptyString(bytes);
    return clazz;
  }

  private String deserializeClassName(BytesContainer bytes) {
    String className = HelperClasses.readString(bytes);
    return className;
  }

  private void serializeDoc(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = serializeClassName(document, bytes);
    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;
    final Set<Map.Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    for (Map.Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      if (!docEntry.exists())
        continue;
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(entry.getKey());
        if (prop != null && docEntry.type == prop.getType())
          docEntry.property = prop;
      }

      OTypeInterface valueType = getDocumentFieldType(docEntry);
      if (docEntry.property != null) {
        //write prop ID
        serializeValue(bytes, (docEntry.property.getId() + 1) * -1, OType.INTEGER);
        if (valueType == OType.ANY) {
          //write type if differs from ANY
          HelperClasses.writeType(bytes, valueType);
        }
      } else {

        if (valueType == OType.LINK && docEntry.value instanceof ODocument) {
          valueType = OType.EMBEDDED;
        }

        //write field name
        serializeValue(bytes, entry.getKey(), OType.STRING);

        //write type
        HelperClasses.writeType(bytes, valueType);
      }
      //write value
      serializeValue(bytes, docEntry.value, valueType);
    }
    //signal for end
    serializeValue(bytes, 0, OType.INTEGER);
  }

  private ODocument deserializeDoc(final BytesContainer bytes, ODocument owner) {
    String className = deserializeClassName(bytes);
    ODocument ret;
    if (className == null || className.equals("")) {
      ret = new ODocument();
    } else {
      ret = new ODocument(className);
    }

    boolean endSignal = false;
    while (!endSignal) {
      //read field name length
      int len = deserializeValue(bytes, OType.INTEGER, null);
      if (len == 0) {
        endSignal = true;
      } else {
        String fieldName;
        Object fieldValue;
        OTypeInterface fieldType;
        if (len < 0) {
          OGlobalProperty prop = HelperClasses.getGlobalProperty(ret, len);
          fieldName = prop.getName();
          fieldType = prop.getType();
          if (fieldType == OType.ANY) {
            //read type
            fieldType = HelperClasses.readDeltaDocumentType(bytes);
          }
        } else {
          //read rest of string
          fieldName = HelperClasses.stringFromBytes(bytes.bytes, bytes.offset, len);
          bytes.offset += len;
          //read type
          fieldType = HelperClasses.readDeltaDocumentType(bytes);
        }
        //read value
        fieldValue = deserializeValue(bytes, fieldType, ret);
        ret.field(fieldName, fieldValue);
      }
    }
    if (!ret.containsField(ODocumentSerializable.CLASS_NAME)) {
      ODocumentInternal.addOwner((ODocument) ret, owner);
    }
    return ret;
  }

  protected OTypeInterface getDocumentFieldType(final ODocumentEntry entry) {
    OTypeInterface type = entry.type;
    if (type == null) {
      final OProperty prop = entry.property;
      if (prop != null)
        type = prop.getType();
    }
    if (type == null || OType.ANY == type)
      type = ODeltaDocumentFieldType.getTypeByValue(entry.value);
    return type;
  }

  protected <T> T deserializeValue(final BytesContainer bytes, OTypeInterface type, final ODocument ownerDocument) {

    Object value = null;
    if (type == OType.INTEGER) {
      value = OVarIntSerializer.readAsInteger(bytes);
    } else if (type == OType.LONG) {
      value = OVarIntSerializer.readAsLong(bytes);
    } else if (type == OType.SHORT) {
      value = OVarIntSerializer.readAsShort(bytes);
    } else if (type == OType.STRING) {
      value = HelperClasses.readString(bytes);
    } else if (type == OType.DOUBLE) {
      value = Double.longBitsToDouble(HelperClasses.readLong(bytes));
    } else if (type == OType.FLOAT) {
      value = Float.intBitsToFloat(HelperClasses.readInteger(bytes));
    } else if (type == OType.BYTE) {
      value = HelperClasses.readByte(bytes);
    } else if (type == OType.BOOLEAN) {
      value = HelperClasses.readByte(bytes) == 1;
    } else if (type == OType.DATETIME) {
      value = new Date(OVarIntSerializer.readAsLong(bytes));
    } else if (type == OType.DATE) {
      long savedTime = OVarIntSerializer.readAsLong(bytes) * HelperClasses.MILLISEC_PER_DAY;
      savedTime = HelperClasses.convertDayToTimezone(TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
      value = new Date(savedTime);
    } else if (type == OType.EMBEDDED) {
      value = deserializeDoc(bytes, ownerDocument);
    } else if (type == OType.EMBEDDEDSET) {
      value = deserializeEmbeddedCollection(bytes, ownerDocument, false);
    } else if (type == OType.EMBEDDEDLIST) {
      value = deserializeEmbeddedCollection(bytes, ownerDocument, true);
    } else if (type == OType.LINKSET) {
      ORecordLazySet collectionSet = null;
      collectionSet = new ORecordLazySet(ownerDocument);
      value = HelperClasses.readLinkCollection(bytes, collectionSet, false);
    } else if (type == OType.LINKLIST) {
      ORecordLazyList collectionList = null;
      collectionList = new ORecordLazyList(ownerDocument);
      value = HelperClasses.readLinkCollection(bytes, collectionList, false);
    } else if (type == OType.BINARY) {
      value = HelperClasses.readBinary(bytes);
    } else if (type == OType.LINK) {
      value = HelperClasses.readOptimizedLink(bytes, false);
    } else if (type == OType.LINKMAP) {
      value = HelperClasses.readLinkMap(bytes, ownerDocument, false);
    } else if (type == OType.EMBEDDEDMAP) {
      value = readEmbeddedMap(bytes, ownerDocument);
    } else if (type == OType.DECIMAL) {
      value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
    } else if (type == OType.LINKBAG) {
      ORidBag bag = HelperClasses.readRidbag(bytes);
      bag.setOwner(ownerDocument);
      value = bag;
    } else if (type == ODeltaDocumentFieldType.DELTA_RECORD) {
      ODocumentDelta delta = deserialize(bytes);
      value = delta;
    } else if (type == OType.CUSTOM) {
      try {
        String className = HelperClasses.readString(bytes);
        Class<?> clazz = Class.forName(className);
        OSerializableStream stream = (OSerializableStream) clazz.newInstance();
        byte[] bytesRepresentation = HelperClasses.readBinary(bytes);
        stream.fromStream(bytesRepresentation);
        if (stream instanceof OSerializableWrapper)
          value = ((OSerializableWrapper) stream).getSerializable();
        else
          value = stream;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return (T) value;
  }

  @Override
  public void fromStream(ODocumentDelta delta, BytesContainer bytes) {
    deserialize(delta, bytes);
  }

  @Override
  public ODocumentDelta fromStream(BytesContainer bytes) {
    return deserialize(bytes);
  }

  private int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      OTypeInterface type = OType.STRING;
      HelperClasses.writeType(bytes, type);
      HelperClasses.writeString(bytes, entry.getKey().toString());
      final Object value = entry.getValue();
      if (value != null) {
        type = HelperClasses.getDeltaTypeFromValueEmbedded(value);
        if (type == OType.LINK && value instanceof ODocument) {
          type = OType.EMBEDDED;
        }
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }
        HelperClasses.writeType(bytes, type);
        serializeValue(bytes, value, type);
      } else {
        //signal for null value
        OByteSerializer.INSTANCE.serialize((byte) -1, bytes.bytes, bytes.alloc(1));
      }
    }

    return fullPos;
  }

  protected Map readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final Map result = new HashMap();

    for (int i = 0; i < size; i++) {
      OTypeInterface keyType = HelperClasses.readDeltaDocumentType(bytes);
      Object key = deserializeValue(bytes, keyType, document);
      byte typeId = HelperClasses.readByte(bytes);
      if (typeId != -1) {
        final OTypeInterface type = ODeltaDocumentFieldType.getFromId(typeId);
        Object value = deserializeValue(bytes, type, document);
        result.put(key, value);
      } else
        result.put(key, null);
    }
    return result;

  }

}
