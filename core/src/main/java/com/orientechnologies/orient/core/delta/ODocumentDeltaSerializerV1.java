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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readOType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OSerializableWrapper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 *
 * @author mdjurovi
 */
public class ODocumentDeltaSerializerV1 implements ODocumentDeltaSerializer{

//  private ORecordSerializerBinaryV1 documentSerializer = new ORecordSerializerBinaryV1();
  
  @Override
  public byte[] toStream(ODocumentDelta delta) {
    BytesContainer bytes = new BytesContainer();
    for (Map.Entry<String, Object> entry : delta.fields.entrySet()){
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();      
      
      //serialize field name
      serializeValue(bytes, fieldName, OType.STRING);
      
      //serialize field type
      OType type = OType.getTypeByClass(fieldValue.getClass());
      HelperClasses.writeType(bytes, type);
      //serialize field value
      serializeValue(bytes, fieldValue, type);
    }
    
    serializeValue(bytes, -1, OType.INTEGER);
    return bytes.fitBytes();
  }
  
  private void serialize(ODocumentDelta deltaDoc, BytesContainer bytes){    
    for (Map.Entry<String, Object> entry : deltaDoc.fields.entrySet()){
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();      
      
      //serialize field name
      serializeValue(bytes, fieldName, OType.STRING);
      
      //serialize field type
      OType type = OType.getTypeByClass(fieldValue.getClass());
      HelperClasses.writeType(bytes, type);
      //serialize field value
      serializeValue(bytes, fieldValue, type);
    }
    serializeValue(bytes, -1, OType.INTEGER);
  }
  
  private ODocumentDelta deserialize(BytesContainer bytes){    
    ODocumentDelta ret = new ODocumentDelta();
    boolean endReached = false;    
    while (!endReached){
      int fieldNameLength = OVarIntSerializer.readAsInteger(bytes);
      if (fieldNameLength > -1){
        if (fieldNameLength > 0){                    
          String fieldName = HelperClasses.stringFromBytes(bytes.bytes, bytes.offset, fieldNameLength);
          bytes.offset += fieldNameLength;
          OType type = HelperClasses.readType(bytes);
          Object value = deserializeValue(bytes, type, null);
          ret.field(fieldName, value);
        }        
      }
      else{
        endReached = true;
      }
    }
    return ret;
  }
  
  protected int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }   
  
  private int serializeEmbeddedCollection(final BytesContainer bytes, final Collection<?> value, final OType linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.    
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        HelperClasses.writeOType(bytes, bytes.alloc(1), OType.ANY);
        continue;
      }
      OType type;
      if (linkedType == null || linkedType == OType.ANY)
        type = HelperClasses.getTypeFromValueEmbedded(itemValue);
      else
        type = linkedType;
      if (type != null) {
        HelperClasses.writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, itemValue, type);
      } else {
        throw new OSerializationException(
            "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
      }
    }
    return pos;
  }
  
  private Collection<?> deserializeEmbeddedCollection(final BytesContainer bytes, final ODocument ownerDocument, boolean isList){
    final int items = OVarIntSerializer.readAsInteger(bytes);    
    
    final OTrackedMultiValue found;
    if (isList){
      found = deserializeEmbeddedList(bytes, ownerDocument);
    }
    else{
      found = deserializeEmbeddedSet(bytes, ownerDocument);
    }    
    return (Collection<?>)found;
  }
  
  protected OTrackedSet deserializeEmbeddedSet(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);    
    
    final OTrackedSet found = new OTrackedSet<>(ownerDocument);

    found.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {

      for (int i = 0; i < items; i++) {
        OType itemType = readOType(bytes, false);
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
  
  private OTrackedList deserializeEmbeddedList(final BytesContainer bytes, final ODocument ownerDocument){
    final int items = OVarIntSerializer.readAsInteger(bytes);    
    
    final OTrackedList found = new OTrackedList<>(ownerDocument);  

    found.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {

      for (int i = 0; i < items; i++) {
        OType itemType = HelperClasses.readOType(bytes, false);
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
  
  private void serializeValue(final BytesContainer bytes, Object value, final OType type) {        
    switch (type) {
    case INTEGER:
    case LONG:
    case SHORT:
      OVarIntSerializer.write(bytes, ((Number) value).longValue());
      break;
    case STRING:
      HelperClasses.writeString(bytes, value.toString());
      break;
    case DOUBLE:
      long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      int pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
      break;
    case FLOAT:
      int fg = Float.floatToIntBits(((Number) value).floatValue());
      pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
      break;
    case BYTE:
      writeByte(bytes, ((Number) value).byteValue());
      break;
    case BOOLEAN:
      byte val = ((Boolean) value) ? (byte) 1 : (byte) 0;
      writeByte(bytes, val);
      break;
    case DATETIME:
      if (value instanceof Number) {
        OVarIntSerializer.write(bytes, ((Number) value).longValue());
      } else
        OVarIntSerializer.write(bytes, ((Date) value).getTime());
      break;
    case DATE:
      long dateValue;
      if (value instanceof Number) {
        dateValue = ((Number) value).longValue();
      } else
        dateValue = ((Date) value).getTime();
      dateValue = HelperClasses.convertDayToTimezone(ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
      OVarIntSerializer.write(bytes, dateValue / HelperClasses.MILLISEC_PER_DAY);
      break;
    case EMBEDDED:      
      if (value instanceof ODocumentSerializable) {
        ODocument cur = ((ODocumentSerializable) value).toDocument();
        cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
        serializeDoc(cur, bytes);
      } else {
        serializeDoc((ODocument) value, bytes);
      }
      break;
    case EMBEDDEDSET:
    case EMBEDDEDLIST:
      if (value.getClass().isArray())
        serializeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), null);
      else
        serializeEmbeddedCollection(bytes, (Collection<?>) value, null);
      break;
    case DECIMAL:
      BigDecimal decimalValue = (BigDecimal) value;
      pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
      ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
      break;
    case BINARY:
      HelperClasses.writeBinary(bytes, (byte[]) (value));
      break;
    case LINKSET:
    case LINKLIST:
      Collection<OIdentifiable> ridCollection = (Collection<OIdentifiable>) value;
      HelperClasses.writeLinkCollection(bytes, ridCollection);
      break;
    case LINK:
      if (!(value instanceof OIdentifiable))
        throw new OValidationException("Value '" + value + "' is not a OIdentifiable");

      HelperClasses.writeOptimizedLink(bytes, (OIdentifiable) value);
      break;
    case LINKMAP:
      HelperClasses.writeLinkMap(bytes, (Map<Object, OIdentifiable>) value);
      break;
    case EMBEDDEDMAP:
      writeEmbeddedMap(bytes, (Map<Object, Object>) value);
      break;
    case LINKBAG:
      writeRidBag(bytes, (ORidBag) value);
      break;
    case CUSTOM:
      if (!(value instanceof OSerializableStream))
        value = new OSerializableWrapper((Serializable) value);
      HelperClasses.writeString(bytes, value.getClass().getName());
      HelperClasses.writeBinary(bytes, ((OSerializableStream) value).toStream());
      break;
    case DELTA_RECORD:      
      ODocumentDelta deltaVal = (ODocumentDelta)value;
      serialize(deltaVal, bytes);
      break;
    case TRANSIENT:
      break;
    case ANY:
      break;
    }    
  }
  
  protected OClass serializeClass(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null)
      HelperClasses.writeString(bytes, clazz.getName());
    else
      writeEmptyString(bytes);
    return clazz;
  }    
  
  private void serializeDoc(final ODocument document, final BytesContainer bytes) {

    final OClass clazz = serializeClass(document, bytes);
    
    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;

    final Set<Map.Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    final Map.Entry<String, ODocumentEntry> values[] = new Map.Entry[fields.size()];
    for (Map.Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      if (!docEntry.exist())
        continue;
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(entry.getKey());
        if (prop != null && docEntry.type == prop.getType())
          docEntry.property = prop;
      }

      OType valueType = getDocumentFieldType(docEntry);
      if (docEntry.property != null) {
        //write prop ID
        serializeValue(bytes, (docEntry.property.getId() + 1) * -1, OType.INTEGER);
        if (valueType == OType.ANY){
          //write type if differs from ANY
          OVarIntSerializer.write(bytes, valueType.getId());
        }
      } else {
        //write type
        serializeValue(bytes, valueType.getId(), OType.INTEGER);
        //write field name
        serializeValue(bytes, entry.getKey(), OType.STRING);        
      }
      //write value
      serializeValue(bytes, values, valueType);
    }
    //signal for end
    serializeValue(bytes, 0, OType.INTEGER);
  }
  
  private ODocument deserializeDoc(final BytesContainer bytes, ODocument owner) {
    ODocument ret = new ODocument();
    boolean endSignal = false;
    while (!endSignal){
      //read field name length
      int len = deserializeValue(bytes, OType.INTEGER, null);
      if (len == 0){
        endSignal = true;
      }
      else{
        String fieldName;
        Object fieldValue;
        OType fieldType;
        if (len < 0){
          OGlobalProperty prop = HelperClasses.getGlobalProperty(ret, len);
          fieldName = prop.getName();
          fieldType = prop.getType();
          if (fieldType == OType.ANY){
            //read type
            fieldType = OType.getById((byte)deserializeValue(bytes, OType.INTEGER, null));
          }                    
        }
        else{
          //read rest of string
          fieldName = HelperClasses.stringFromBytes(bytes.bytes, bytes.offset, len);
          bytes.offset += len;
          //read type
          fieldType = OType.getById((byte)deserializeValue(bytes, OType.INTEGER, null));                              
        }
        //read value
        fieldValue = deserializeValue(bytes, fieldType, ret);
        ret.field(fieldName, fieldValue);
      }
    }
    if (!ret.containsField(ODocumentSerializable.CLASS_NAME)){
      ODocumentInternal.addOwner((ODocument) ret, owner);
    }
    return ret;
  }
  
  private void writeByte(BytesContainer bytes, byte val){
    int pos = bytes.alloc(OByteSerializer.BYTE_SIZE);
    OByteSerializer.INSTANCE.serialize(val, bytes.bytes, pos);
  }
  
  protected OType getDocumentFieldType(final ODocumentEntry entry) {
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
  
  protected <T> T deserializeValue(final BytesContainer bytes, final OType type, final ODocument ownerDocument) {
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
      value = HelperClasses.readString(bytes);      
      break;
    case DOUBLE:      
      value = Double.longBitsToDouble(HelperClasses.readLong(bytes));      
      break;
    case FLOAT:      
      value = Float.intBitsToFloat(HelperClasses.readInteger(bytes));      
      break;
    case BYTE:      
      value = HelperClasses.readByte(bytes);
      break;
    case BOOLEAN:      
      value = HelperClasses.readByte(bytes) == 1;
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
      value = deserializeDoc(bytes, ownerDocument);
      break;
    case EMBEDDEDSET:      
      value = deserializeEmbeddedCollection(bytes, ownerDocument, false);      
      break;
    case EMBEDDEDLIST:      
      value = deserializeEmbeddedCollection(bytes, ownerDocument, true);      
      break;
    case LINKSET:
      ORecordLazySet collectionSet = null;      
      collectionSet = new ORecordLazySet(ownerDocument);      
      value = HelperClasses.readLinkCollection(bytes, collectionSet, false);
      break;
    case LINKLIST:
      ORecordLazyList collectionList = null;      
      collectionList = new ORecordLazyList(ownerDocument);      
      value = HelperClasses.readLinkCollection(bytes, collectionList, false);
      break;
    case BINARY:      
      value = HelperClasses.readBinary(bytes);      
      break;
    case LINK:
      value = HelperClasses.readOptimizedLink(bytes, false);
      break;
    case LINKMAP:
      value = readLinkMap(bytes, ownerDocument, false);
      break;
    case EMBEDDEDMAP:      
      value = readEmbeddedMap(bytes, ownerDocument);      
      break;
    case DECIMAL:
      value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
      break;
    case LINKBAG:
      ORidBag bag = readRidbag(bytes);
      bag.setOwner(ownerDocument);
      value = bag;
      break;
    case DELTA_RECORD:
      ODocumentDelta delta = deserialize(bytes);
      value = delta;
      break;
    case TRANSIENT:
      break;
    case CUSTOM:
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
      break;
    case ANY:
      break;

    }
    return (T)value;
  }
    

  @Override
  public void fromStream(BytesContainer bytes, ODocumentDelta toDoc) {
    boolean endReached = false;    
    while (!endReached){
      int fieldNameLength = OVarIntSerializer.readAsInteger(bytes);
      if (fieldNameLength > -1){
        if (fieldNameLength > 0){                    
          String fieldName = HelperClasses.stringFromBytes(bytes.bytes, bytes.offset, fieldNameLength);
          bytes.offset += fieldNameLength;
          OType type = HelperClasses.readType(bytes);
          Object value = deserializeValue(bytes, type, null);
          toDoc.field(fieldName, value);
        }        
      }
      else{
        endReached = true;
      }
    }
  }
    
  
}
