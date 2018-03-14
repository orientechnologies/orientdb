package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Triple;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Tuple;
import java.util.*;
import java.util.Map.Entry;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.*;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;

public class ORecordSerializerBinaryV1 extends ORecordSerializerBinaryV0{          

  private enum Signal{
    CONTINUE,
    RETURN,
    RETURN_VALUE,
    NO_ACTION
  }
  
  private Tuple<Boolean, String> processLenLargerThanZeroDeserializePartial(final String[] iFields, final BytesContainer bytes, int len, byte[][] fields){
    boolean match = false;
    String fieldName = null;
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
    
    return new Tuple<>(match, fieldName);
  }
  
  private Tuple<Boolean, String> processLenSmallerThanZeroDeserializePartial(OGlobalProperty prop, final String[] iFields){    
    String fieldName = prop.getName();

    boolean matchField = false;
    for (String f : iFields) {
      if (fieldName.equals(f)) {
        matchField = true;
        break;
      }
    }
    
    return new Tuple<>(matchField, fieldName);
  }
  
  private Tuple<Signal, Triple<Integer, OType, String>> processLessThanZeroDeserializePartialFields(final ODocument document,
          final int len, final String[] iFields, final BytesContainer bytes){
    // LOAD GLOBAL PROPERTY BY ID
    final OGlobalProperty prop = getGlobalProperty(document, len);
    Tuple<Boolean, String> matchFieldName = processLenSmallerThanZeroDeserializePartial(prop, iFields);

    boolean matchField = matchFieldName.getFirstVal();
    String fieldName = matchFieldName.getSecondVal();

    if (!matchField) {
      // FIELD NOT INCLUDED: SKIP IT
      bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
      return new Tuple<>(Signal.CONTINUE, null);
    }

    Integer valuePos = readInteger(bytes);
    OType type = getTypeForLenLessThanZero(prop, bytes);
    Triple<Integer, OType, String> value = new Triple<>(valuePos, type, fieldName);
    return new Tuple<>(Signal.RETURN_VALUE, value);
  }
  
  @Override
   public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields){
    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i)
      fields[i] = iFields[i].getBytes();

    String fieldName;
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
        Tuple<Boolean, String> matchFieldName = processLenLargerThanZeroDeserializePartial(iFields, bytes, len, fields);
        boolean match = matchFieldName.getFirstVal();
        fieldName = matchFieldName.getSecondVal();
        
        if (!match) {
          // FIELD NOT INCLUDED: SKIP IT
          bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
          continue;
        }
        valuePos = readInteger(bytes);
        type = readOType(bytes);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        Tuple<Signal, Triple<Integer, OType, String>> actionSignal = processLessThanZeroDeserializePartialFields(document, len, iFields, bytes);
        switch (actionSignal.getFirstVal()){
          case CONTINUE:
            continue;            
          case RETURN_VALUE:
          default:
            valuePos = actionSignal.getSecondVal().getFirstVal();
            type = actionSignal.getSecondVal().getSecondVal();
            fieldName = actionSignal.getSecondVal().getThirdVal();
            break;
        }        
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
    
  
  @Override
  public void deserializePartialWithClassName(final ODocument document, final BytesContainer bytes, final String[] iFields) {
        
    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);    

    deserializePartial(document, bytes, iFields);
  }

  private boolean checkMatchForLargerThenZero(final BytesContainer bytes, final byte[] field, int len){
    boolean match = true;
    for (int j = 0; j < len; ++j)
      if (bytes.bytes[bytes.offset + j] != field[j]) {
        match = false;
        break;
      }
    
    return match;
  }
  
  private OType getTypeForLenLessThanZero(final OGlobalProperty prop, final BytesContainer bytes){
    final OType type;
    if (prop.getType() != OType.ANY)
      type = prop.getType();
    else
      type = readOType(bytes);
    
    return type;
  }
  
  private Tuple<Signal, OBinaryField> processLenLargerThanZeroDeserializeField(final BytesContainer bytes, final String iFieldName,
          final byte[] field, int len){
    if (iFieldName.length() == len) {
      boolean match = checkMatchForLargerThenZero(bytes, field, len);

      bytes.skip(len);
      final int valuePos = readInteger(bytes);
      final OType type = readOType(bytes);

      if (valuePos == 0)
        return new Tuple<>(Signal.RETURN_VALUE, null);

      if (!match)
        return new Tuple<>(Signal.CONTINUE, null);

      if (!getComparator().isBinaryComparable(type))
        return new Tuple<>(Signal.RETURN_VALUE, null);

      bytes.offset = valuePos;
      return new Tuple<>(Signal.RETURN_VALUE, new OBinaryField(iFieldName, type, bytes, null));
    }

    // SKIP IT
    bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
    return new Tuple<>(Signal.NO_ACTION, null);
  }
  
  private Tuple<Signal, OBinaryField> processLenLessThanZeroDeserializeField(int len, final OImmutableSchema _schema,
          final String iFieldName, final OClass iClass, final BytesContainer bytes){
    final int id = (len * -1) - 1;
    final OGlobalProperty prop = _schema.getGlobalPropertyById(id);
    if (iFieldName.equals(prop.getName())) {
      final int valuePos = readInteger(bytes);
      final OType type = getTypeForLenLessThanZero(prop, bytes);

      if (valuePos == 0 || !getComparator().isBinaryComparable(type))
        return new Tuple<>(Signal.RETURN_VALUE, null);

      bytes.offset = valuePos;

      final OProperty classProp = iClass.getProperty(iFieldName);
      return new Tuple<>(Signal.RETURN_VALUE, new OBinaryField(iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null));
    }
    bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
    return new Tuple<>(Signal.NO_ACTION, null);
  }
  
  @Override
  public OBinaryField deserializeField(final BytesContainer bytes, final OClass iClass, final String iFieldName){
    final byte[] field = iFieldName.getBytes();

    final OMetadataInternal metadata = (OMetadataInternal) ODatabaseRecordThreadLocal.instance().get().getMetadata();
    final OImmutableSchema _schema = metadata.getImmutableSchemaSnapshot();

    while (true) {
      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len == 0) {
        // SCAN COMPLETED, NO FIELD FOUND
        return null;
      } else if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        Tuple<Signal, OBinaryField> actionSignal = processLenLargerThanZeroDeserializeField(bytes, iFieldName, field, len);
        switch(actionSignal.getFirstVal()){
          case RETURN_VALUE:
            return actionSignal.getSecondVal();
          case CONTINUE:            
          case NO_ACTION:
          default:
            break;
        }
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        Tuple<Signal, OBinaryField> actionSignal = processLenLessThanZeroDeserializeField(len, _schema, iFieldName, iClass, bytes);
        switch(actionSignal.getFirstVal()){
          case RETURN_VALUE:
            return actionSignal.getSecondVal();
          case CONTINUE:            
          case NO_ACTION:
          default:
            break;
        }
      }
    }
  }
  
  @Override
  public OBinaryField deserializeFieldWithClassName(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    // SKIP CLASS NAME    
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);    

    return deserializeField(bytes, iClass, iFieldName);
  }

  @Override
  public void deserialize(final ODocument document, final BytesContainer bytes) {
    int last = 0;
    String fieldName;
    int valuePos;
    OType type;
    while (true) {
      OGlobalProperty prop;
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
        type = getTypeForLenLessThanZero(prop, bytes);
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
      } 
      else 
        ODocumentInternal.rawField(document, fieldName, null, null);      
    }

    ORecordInternal.clearSource(document);

    if (last > bytes.offset) {
      bytes.offset = last;
    }
  }

  @Override
  public void deserializeWithClassName(final ODocument document, final BytesContainer bytes) {

    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    deserialize(document, bytes);
  }

  @Override
  public String[] getFieldNames(ODocument reference, final BytesContainer bytes,  boolean readClassName) {
   // SKIP CLASS NAME
   if (readClassName){
     final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
     bytes.skip(classNameLen);
   }

   final List<String> result = new ArrayList<>();

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

  private int serializeAllocateSpace(final BytesContainer bytes,
          final Entry<String, ODocumentEntry> values[], final Map<String, OProperty> props,
          final Set<Entry<String, ODocumentEntry>> fields, final int[] pos) {
    int i = 0;
    
    for (Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      if (!docEntry.exist()) {
        continue;
      }
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(entry.getKey());
        if (prop != null && docEntry.type == prop.getType()) {
          docEntry.property = prop;
        }
      }

      if (docEntry.property != null) {
        int id = docEntry.property.getId();
        OVarIntSerializer.write(bytes, (id + 1) * -1);
        if (docEntry.property.getType() != OType.ANY) {
          pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE);
        } else {
          pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
        }
      } else {
        writeString(bytes, entry.getKey());
        pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
      }
      values[i] = entry;
      i++;
    }
    return i;
  }
  
  private void serializeWriteValues(final BytesContainer bytes, final ODocument document,
          int size, final Entry<String, ODocumentEntry> values[], final int[] pos){
    for (int i = 0; i < size; i++) {
      int pointer;
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
  
  private void serializeDocument(final ODocument document, final BytesContainer bytes, final OClass clazz){
    
    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;

    final Set<Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    final int[] pos = new int[fields.size()];    

    final Entry<String, ODocumentEntry> values[] = new Entry[fields.size()];
    int i = serializeAllocateSpace(bytes, values, props, fields, pos);
    writeEmptyString(bytes);    

    serializeWriteValues(bytes, document, i, values, pos);
  }
  
  @Override
  public void serializeWithClassName(final ODocument document, final BytesContainer bytes, final boolean iClassOnly){
    final OClass clazz = serializeClass(document, bytes, true);
    if (iClassOnly) {
      writeEmptyString(bytes);
      return;
    }
    serializeDocument(document, bytes, clazz);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void serialize(final ODocument document, final BytesContainer bytes, final boolean iClassOnly) {
    final OClass clazz = serializeClass(document, bytes, false);
    if (iClassOnly) {
      writeEmptyString(bytes);
      return;
    }
    serializeDocument(document, bytes, clazz);
  }  

  protected OClass serializeClass(final ODocument document, final BytesContainer bytes, boolean serializeClassName) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (serializeClassName){
      if (clazz != null && document.isEmbedded())
        writeString(bytes, clazz.getName());
      else
        writeEmptyString(bytes);
    }
    return clazz;
  }
  
  @Override
  public boolean isSerializingClassNameByDefault() {
    return false;
  }
  
  @Override
  public <RET> RET deserializeFieldTyped(BytesContainer bytes, String iFieldName, boolean isEmbedded, int serializerVersion){    
    if (isEmbedded){
      skipClassName(bytes);
    }
    return deserializeFieldTypedLoopAndReturn(bytes, iFieldName, serializerVersion);
  }
  
  @Override
  public boolean isSerializingClassNameForEmbedded() {
    return true;
  }
  
  @Override
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
        serializeWithClassName(cur, bytes, false);
      } else {
        serializeWithClassName((ODocument) value, bytes, false);
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
      ORidBag ridbag = ((ORidBag) value);
      pointer = writeRidBag(bytes, ridbag);
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
  
  @Override
  protected Object deserializeValue(final BytesContainer bytes, final OType type, final ODocument ownerDocument, boolean embeddedAsDocument, 
          int valueLengthInBytes, int serializerVersion, boolean justRunThrough) {
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
      if (justRunThrough){
        int length = OVarIntSerializer.readAsInteger(bytes);
        bytes.skip(length);
      }
      else{
        value = readString(bytes);
      }
      break;
    case DOUBLE:
      if (justRunThrough){
        bytes.skip(OLongSerializer.LONG_SIZE);
      }
      else{
        value = Double.longBitsToDouble(readLong(bytes));
      }
      break;
    case FLOAT:
      if (justRunThrough){
        bytes.skip(OIntegerSerializer.INT_SIZE);
      }
      else{
        value = Float.intBitsToFloat(readInteger(bytes));
      }
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
      if (!justRunThrough){
        savedTime = convertDayToTimezone(TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
        value = new Date(savedTime);
      }
      break;
    case EMBEDDED:
      if (embeddedAsDocument){
        value = deserializeEmbeddedAsDocument(bytes, ownerDocument);
      }
      else{
        value = deserializeEmbeddedAsBytes(bytes, valueLengthInBytes, serializerVersion);        
      }      
      break;
    case EMBEDDEDSET:
      if (embeddedAsDocument){
        value = readEmbeddedSet(bytes, ownerDocument);
      }
      else{
        value = deserializeEmbeddedCollectionAsCollectionOfBytes(bytes, serializerVersion);        
      }      
      break;
    case EMBEDDEDLIST:
      if (embeddedAsDocument){        
        value = readEmbeddedList(bytes, ownerDocument);        
      }
      else{
        value = deserializeEmbeddedCollectionAsCollectionOfBytes(bytes, serializerVersion);        
      }
      break;
    case LINKSET:
      value = readLinkCollection(bytes, new ORecordLazySet(ownerDocument));
      break;
    case LINKLIST:
      value = readLinkCollection(bytes, new ORecordLazyList(ownerDocument));
      break;
    case BINARY:
      if (justRunThrough){
        int len = OVarIntSerializer.readAsInteger(bytes);
        bytes.skip(len);
      }
      else{
        value = readBinary(bytes);
      }
      break;
    case LINK:      
      value = readOptimizedLink(bytes);      
      break;
    case LINKMAP:
      value = readLinkMap(bytes, ownerDocument);
      break;
    case EMBEDDEDMAP:
      if (embeddedAsDocument){
        value = readEmbeddedMap(bytes, ownerDocument);
      }
      else{
        value = deserializeEmbeddedMapAsMapOfBytes(bytes, serializerVersion);                
      }
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
    case TRANSIENT:
      break;
    case CUSTOM:
      try {
        String className = readString(bytes);
        Class<?> clazz = Class.forName(className);
        OSerializableStream stream = (OSerializableStream) clazz.newInstance();
        byte[] bytesRepresentation = readBinary(bytes);
        if (embeddedAsDocument){
          stream.fromStream(bytesRepresentation);
          if (stream instanceof OSerializableWrapper)
            value = ((OSerializableWrapper) stream).getSerializable();
          else
            value = stream;
        }
        else{
          OResultBinary retVal = new OResultBinary(bytesRepresentation, 0, bytesRepresentation.length, serializerVersion);
          return retVal;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      break;
    case ANY:
      break;

    }
    return value;
  }
  
  private static int getHighLevelDocClusterId(ORidBag ridbag) {
    ORidBagDelegate delegate = ridbag.getDelegate();
    ORecordElement owner = delegate.getOwner();
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner != null)
      return ((OIdentifiable) owner).getIdentity().getClusterId();

    return -1;
  }
  
  private static void writeEmbeddedRidbag(BytesContainer bytes, ORidBag ridbag){
    OVarIntSerializer.write(bytes, ridbag.size());
    Object[] entries = ((OEmbeddedRidBag)ridbag.getDelegate()).getEntries();
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    for (int i = 0; i < entries.length; i++) {
      Object entry = entries[i];
      if (entry instanceof OIdentifiable){
        OIdentifiable itemValue = (OIdentifiable)entry;
        final ORID rid = itemValue.getIdentity();
        if (db != null && !db.isClosed() && db.getTransaction().isActive() && !itemValue.getIdentity().isPersistent()) {
          itemValue = db.getTransaction().getRecord(itemValue.getIdentity());          
        }
        if (itemValue == null){
          //should never happen
          String errorMessage = "Found null entry in ridbag with rid=" + rid;
          OSerializationException exc = new OSerializationException(errorMessage);
          OLogManager.instance().error(ORecordSerializerBinaryV1.class, errorMessage, null);
          throw exc;
        }
        else{
          entries[i] = itemValue.getIdentity();
          writeLinkOptimized(bytes, itemValue);
        }
      }
    }
  }
  
  private static void writeSBTreeRidbag(BytesContainer bytes, ORidBag ridbag, UUID ownerUuid){
    ((OSBTreeRidBag)ridbag.getDelegate()).applyNewEntries();
      
    OBonsaiCollectionPointer pointer = ridbag.getPointer();

    final ORecordSerializationContext context;
    boolean remoteMode = ODatabaseRecordThreadLocal.instance().get().getStorage() instanceof OStorageProxy;
    if (remoteMode) {
      context = null;
    } else
      context = ORecordSerializationContext.getContext();

    if (pointer == null && context != null) {      
      final int clusterId = getHighLevelDocClusterId(ridbag);
      assert clusterId > -1;
      pointer = ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager()
          .createSBTree(clusterId, ownerUuid);      
    }

    ((OSBTreeRidBag)ridbag.getDelegate()).setCollectionPointer(pointer);

    OVarIntSerializer.write(bytes, pointer.getFileId());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
    OVarIntSerializer.write(bytes, ridbag.size());

    if (context != null){
      ((OSBTreeRidBag)ridbag.getDelegate()).handleContextSBTree(context, pointer);
      OVarIntSerializer.write(bytes, 0);
    }
    else{
      OVarIntSerializer.write(bytes, 0);

      //removed changes serialization
    }
  }
  
  private static int writeRidBag(BytesContainer bytes, ORidBag ridbag) {    
    ridbag.checkAndConvert();
    
    UUID ownerUuid = ridbag.getTemporaryId();
    
    int positionOffset = bytes.offset;
    final OSBTreeCollectionManager sbTreeCollectionManager = ODatabaseRecordThreadLocal.instance().get()
        .getSbTreeCollectionManager();
    UUID uuid = null;
    if (sbTreeCollectionManager != null)
      uuid = sbTreeCollectionManager.listenForChanges(ridbag);
    
    byte configByte = 0;
    if (ridbag.isEmbedded())
      configByte |= 1;

    if (uuid != null)
      configByte |= 2;
    
    //alloc will move offset and do skip
    int posForWrite = bytes.alloc(OByteSerializer.BYTE_SIZE);
    OByteSerializer.INSTANCE.serialize(configByte, bytes.bytes, posForWrite);    
    
    //removed serializing UUID
    
    if (ridbag.isEmbedded()) {
      writeEmbeddedRidbag(bytes, ridbag);
    } else { 
      writeSBTreeRidbag(bytes, ridbag, ownerUuid);
    }
    return positionOffset;
  }
  
  private static ORidBag readRidbag(BytesContainer bytes){
    byte configByte = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset++);    
    boolean isEmbedded = (configByte & 1) != 0;
//    boolean hasUUID = (configByte & 2) != 0;
    
    UUID uuid = null;
    //removed deserializing UUID
    
    ORidBag ridbag = null;
    if (isEmbedded){
      ridbag = new ORidBag();
      int size = OVarIntSerializer.readAsInteger(bytes);
      ridbag.getDelegate().setSize(size);
      for (int i = 0; i < size; i++){
        OIdentifiable record = readLinkOptimizedEmbedded(bytes);
        if (record == null){
          //should never happen
          String errorMessage = "Deserialized null object during ridbag deserialization";
          OSerializationException exc = new OSerializationException("");
          OLogManager.instance().error(ORecordSerializerBinaryV1.class, errorMessage, null);
          throw exc;
        }
        else
          ((OEmbeddedRidBag)ridbag.getDelegate()).addEntry(record);
      }
    }
    else{
      long fileId = OVarIntSerializer.readAsLong(bytes);
      long pageIndex = OVarIntSerializer.readAsLong(bytes);
      int pageOffset = OVarIntSerializer.readAsInteger(bytes);
      //read bag size
      OVarIntSerializer.readAsInteger(bytes);            
      
      OBonsaiCollectionPointer pointer = null;
      if (fileId != -1)
        pointer = new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
      
      Map<OIdentifiable, Change> changes = new HashMap<>();
      
      int changesSize = OVarIntSerializer.readAsInteger(bytes);
      for (int i = 0; i < changesSize; i++){        
        OIdentifiable recId = readLinkOptimizedSBTree(bytes);                        
        Change change = deserializeChange(bytes);        
        changes.put(recId, change);
      }
      
      ridbag = new ORidBag(pointer, changes, uuid);
      ridbag.getDelegate().setSize(-1);
    }
    return ridbag;
  }
    
  private static Change deserializeChange(BytesContainer bytes){
    byte type = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OByteSerializer.BYTE_SIZE);
    int change = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OIntegerSerializer.INT_SIZE);
    return ChangeSerializationHelper.createChangeInstance(type, change);
  }
  
  private static OIdentifiable readLinkOptimizedEmbedded(final BytesContainer bytes) {
    ORID rid = new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    OIdentifiable identifiable = null;
    if (rid.isTemporary())
      identifiable = rid.getRecord();

    if (identifiable == null)
      identifiable = rid;
    
    return identifiable;
  }
  
  private static OIdentifiable readLinkOptimizedSBTree(final BytesContainer bytes) {
    ORID rid = new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    final OIdentifiable identifiable;
    if (rid.isTemporary() && rid.getRecord() != null)
      identifiable = rid.getRecord();
    else
      identifiable = rid;
    return identifiable;
  }
  
  private static void writeLinkOptimized(final BytesContainer bytes, OIdentifiable link){
    ORID id = link.getIdentity();
    OVarIntSerializer.write(bytes, id.getClusterId());
    OVarIntSerializer.write(bytes, id.getClusterPosition());
  }
  
}
