package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Triple;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Tuple;
import java.util.*;
import java.util.Map.Entry;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.*;

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
     
}
