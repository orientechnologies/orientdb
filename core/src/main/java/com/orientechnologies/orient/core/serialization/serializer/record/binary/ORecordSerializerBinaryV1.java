package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MapRecordInfo;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Triple;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Tuple;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.*;

public class ORecordSerializerBinaryV1 implements ODocumentSerializer {
  private final OBinaryComparatorV0 comparator = new OBinaryComparatorV0();

  private enum Signal {
    CONTINUE, RETURN, RETURN_VALUE, NO_ACTION
  }

  private Tuple<Boolean, String> processNamedFieldInDeserializePartial(final String[] iFields, final BytesContainer bytes, int len,
      byte[][] fields) {
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
          match = true;
          break;
        }
      }
    }

    //field name is used only if match exists
    if (match)
      fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
    bytes.skip(len);
    return new Tuple<>(match, fieldName);
  }

  private Tuple<Boolean, String> checkIfPropertyNameMatchSome(OGlobalProperty prop, final String[] iFields) {
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

  private Triple<Signal, Triple<Integer, OType, String>, Integer> processPropertyFiledInDeserializePartial(final ODocument document,
      final int len, final String[] iFields, final BytesContainer bytes, int cumulativeLength, int headerStart, int headerLength) {
    // LOAD GLOBAL PROPERTY BY ID
    final OGlobalProperty prop = getGlobalProperty(document, len);
    Tuple<Boolean, String> matchFieldName = checkIfPropertyNameMatchSome(prop, iFields);

    boolean matchField = matchFieldName.getFirstVal();
    String fieldName = matchFieldName.getSecondVal();

    int fieldLength = OVarIntSerializer.readAsInteger(bytes);
    OType type = getPropertyTypeFromStream(prop, bytes);

    if (!matchField) {
      return new Triple<>(Signal.CONTINUE, null, cumulativeLength + fieldLength);
    }

    int valuePos;
    if (fieldLength == 0) {
      valuePos = 0;
    } else {
      valuePos = cumulativeLength + headerStart + headerLength;
    }
    Triple<Integer, OType, String> value = new Triple<>(valuePos, type, fieldName);
    return new Triple<>(Signal.RETURN_VALUE, value, cumulativeLength + fieldLength);
  }

  public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields) {
    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]            
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i) {
      fields[i] = iFields[i].getBytes();
    }

    String fieldName;
    int valuePos;
    OType type;
    int unmarshalledFields = 0;

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int cumulativeLength = 0;

    while (true) {
      if (bytes.offset >= headerStart + headerLength)
        break;

      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        Tuple<Boolean, String> matchFieldName = processNamedFieldInDeserializePartial(iFields, bytes, len, fields);
        boolean match = matchFieldName.getFirstVal();
        fieldName = matchFieldName.getSecondVal();
        Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        int fieldLength = pointerAndType.getFirstVal();

        if (!match) {
          // FIELD NOT INCLUDED: SKIP IT
          cumulativeLength += fieldLength;
          continue;
        }

        type = pointerAndType.getSecondVal();
        if (fieldLength == 0) {
          valuePos = 0;
        } else {
          valuePos = headerStart + headerLength + cumulativeLength;
        }
        cumulativeLength += fieldLength;
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        Triple<Signal, Triple<Integer, OType, String>, Integer> actionSignal = processPropertyFiledInDeserializePartial(document,
            len, iFields, bytes, cumulativeLength, headerStart, headerLength);
        cumulativeLength = actionSignal.getThirdVal();
        switch (actionSignal.getFirstVal()) {
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

  private boolean checkMatchForLargerThenZero(final BytesContainer bytes, final byte[] field, int len) {
    if (field.length != len) {
      return false;
    }
    boolean match = true;
    for (int j = 0; j < len; ++j)
      if (bytes.bytes[bytes.offset + j] != field[j]) {
        match = false;
        break;
      }

    return match;
  }

  private OType getPropertyTypeFromStream(final OGlobalProperty prop, final BytesContainer bytes) {
    final OType type;
    if (prop.getType() != OType.ANY)
      type = prop.getType();
    else
      type = readOType(bytes, false);

    return type;
  }

  private Triple<Signal, OBinaryField, Integer> processNamedFieldDeserializeField(final BytesContainer bytes,
      final String iFieldName, final byte[] field, int len, Integer cumulativeLength, int headerStart, int headerLength) {

    boolean match = checkMatchForLargerThenZero(bytes, field, len);

    bytes.skip(len);
    Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
    final int fieldLength = pointerAndType.getFirstVal();
    final OType type = pointerAndType.getSecondVal();

    if (fieldLength == 0)
      return new Triple<>(Signal.RETURN_VALUE, null, cumulativeLength);

    if (!match)
      return new Triple<>(Signal.CONTINUE, null, cumulativeLength + fieldLength);

    if (!getComparator().isBinaryComparable(type))
      return new Triple<>(Signal.RETURN_VALUE, null, cumulativeLength + fieldLength);

    int valuePos = headerStart + headerLength + cumulativeLength;
    bytes.offset = valuePos;
    return new Triple<>(Signal.RETURN_VALUE, new OBinaryField(iFieldName, type, bytes, null), cumulativeLength + fieldLength);
  }

  private Triple<Signal, OBinaryField, Integer> processPropertyDeserializeField(int len, final OImmutableSchema schema,
      final String iFieldName, final OClass iClass, final BytesContainer bytes, int cumulativeLength, int headerStart,
      int headerLength) {
    final int id = (len * -1) - 1;
    final OGlobalProperty prop = schema.getGlobalPropertyById(id);
    final int fieldLength = OVarIntSerializer.readAsInteger(bytes);
    final OType type;
    type = getPropertyTypeFromStream(prop, bytes);

    if (!iFieldName.equals(prop.getName())) {
      return new Triple<>(Signal.NO_ACTION, null, cumulativeLength + fieldLength);
    }

    int valuePos = headerStart + headerLength + cumulativeLength;

    if (fieldLength == 0 || valuePos == 0 || !getComparator().isBinaryComparable(type))
      return new Triple<>(Signal.RETURN_VALUE, null, cumulativeLength + fieldLength);

    bytes.offset = valuePos;

    final OProperty classProp = iClass.getProperty(iFieldName);
    return new Triple<>(Signal.RETURN_VALUE,
        new OBinaryField(iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null),
        cumulativeLength + fieldLength);
  }

  public OBinaryField deserializeField(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    final byte[] field = iFieldName.getBytes();

    final OMetadataInternal metadata = ODatabaseRecordThreadLocal.instance().get().getMetadata();
    final OImmutableSchema _schema = metadata.getImmutableSchemaSnapshot();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int cumulativeLength = 0;

    while (true) {
      if (bytes.offset >= headerStart + headerLength)
        return null;

      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        Triple<Signal, OBinaryField, Integer> actionSignal = processNamedFieldDeserializeField(bytes, iFieldName, field, len,
            cumulativeLength, headerStart, headerLength);
        cumulativeLength = actionSignal.getThirdVal();
        switch (actionSignal.getFirstVal()) {
        case RETURN_VALUE:
          return actionSignal.getSecondVal();
        case CONTINUE:
        case NO_ACTION:
        default:
          break;
        }
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        Triple<Signal, OBinaryField, Integer> actionSignal = processPropertyDeserializeField(len, _schema, iFieldName, iClass,
            bytes, cumulativeLength, headerStart, headerLength);
        cumulativeLength = actionSignal.getThirdVal();
        switch (actionSignal.getFirstVal()) {
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

  public OBinaryField deserializeFieldWithClassName(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    // akip class name bytes   
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    return deserializeField(bytes, iClass, iFieldName);
  }

  public void deserialize(final ODocument document, final BytesContainer bytes) {
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;

    int last = 0;
    String fieldName;
    OType type;
    int cumulativeSize = 0;
    while (bytes.offset < headerStart + headerLength) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      int fieldLength;
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
        bytes.skip(len);
        Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        fieldLength = pointerAndType.getFirstVal();
        type = pointerAndType.getSecondVal();
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        prop = getGlobalProperty(document, len);
        fieldName = prop.getName();
        fieldLength = OVarIntSerializer.readAsInteger(bytes);
        type = getPropertyTypeFromStream(prop, bytes);
      }

      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        cumulativeSize += fieldLength;
        continue;
      }

      if (fieldLength != 0) {
        int headerCursor = bytes.offset;

        int valuePos = cumulativeSize + headerStart + headerLength;

        bytes.offset = valuePos;
        final Object value = deserializeValue(bytes, type, document);
        if (bytes.offset > last)
          last = bytes.offset;
        bytes.offset = headerCursor;
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else
        ODocumentInternal.rawField(document, fieldName, null, null);

      cumulativeSize += fieldLength;
    }

    ORecordInternal.clearSource(document);

    if (last > bytes.offset) {
      bytes.offset = last;
    }
  }

  public void deserializeWithClassName(final ODocument document, final BytesContainer bytes) {

    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    deserialize(document, bytes);
  }

  public String[] getFieldNames(ODocument reference, final BytesContainer bytes, boolean readClassName) {
    // SKIP CLASS NAME
    if (readClassName) {
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }

    //skip header length
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;

    final List<String> result = new ArrayList<>();

    String fieldName;
    while (bytes.offset < headerStart + headerLength) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
        result.add(fieldName);

        // SKIP THE REST
        bytes.skip(len);
        OVarIntSerializer.readAsInteger(bytes);
        bytes.skip(1);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        prop = ODocumentInternal.getGlobalPropertyById(reference, id);
        if (prop == null) {
          throw new OSerializationException("Missing property definition for property id '" + id + "'");
        }
        result.add(prop.getName());

        // SKIP THE REST
        OVarIntSerializer.readAsInteger(bytes);
        if (prop.getType() == OType.ANY)
          bytes.skip(1);
      }
    }

    return result.toArray(new String[0]);
  }

  private void serializeWriteValues(final BytesContainer headerBuffer, final BytesContainer valuesBuffer, final ODocument document,
      Set<Entry<String, ODocumentEntry>> fields, final Map<String, OProperty> props) {

    for (Entry<String, ODocumentEntry> field : fields) {
      ODocumentEntry docEntry = field.getValue();
      if (!field.getValue().exist()) {
        continue;
      }
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(field.getKey());
        if (prop != null && docEntry.type == prop.getType()) {
          docEntry.property = prop;
        }
      }

      if (docEntry.property == null) {
        String fieldName = field.getKey();
        writeString(headerBuffer, fieldName);
      } else {
        OVarIntSerializer.write(headerBuffer, (docEntry.property.getId() + 1) * -1);
      }

      final Object value = field.getValue().value;

      final OType type;
      if (value != null) {
        type = getFieldType(field.getValue());
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }
        int startOffset = valuesBuffer.offset;
        serializeValue(valuesBuffer, value, type, getLinkedType(document, type, field.getKey()));
        int valueLength = valuesBuffer.offset - startOffset;
        OVarIntSerializer.write(headerBuffer, valueLength);
      } else {
        //handle null fields
        OVarIntSerializer.write(headerBuffer, 0);
        type = OType.ANY;
      }

      //write type. Type should be written both for regular and null fields
      if (field.getValue().property == null || field.getValue().property.getType() == OType.ANY) {
        int typeOffset = headerBuffer.alloc(OByteSerializer.BYTE_SIZE);
        OByteSerializer.INSTANCE.serialize((byte) type.getId(), headerBuffer.bytes, typeOffset);
      }
    }
  }

  private void merge(BytesContainer destinationBuffer, BytesContainer sourceBuffer1, BytesContainer sourceBuffer2) {
    destinationBuffer.offset = destinationBuffer.allocExact(sourceBuffer1.offset + sourceBuffer2.offset);
    System.arraycopy(sourceBuffer1.bytes, 0, destinationBuffer.bytes, destinationBuffer.offset, sourceBuffer1.offset);
    System.arraycopy(sourceBuffer2.bytes, 0, destinationBuffer.bytes, destinationBuffer.offset + sourceBuffer1.offset,
        sourceBuffer2.offset);
    destinationBuffer.offset += sourceBuffer1.offset + sourceBuffer2.offset;
  }

  private void serializeDocument(final ODocument document, final BytesContainer bytes, final OClass clazz) {
    //allocate space for header length

    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;
    final Set<Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    BytesContainer valuesBuffer = new BytesContainer();
    BytesContainer headerBuffer = new BytesContainer();

    serializeWriteValues(headerBuffer, valuesBuffer, document, fields, props);
    int headerLength = headerBuffer.offset;
    //write header length as soon as possible
    OVarIntSerializer.write(bytes, headerLength);

    merge(bytes, headerBuffer, valuesBuffer);
  }

  public void serializeWithClassName(final ODocument document, final BytesContainer bytes, final boolean iClassOnly) {
    final OClass clazz = serializeClass(document, bytes, true);
    if (iClassOnly) {
      writeEmptyString(bytes);
      return;
    }
    serializeDocument(document, bytes, clazz);
  }

  @SuppressWarnings("unchecked")
  public void serialize(final ODocument document, final BytesContainer bytes, final boolean iClassOnly) {
    final OClass clazz = serializeClass(document, bytes, false);
    if (iClassOnly) {
      writeEmptyString(bytes);
      return;
    }
    serializeDocument(document, bytes, clazz);
  }

  private OClass serializeClass(final ODocument document, final BytesContainer bytes, boolean serializeClassName) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (serializeClassName) {
      if (clazz != null && document.isEmbedded())
        writeString(bytes, clazz.getName());
      else
        writeEmptyString(bytes);
    }
    return clazz;
  }

  public boolean isSerializingClassNameByDefault() {
    return false;
  }

  public <RET> RET deserializeFieldTyped(BytesContainer bytes, String iFieldName, boolean isEmbedded, int serializerVersion) {
    if (isEmbedded) {
      skipClassName(bytes);
    }
    return deserializeFieldTypedLoopAndReturn(bytes, iFieldName, serializerVersion);
  }

  protected <RET> RET deserializeFieldTypedLoopAndReturn(BytesContainer bytes, String iFieldName, int serializerVersion) {
    final byte[] field = iFieldName.getBytes();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int cumulativeLength = 0;

    final OMetadataInternal metadata = ODatabaseRecordThreadLocal.instance().get().getMetadata();
    final OImmutableSchema _schema = metadata.getImmutableSchemaSnapshot();

    while (true) {
      if (bytes.offset >= headerStart + headerLength)
        return null;

      int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME        
        boolean match = checkMatchForLargerThenZero(bytes, field, len);

        bytes.skip(len);
        Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        int fieldLength = pointerAndType.getFirstVal();
        OType type = pointerAndType.getSecondVal();

        int valuePos = cumulativeLength + headerStart + headerLength;
        cumulativeLength += fieldLength;
        if (valuePos == 0 || fieldLength == 0)
          return null;

        if (!match)
          continue;

        bytes.offset = valuePos;
        Object value = deserializeValue(bytes, type, null, false, fieldLength, serializerVersion, false);
        //noinspection unchecked
        return (RET) value;
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = _schema.getGlobalPropertyById(id);
        final int fieldLength = OVarIntSerializer.readAsInteger(bytes);
        OType type = getPropertyTypeFromStream(prop, bytes);

        int valuePos = cumulativeLength + headerStart + headerLength;
        cumulativeLength += fieldLength;

        if (!iFieldName.equals(prop.getName()))
          continue;

        if (valuePos == 0 || fieldLength == 0)
          return null;

        bytes.offset = valuePos;

        Object value = deserializeValue(bytes, type, null, false, fieldLength, serializerVersion, false);
        //noinspection unchecked
        return (RET) value;
      }
    }
  }

  public boolean isSerializingClassNameForEmbedded() {
    return true;
  }

  /**
   * use only for named fields
   */
  private Tuple<Integer, OType> getFieldSizeAndTypeFromCurrentPosition(BytesContainer bytes) {
    int fieldSize = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes, false);
    return new Tuple<>(fieldSize, type);
  }

  public void deserializeDebug(BytesContainer bytes, ODatabaseDocumentInternal db, ORecordSerializationDebug debugInfo,
      OImmutableSchema schema) {

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerPos = bytes.offset;

    debugInfo.properties = new ArrayList<>();
    int last = 0;
    String fieldName;
    OType type;
    int cumulativeLength = 0;
    while (true) {
      ORecordSerializationDebugProperty debugProperty = new ORecordSerializationDebugProperty();
      OGlobalProperty prop;

      int fieldLength;

      try {
        if (bytes.offset >= headerPos + headerLength)
          break;

        final int len = OVarIntSerializer.readAsInteger(bytes);
        debugInfo.properties.add(debugProperty);
        if (len > 0) {
          // PARSE FIELD NAME
          fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
          bytes.skip(len);

          Tuple<Integer, OType> valuePositionAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
          fieldLength = valuePositionAndType.getFirstVal();
          type = valuePositionAndType.getSecondVal();
        } else {
          // LOAD GLOBAL PROPERTY BY ID
          final int id = (len * -1) - 1;
          debugProperty.globalId = id;
          prop = schema.getGlobalPropertyById(id);
          fieldLength = OVarIntSerializer.readAsInteger(bytes);
          debugProperty.valuePos = headerPos + headerLength + cumulativeLength;
          if (prop != null) {
            fieldName = prop.getName();
            type = getPropertyTypeFromStream(prop, bytes);
          } else {
            cumulativeLength += fieldLength;
            continue;
          }
        }
        debugProperty.name = fieldName;
        debugProperty.type = type;

        int valuePos;
        if (fieldLength > 0)
          valuePos = headerPos + headerLength + cumulativeLength;
        else
          valuePos = 0;

        cumulativeLength += fieldLength;

        if (valuePos != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = valuePos;
          try {
            debugProperty.value = deserializeValue(bytes, type, new ODocument());
          } catch (RuntimeException ex) {
            debugProperty.faildToRead = true;
            debugProperty.readingException = ex;
            debugProperty.failPosition = bytes.offset;
          }
          if (bytes.offset > last)
            last = bytes.offset;
          bytes.offset = headerCursor;
        } else
          debugProperty.value = null;
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      OType type = OType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      final Object value = entry.getValue();
      if (value != null) {
        type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, value, type, null);
      } else {
        //signal for null value
        int pointer = bytes.alloc(1);
        OByteSerializer.INSTANCE.serialize((byte) -1, bytes.bytes, pointer);
      }
    }

    return fullPos;
  }

  protected Object readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap<Object> result = new OTrackedMap<>(document);

    result.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {
      for (int i = 0; i < size; i++) {
        OType keyType = readOType(bytes, false);
        Object key = deserializeValue(bytes, keyType, document);
        final OType type = HelperClasses.readType(bytes);
        if (type != null) {
          Object value = deserializeValue(bytes, type, document);
          result.put(key, value);
        } else
          result.put(key, null);
      }
      return result;

    } finally {
      result.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  protected List<MapRecordInfo> getPositionsFromEmbeddedMap(final BytesContainer bytes, int serializerVersion) {
    List<MapRecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      OType keyType = readOType(bytes, false);
      String key = readString(bytes);
      OType valueType = HelperClasses.readType(bytes);
      MapRecordInfo recordInfo = new MapRecordInfo();
      recordInfo.fieldType = valueType;
      recordInfo.key = key;
      recordInfo.keyType = keyType;
      int currentOffset = bytes.offset;

      if (valueType != null) {
        recordInfo.fieldStartOffset = bytes.offset;
        deserializeValue(bytes, valueType, null, true, -1,serializerVersion, true);
        recordInfo.fieldLength = bytes.offset - currentOffset;
        retList.add(recordInfo);
      } else {
        recordInfo.fieldStartOffset = 0;
        recordInfo.fieldLength = 0;
        retList.add(recordInfo);
      }
    }

    return retList;
  }

  protected int writeRidBag(BytesContainer bytes, ORidBag ridbag) {
    int positionOffset = bytes.offset;
    HelperClasses.writeRidBag(bytes, ridbag);
    return positionOffset;
  }

  protected ORidBag readRidbag(BytesContainer bytes) {
    return HelperClasses.readRidbag(bytes);
  }

  public Object deserializeValue(final BytesContainer bytes, final OType type, final ODocument ownerDocument) {
    return deserializeValue(bytes, type, ownerDocument, true, -1, -1, false);
  }

  protected Object deserializeValue(final BytesContainer bytes, final OType type, final ODocument ownerDocument,
      boolean embeddedAsDocument, int valueLengthInBytes, int serializerVersion, boolean justRunThrough) {
    if (type == null) {
      throw new ODatabaseException("Invalid type value: null");
    }
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
      if (justRunThrough) {
        int length = OVarIntSerializer.readAsInteger(bytes);
        bytes.skip(length);
      } else {
        value = readString(bytes);
      }
      break;
    case DOUBLE:
      if (justRunThrough) {
        bytes.skip(OLongSerializer.LONG_SIZE);
      } else {
        value = Double.longBitsToDouble(readLong(bytes));
      }
      break;
    case FLOAT:
      if (justRunThrough) {
        bytes.skip(OIntegerSerializer.INT_SIZE);
      } else {
        value = Float.intBitsToFloat(readInteger(bytes));
      }
      break;
    case BYTE:
      if (justRunThrough)
        bytes.offset++;
      else
        value = readByte(bytes);
      break;
    case BOOLEAN:
      if (justRunThrough)
        bytes.offset++;
      else
        value = readByte(bytes) == 1;
      break;
    case DATETIME:
      if (justRunThrough)
        OVarIntSerializer.readAsLong(bytes);
      else
        value = new Date(OVarIntSerializer.readAsLong(bytes));
      break;
    case DATE:
      if (justRunThrough)
        OVarIntSerializer.readAsLong(bytes);
      else {
        long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
        savedTime = convertDayToTimezone(TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
        value = new Date(savedTime);
      }
      break;
    case EMBEDDED:
      if (embeddedAsDocument) {
        value = deserializeEmbeddedAsDocument(bytes, ownerDocument);
      } else {
        value = deserializeEmbeddedAsBytes(bytes, valueLengthInBytes, serializerVersion);
      }
      break;
    case EMBEDDEDSET:
      if (embeddedAsDocument) {
        value = readEmbeddedSet(bytes, ownerDocument);
      } else {
        value = deserializeEmbeddedCollectionAsCollectionOfBytes(bytes, serializerVersion);
      }
      break;
    case EMBEDDEDLIST:
      if (embeddedAsDocument) {
        value = readEmbeddedList(bytes, ownerDocument);
      } else {
        value = deserializeEmbeddedCollectionAsCollectionOfBytes(bytes, serializerVersion);
      }
      break;
    case LINKSET:
      ORecordLazySet collectionSet = null;
      if (!justRunThrough) {
        collectionSet = new ORecordLazySet(ownerDocument);
      }
      value = readLinkCollection(bytes, collectionSet, justRunThrough);
      break;
    case LINKLIST:
      ORecordLazyList collectionList = null;
      if (!justRunThrough) {
        collectionList = new ORecordLazyList(ownerDocument);
      }
      value = readLinkCollection(bytes, collectionList, justRunThrough);
      break;
    case BINARY:
      if (justRunThrough) {
        int len = OVarIntSerializer.readAsInteger(bytes);
        bytes.skip(len);
      } else {
        value = readBinary(bytes);
      }
      break;
    case LINK:
      value = readOptimizedLink(bytes, justRunThrough);
      break;
    case LINKMAP:
      value = readLinkMap(bytes, ownerDocument, justRunThrough);
      break;
    case EMBEDDEDMAP:
      if (embeddedAsDocument) {
        value = readEmbeddedMap(bytes, ownerDocument);
      } else {
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
        if (embeddedAsDocument) {
          stream.fromStream(bytesRepresentation);
          if (stream instanceof OSerializableWrapper)
            value = ((OSerializableWrapper) stream).getSerializable();
          else
            value = stream;
        } else {
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

  protected OType getFieldType(final ODocumentEntry entry) {
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

  protected int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  @SuppressWarnings("unchecked")
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
      pointer = writeRidBag(bytes, (ORidBag) value);
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

  protected int writeEmbeddedCollection(final BytesContainer bytes, final Collection<?> value, final OType linkedType) {
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
            "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
      }
    }
    return pos;
  }

  protected List deserializeEmbeddedCollectionAsCollectionOfBytes(final BytesContainer bytes, int serializerVersion) {
    List retVal = new ArrayList();
    List<RecordInfo> fieldsInfo = getPositionsFromEmbeddedCollection(bytes, serializerVersion);
    for (RecordInfo fieldInfo : fieldsInfo) {
      if (fieldInfo.fieldType.isEmbedded()) {
        OResultBinary result = new OResultBinary(bytes.bytes, fieldInfo.fieldStartOffset, fieldInfo.fieldLength, serializerVersion);
        retVal.add(result);
      } else {
        int currentOffset = bytes.offset;
        bytes.offset = fieldInfo.fieldStartOffset;
        Object value = deserializeValue(bytes, fieldInfo.fieldType, null);
        retVal.add(value);
        bytes.offset = currentOffset;
      }
    }

    return retVal;
  }

  protected Map<String, Object> deserializeEmbeddedMapAsMapOfBytes(final BytesContainer bytes, int serializerVersion) {
    Map<String, Object> retVal = new TreeMap<>();
    List<MapRecordInfo> positionsWithLengths = getPositionsFromEmbeddedMap(bytes, serializerVersion);
    for (MapRecordInfo recordInfo : positionsWithLengths) {
      String key = recordInfo.key;
      Object value;
      if (recordInfo.fieldType.isEmbedded()) {
        value = new OResultBinary(bytes.bytes, recordInfo.fieldStartOffset, recordInfo.fieldLength, serializerVersion);
      } else {
        int currentOffset = bytes.offset;
        bytes.offset = recordInfo.fieldStartOffset;
        value = deserializeValue(bytes, recordInfo.fieldType, null);
        bytes.offset = currentOffset;
      }
      retVal.put(key, value);
    }
    return retVal;
  }

  protected Object deserializeEmbeddedAsDocument(final BytesContainer bytes, final ODocument ownerDocument) {
    Object value = new ODocument();
    deserializeWithClassName((ODocument) value, bytes);
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
    return value;
  }

  //returns begin position and length for each value in embedded collection
  private List<RecordInfo> getPositionsFromEmbeddedCollection(final BytesContainer bytes, int serializerVersion) {
    List<RecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);
    //read collection type
    readByte(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      //read element

      //read data type
      OType dataType = readOType(bytes, false);
      int fieldStart = bytes.offset;

      RecordInfo fieldInfo = new RecordInfo();
      fieldInfo.fieldStartOffset = fieldStart;
      fieldInfo.fieldType = dataType;

      //TODO find better way to skip data bytes;
      deserializeValue(bytes, dataType, null, true, -1, serializerVersion, true);
      fieldInfo.fieldLength = bytes.offset - fieldStart;
      retList.add(fieldInfo);
    }

    return retList;
  }

  protected OResultBinary deserializeEmbeddedAsBytes(final BytesContainer bytes, int valueLength, int serializerVersion) {
    int startOffset = bytes.offset;
    return new OResultBinary(bytes.bytes, startOffset, valueLength, serializerVersion);
  }

  protected Collection<?> readEmbeddedSet(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes, false);

    if (type == OType.ANY) {
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
    // TODO: manage case where type is known
    return null;
  }

  protected Collection<?> readEmbeddedList(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes, false);

    if (type == OType.ANY) {
      final OTrackedList found = new OTrackedList<>(ownerDocument);

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
    // TODO: manage case where type is known
    return null;
  }

  protected void skipClassName(BytesContainer bytes) {
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);
  }

  @Override
  public OBinaryComparator getComparator() {
    return comparator;
  }

  @Override
  public Tuple<Integer, OType> getPointerAndTypeFromCurrentPosition(BytesContainer bytes) {
    int valuePos = readInteger(bytes);
    OType type = readOType(bytes, false);
    return new Tuple<>(valuePos, type);
  }

}
