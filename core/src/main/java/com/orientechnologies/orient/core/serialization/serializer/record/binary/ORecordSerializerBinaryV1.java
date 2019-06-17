package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
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
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MapRecordInfo;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Triple;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Tuple;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.*;

public class ORecordSerializerBinaryV1 extends ORecordSerializerBinaryV0 {

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

    Integer fieldLength = OVarIntSerializer.readAsInteger(bytes);
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

  @Override
  public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields) {
    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]            
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i) {
      fields[i] = bytesFromString(iFields[i]);
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

  @Override
  public void deserializePartialWithClassName(final ODocument document, final BytesContainer bytes, final String[] iFields) {

    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    deserializePartial(document, bytes, iFields);
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

  private Triple<Signal, OBinaryField, Integer> processPropertyDeserializeField(int len, final OImmutableSchema _schema,
      final String iFieldName, final OClass iClass, final BytesContainer bytes, int cumulativeLength, int headerStart,
      int headerLength) {
    final int id = (len * -1) - 1;
    final OGlobalProperty prop = _schema.getGlobalPropertyById(id);
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

  @Override
  public OBinaryField deserializeField(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    final byte[] field = iFieldName.getBytes();

    final OMetadataInternal metadata = (OMetadataInternal) ODatabaseRecordThreadLocal.instance().get().getMetadata();
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

  @Override
  public OBinaryField deserializeFieldWithClassName(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    // akip class name bytes   
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    return deserializeField(bytes, iClass, iFieldName);
  }

  @Override
  public void deserialize(final ODocument document, final BytesContainer bytes) {
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;

    int last = 0;
    String fieldName;
    OType type;
    int cumulativeSize = 0;
    while (true) {

      if (bytes.offset >= headerStart + headerLength)
        break;

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

  @Override
  public void deserializeWithClassName(final ODocument document, final BytesContainer bytes) {

    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    deserialize(document, bytes);
  }

  @Override
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
    while (true) {
      if (bytes.offset >= headerStart + headerLength)
        break;

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

    return result.toArray(new String[result.size()]);
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
        Tuple<Integer, Integer> dataPointerAndLength = serializeValue(valuesBuffer, value, type,
            getLinkedType(document, type, field.getKey()));
        int valueLength = dataPointerAndLength.getSecondVal();
        OVarIntSerializer.write(headerBuffer, valueLength);
      }
      //handle null fields
      else {
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

  @Override
  public void serializeWithClassName(final ODocument document, final BytesContainer bytes, final boolean iClassOnly) {
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
    if (serializeClassName) {
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
  public <RET> RET deserializeFieldTyped(BytesContainer bytes, String iFieldName, boolean isEmbedded, int serializerVersion) {
    if (isEmbedded) {
      skipClassName(bytes);
    }
    return deserializeFieldTypedLoopAndReturn(bytes, iFieldName, serializerVersion);
  }

  @Override
  protected <RET> RET deserializeFieldTypedLoopAndReturn(BytesContainer bytes, String iFieldName, int serializerVersion) {
    final byte[] field = iFieldName.getBytes();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int cumulativeLength = 0;

    final OMetadataInternal metadata = (OMetadataInternal) ODatabaseRecordThreadLocal.instance().get().getMetadata();
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
        return (RET) value;
      }
    }
  }

  @Override
  public boolean isSerializingClassNameForEmbedded() {
    return true;
  }

  /**
   * use only for named fields
   */
  private Tuple<Integer, OType> getFieldSizeAndTypeFromCurrentPosition(BytesContainer bytes) {
    int fieldSize = OVarIntSerializer.readAsInteger(bytes);
    byte typeId = readByte(bytes);
    OType type = OType.getById(typeId);
    return new Tuple<>(fieldSize, type);
  }

  @Override
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
  @Override
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
        OByteSerializer.INSTANCE.serialize((byte) -1, bytes.bytes, bytes.alloc(1));
      }
    }

    return fullPos;
  }

  @Override
  protected Object readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap<Object> result = new OTrackedMap<>(document);

    result.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {
      for (int i = 0; i < size; i++) {
        OType keyType = readOType(bytes, false);
        Object key = deserializeValue(bytes, keyType, document);
        byte typeId = readByte(bytes);
        if (typeId != -1) {
          final OType type = OType.getById(typeId);
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

  @Override
  protected List<MapRecordInfo> getPositionsFromEmbeddedMap(final BytesContainer bytes, int serializerVersion) {
    List<MapRecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      byte keyTypeId = readByte(bytes);
      String key = readString(bytes);
      byte valueTypeId = readByte(bytes);
      if (valueTypeId != -1) {
        OType valueType = OType.getById(valueTypeId);
        MapRecordInfo recordInfo = new MapRecordInfo();
        recordInfo.fieldStartOffset = bytes.offset;
        recordInfo.fieldType = valueType;
        recordInfo.key = key;
        recordInfo.keyType = OType.getById(keyTypeId);
        int currentOffset = bytes.offset;

        deserializeValue(bytes, valueType, null, true, -1, serializerVersion, true);
        recordInfo.fieldLength = bytes.offset - currentOffset;

        retList.add(recordInfo);
      }
    }

    return retList;
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

  private static void writeEmbeddedRidbag(BytesContainer bytes, ORidBag ridbag) {
    OVarIntSerializer.write(bytes, ridbag.size());
    Object[] entries = ((OEmbeddedRidBag) ridbag.getDelegate()).getEntries();
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    for (int i = 0; i < entries.length; i++) {
      Object entry = entries[i];
      if (entry instanceof OIdentifiable) {
        OIdentifiable itemValue = (OIdentifiable) entry;
        final ORID rid = itemValue.getIdentity();
        if (db != null && !db.isClosed() && db.getTransaction().isActive() && !itemValue.getIdentity().isPersistent()) {
          itemValue = db.getTransaction().getRecord(itemValue.getIdentity());
        }
        if (itemValue == null) {
          //should never happen
          String errorMessage = "Found null entry in ridbag with rid=" + rid;
          OSerializationException exc = new OSerializationException(errorMessage);
          OLogManager.instance().error(ORecordSerializerBinaryV1.class, errorMessage, null);
          throw exc;
        } else {
          entries[i] = itemValue.getIdentity();
          writeLinkOptimized(bytes, itemValue);
        }
      }
    }
  }

  private static void writeSBTreeRidbag(BytesContainer bytes, ORidBag ridbag, UUID ownerUuid) {
    ((OSBTreeRidBag) ridbag.getDelegate()).applyNewEntries();

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
      try {
        pointer = ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager().createSBTree(clusterId, ownerUuid);
      } catch (IOException e) {
        throw OException.wrapException(new ODatabaseException("Errur during ridbag creation"), e);
      }
    }

    ((OSBTreeRidBag) ridbag.getDelegate()).setCollectionPointer(pointer);

    OVarIntSerializer.write(bytes, pointer.getFileId());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
    OVarIntSerializer.write(bytes, ridbag.size());

    if (context != null) {
      ((OSBTreeRidBag) ridbag.getDelegate()).handleContextSBTree(context, pointer);
      OVarIntSerializer.write(bytes, 0);
    } else {
      OVarIntSerializer.write(bytes, 0);

      //removed changes serialization
    }
  }

  @Override
  protected int writeRidBag(BytesContainer bytes, ORidBag ridbag) {
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

  @Override
  protected ORidBag readRidbag(BytesContainer bytes) {
    byte configByte = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset++);
    boolean isEmbedded = (configByte & 1) != 0;

    UUID uuid = null;
    //removed deserializing UUID

    ORidBag ridbag = null;
    if (isEmbedded) {
      ridbag = new ORidBag();
      int size = OVarIntSerializer.readAsInteger(bytes);
      ridbag.getDelegate().setSize(size);
      for (int i = 0; i < size; i++) {
        OIdentifiable record = readLinkOptimizedEmbedded(bytes);
        ((OEmbeddedRidBag) ridbag.getDelegate()).addEntry(record);
      }
    } else {
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
      for (int i = 0; i < changesSize; i++) {
        OIdentifiable recId = readLinkOptimizedSBTree(bytes);
        Change change = deserializeChange(bytes);
        changes.put(recId, change);
      }

      ridbag = new ORidBag(pointer, changes, uuid);
      ridbag.getDelegate().setSize(-1);
    }
    return ridbag;
  }

  private static Change deserializeChange(BytesContainer bytes) {
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

  private static void writeLinkOptimized(final BytesContainer bytes, OIdentifiable link) {
    ORID id = link.getIdentity();
    OVarIntSerializer.write(bytes, id.getClusterId());
    OVarIntSerializer.write(bytes, id.getClusterPosition());
  }

}
