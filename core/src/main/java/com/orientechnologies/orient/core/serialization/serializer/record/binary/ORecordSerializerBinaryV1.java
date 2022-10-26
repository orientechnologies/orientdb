package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.bytesFromString;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.getGlobalProperty;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.getLinkedType;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.getTypeFromValueEmbedded;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readBinary;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readInteger;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readLinkCollection;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readLinkMap;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readLong;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readOType;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readOptimizedLink;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readString;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.stringFromBytesIntern;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeBinary;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeLinkCollection;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeLinkMap;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeOType;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeOptimizedLink;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeString;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OPropertyEncryption;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MapRecordInfo;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.RecordInfo;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.Tuple;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

public class ORecordSerializerBinaryV1 implements ODocumentSerializer {
  private final OBinaryComparatorV0 comparator = new OBinaryComparatorV0();

  private int findMatchingFieldName(final BytesContainer bytes, int len, byte[][] fields) {
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i] != null && fields[i].length == len) {
        boolean matchField = true;
        for (int j = 0; j < len; ++j) {
          if (bytes.bytes[bytes.offset + j] != fields[i][j]) {
            matchField = false;
            break;
          }
        }
        if (matchField) {
          return i;
        }
      }
    }

    return -1;
  }

  private boolean checkIfPropertyNameMatchSome(OGlobalProperty prop, final String[] fields) {
    String fieldName = prop.getName();

    for (int i = 0; i < fields.length; i++) {
      if (fieldName.equals(fields[i])) {
        return true;
      }
    }

    return false;
  }

  public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields) {
    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i) {
      fields[i] = bytesFromString(iFields[i]);
    }

    String fieldName;
    OType type;
    int unmarshalledFields = 0;

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int currentValuePos = valuesStart;

    while (bytes.offset < valuesStart) {

      final int len = OVarIntSerializer.readAsInteger(bytes);
      boolean found;
      int fieldLength;
      if (len > 0) {
        int fieldPos = findMatchingFieldName(bytes, len, fields);
        bytes.skip(len);
        Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        fieldLength = pointerAndType.getFirstVal();
        type = pointerAndType.getSecondVal();

        if (fieldPos >= 0) {
          fieldName = iFields[fieldPos];
          found = true;
        } else {
          fieldName = null;
          found = false;
        }
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final OGlobalProperty prop = getGlobalProperty(document, len);
        found = checkIfPropertyNameMatchSome(prop, iFields);

        fieldLength = OVarIntSerializer.readAsInteger(bytes);
        type = getPropertyTypeFromStream(prop, bytes);

        fieldName = prop.getName();
      }
      if (found) {
        if (fieldLength != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = currentValuePos;
          final Object value = deserializeValue(bytes, type, document);
          bytes.offset = headerCursor;
          ODocumentInternal.rawField(document, fieldName, value, type);
        } else {
          // If pos us 0 the value is null just set it.
          ODocumentInternal.rawField(document, fieldName, null, null);
        }
        if (++unmarshalledFields == iFields.length)
          // ALL REQUESTED FIELDS UNMARSHALLED: EXIT
          break;
      }
      currentValuePos += fieldLength;
    }
  }

  private boolean checkMatchForLargerThenZero(
      final BytesContainer bytes, final byte[] field, int len) {
    if (field.length != len) {
      return false;
    }
    boolean match = true;
    for (int j = 0; j < len; ++j) {
      if (bytes.bytes[bytes.offset + j] != field[j]) {
        match = false;
        break;
      }
    }

    return match;
  }

  private OType getPropertyTypeFromStream(final OGlobalProperty prop, final BytesContainer bytes) {
    final OType type;
    if (prop.getType() != OType.ANY) type = prop.getType();
    else type = readOType(bytes, false);

    return type;
  }

  public OBinaryField deserializeField(
      final BytesContainer bytes,
      final OClass iClass,
      final String iFieldName,
      boolean embedded,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {

    if (embedded) {
      // skip class name bytes
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }
    final byte[] field = iFieldName.getBytes();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int currentValuePos = valuesStart;

    while (bytes.offset < valuesStart) {

      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {

        boolean match = checkMatchForLargerThenZero(bytes, field, len);

        bytes.skip(len);
        Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        final int fieldLength = pointerAndType.getFirstVal();
        final OType type = pointerAndType.getSecondVal();

        if (match) {
          if (fieldLength == 0 || !getComparator().isBinaryComparable(type)) {
            return null;
          }

          bytes.offset = currentValuePos;
          return new OBinaryField(iFieldName, type, bytes, null);
        }
        currentValuePos += fieldLength;
      } else {

        final int id = (len * -1) - 1;
        final OGlobalProperty prop = schema.getGlobalPropertyById(id);
        final int fieldLength = OVarIntSerializer.readAsInteger(bytes);
        final OType type;
        type = getPropertyTypeFromStream(prop, bytes);

        if (iFieldName.equals(prop.getName())) {
          if (fieldLength == 0 || !getComparator().isBinaryComparable(type)) {
            return null;
          }
          bytes.offset = currentValuePos;
          final OProperty classProp = iClass.getProperty(iFieldName);
          return new OBinaryField(
              iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null);
        }
        currentValuePos += fieldLength;
      }
    }
    return null;
  }

  public void deserialize(final ODocument document, final BytesContainer bytes) {
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int last = 0;
    String fieldName;
    OType type;
    int cumulativeSize = valuesStart;
    while (bytes.offset < valuesStart) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      int fieldLength;
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
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

      if (!ODocumentInternal.rawContainsField(document, fieldName)) {
        if (fieldLength != 0) {
          int headerCursor = bytes.offset;

          bytes.offset = cumulativeSize;
          final Object value = deserializeValue(bytes, type, document);
          if (bytes.offset > last) last = bytes.offset;
          bytes.offset = headerCursor;
          ODocumentInternal.rawField(document, fieldName, value, type);
        } else {
          ODocumentInternal.rawField(document, fieldName, null, null);
        }
      }

      cumulativeSize += fieldLength;
    }

    ORecordInternal.clearSource(document);

    if (last > bytes.offset) {
      bytes.offset = last;
    }
  }

  public void deserializeWithClassName(final ODocument document, final BytesContainer bytes) {

    final String className = readString(bytes);
    if (className.length() != 0) ODocumentInternal.fillClassNameIfNeeded(document, className);

    deserialize(document, bytes);
  }

  public String[] getFieldNames(ODocument reference, final BytesContainer bytes, boolean embedded) {
    // SKIP CLASS NAME
    if (embedded) {
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }

    // skip header length
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;

    final List<String> result = new ArrayList<>();

    String fieldName;
    while (bytes.offset < headerStart + headerLength) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
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
          throw new OSerializationException(
              "Missing property definition for property id '" + id + "'");
        }
        result.add(prop.getName());

        // SKIP THE REST
        OVarIntSerializer.readAsInteger(bytes);
        if (prop.getType() == OType.ANY) bytes.skip(1);
      }
    }

    return result.toArray(new String[0]);
  }

  private void serializeValues(
      final BytesContainer headerBuffer,
      final BytesContainer valuesBuffer,
      final ODocument document,
      Set<Entry<String, ODocumentEntry>> fields,
      final Map<String, OProperty> props,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
    OClass oClass = ODocumentInternal.getImmutableSchemaClass(document);
    for (Entry<String, ODocumentEntry> field : fields) {
      ODocumentEntry docEntry = field.getValue();
      if (!field.getValue().exists()) {
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
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the ODocument binary serializer");
        }
        int startOffset = valuesBuffer.offset;
        serializeValue(
            valuesBuffer,
            value,
            type,
            getLinkedType(oClass, type, field.getKey()),
            schema,
            encryption);
        int valueLength = valuesBuffer.offset - startOffset;
        OVarIntSerializer.write(headerBuffer, valueLength);
      } else {
        // handle null fields
        OVarIntSerializer.write(headerBuffer, 0);
        type = OType.ANY;
      }

      // write type. Type should be written both for regular and null fields
      if (field.getValue().property == null || field.getValue().property.getType() == OType.ANY) {
        int typeOffset = headerBuffer.alloc(OByteSerializer.BYTE_SIZE);
        OByteSerializer.INSTANCE.serialize((byte) type.getId(), headerBuffer.bytes, typeOffset);
      }
    }
  }

  private void merge(
      BytesContainer destinationBuffer,
      BytesContainer sourceBuffer1,
      BytesContainer sourceBuffer2) {
    destinationBuffer.offset =
        destinationBuffer.allocExact(sourceBuffer1.offset + sourceBuffer2.offset);
    System.arraycopy(
        sourceBuffer1.bytes,
        0,
        destinationBuffer.bytes,
        destinationBuffer.offset,
        sourceBuffer1.offset);
    System.arraycopy(
        sourceBuffer2.bytes,
        0,
        destinationBuffer.bytes,
        destinationBuffer.offset + sourceBuffer1.offset,
        sourceBuffer2.offset);
    destinationBuffer.offset += sourceBuffer1.offset + sourceBuffer2.offset;
  }

  private void serializeDocument(
      final ODocument document,
      final BytesContainer bytes,
      final OClass clazz,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
    // allocate space for header length

    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;
    final Set<Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    BytesContainer valuesBuffer = new BytesContainer();
    BytesContainer headerBuffer = new BytesContainer();

    serializeValues(headerBuffer, valuesBuffer, document, fields, props, schema, encryption);
    int headerLength = headerBuffer.offset;
    // write header length as soon as possible
    OVarIntSerializer.write(bytes, headerLength);

    merge(bytes, headerBuffer, valuesBuffer);
  }

  public void serializeWithClassName(final ODocument document, final BytesContainer bytes) {
    OImmutableSchema schema = ODocumentInternal.getImmutableSchema(document);
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null && document.isEmbedded()) writeString(bytes, clazz.getName());
    else writeEmptyString(bytes);
    OPropertyEncryption encryption = ODocumentInternal.getPropertyEncryption(document);
    serializeDocument(document, bytes, clazz, schema, encryption);
  }

  public void serialize(final ODocument document, final BytesContainer bytes) {
    OImmutableSchema schema = ODocumentInternal.getImmutableSchema(document);
    OPropertyEncryption encryption = ODocumentInternal.getPropertyEncryption(document);
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    serializeDocument(document, bytes, clazz, schema, encryption);
  }

  public boolean isSerializingClassNameByDefault() {
    return false;
  }

  public <RET> RET deserializeFieldTyped(
      BytesContainer bytes,
      String iFieldName,
      boolean isEmbedded,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
    if (isEmbedded) {
      skipClassName(bytes);
    }
    return deserializeFieldTypedLoopAndReturn(bytes, iFieldName, schema, encryption);
  }

  protected <RET> RET deserializeFieldTypedLoopAndReturn(
      BytesContainer bytes,
      String iFieldName,
      final OImmutableSchema schema,
      OPropertyEncryption encryption) {
    final byte[] field = iFieldName.getBytes();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int cumulativeLength = valuesStart;

    while (bytes.offset < valuesStart) {

      int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        boolean match = checkMatchForLargerThenZero(bytes, field, len);

        bytes.skip(len);
        Tuple<Integer, OType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        int fieldLength = pointerAndType.getFirstVal();
        OType type = pointerAndType.getSecondVal();

        if (match) {
          if (fieldLength == 0) {
            return null;
          }

          bytes.offset = cumulativeLength;
          Object value = deserializeValue(bytes, type, null, false, fieldLength, false, schema);
          //noinspection unchecked
          return (RET) value;
        }
        cumulativeLength += fieldLength;
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = schema.getGlobalPropertyById(id);
        final int fieldLength = OVarIntSerializer.readAsInteger(bytes);
        OType type = getPropertyTypeFromStream(prop, bytes);

        if (iFieldName.equals(prop.getName())) {

          if (fieldLength == 0) {
            return null;
          }

          bytes.offset = cumulativeLength;

          Object value = deserializeValue(bytes, type, null, false, fieldLength, false, schema);
          //noinspection unchecked
          return (RET) value;
        }
        cumulativeLength += fieldLength;
      }
    }
    return null;
  }

  /** use only for named fields */
  private Tuple<Integer, OType> getFieldSizeAndTypeFromCurrentPosition(BytesContainer bytes) {
    int fieldSize = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes, false);
    return new Tuple<>(fieldSize, type);
  }

  public void deserializeDebug(
      BytesContainer bytes,
      ODatabaseDocumentInternal db,
      ORecordSerializationDebug debugInfo,
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
        if (bytes.offset >= headerPos + headerLength) break;

        final int len = OVarIntSerializer.readAsInteger(bytes);
        debugInfo.properties.add(debugProperty);
        if (len > 0) {
          // PARSE FIELD NAME
          fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
          bytes.skip(len);

          Tuple<Integer, OType> valuePositionAndType =
              getFieldSizeAndTypeFromCurrentPosition(bytes);
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
        if (fieldLength > 0) valuePos = headerPos + headerLength + cumulativeLength;
        else valuePos = 0;

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
          if (bytes.offset > last) last = bytes.offset;
          bytes.offset = headerCursor;
        } else debugProperty.value = null;
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected int writeEmbeddedMap(
      BytesContainer bytes,
      Map<Object, Object> map,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
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
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the ODocument binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, value, type, null, schema, encryption);
      } else {
        // signal for null value
        int pointer = bytes.alloc(1);
        OByteSerializer.INSTANCE.serialize((byte) -1, bytes.bytes, pointer);
      }
    }

    return fullPos;
  }

  protected Object readEmbeddedMap(final BytesContainer bytes, final ORecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap<Object> result = new OTrackedMap<>(owner);
    for (int i = 0; i < size; i++) {
      OType keyType = readOType(bytes, false);
      Object key = deserializeValue(bytes, keyType, result);
      final OType type = HelperClasses.readType(bytes);
      if (type != null) {
        Object value = deserializeValue(bytes, type, result);
        result.putInternal(key, value);
      } else result.putInternal(key, null);
    }
    return result;
  }

  protected List<MapRecordInfo> getPositionsFromEmbeddedMap(
      final BytesContainer bytes, OImmutableSchema schema) {
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
        deserializeValue(bytes, valueType, null, true, -1, true, schema);
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

  public Object deserializeValue(
      final BytesContainer bytes, final OType type, final ORecordElement owner) {
    ORecordElement doc = owner;
    while (!(doc instanceof ODocument) && doc != null) {
      doc = doc.getOwner();
    }
    OImmutableSchema schema = ODocumentInternal.getImmutableSchema((ODocument) doc);
    return deserializeValue(bytes, type, owner, true, -1, false, schema);
  }

  protected Object deserializeValue(
      final BytesContainer bytes,
      final OType type,
      final ORecordElement owner,
      boolean embeddedAsDocument,
      int valueLengthInBytes,
      boolean justRunThrough,
      OImmutableSchema schema) {
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
        if (justRunThrough) bytes.offset++;
        else value = readByte(bytes);
        break;
      case BOOLEAN:
        if (justRunThrough) bytes.offset++;
        else value = readByte(bytes) == 1;
        break;
      case DATETIME:
        if (justRunThrough) OVarIntSerializer.readAsLong(bytes);
        else value = new Date(OVarIntSerializer.readAsLong(bytes));
        break;
      case DATE:
        if (justRunThrough) OVarIntSerializer.readAsLong(bytes);
        else {
          long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
          savedTime =
              convertDayToTimezone(
                  TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
          value = new Date(savedTime);
        }
        break;
      case EMBEDDED:
        if (embeddedAsDocument) {
          value = deserializeEmbeddedAsDocument(bytes, owner);
        } else {
          value = deserializeEmbeddedAsBytes(bytes, valueLengthInBytes, schema);
        }
        break;
      case EMBEDDEDSET:
        if (embeddedAsDocument) {
          value = readEmbeddedSet(bytes, owner);
        } else {
          value = deserializeEmbeddedCollectionAsCollectionOfBytes(bytes, schema);
        }
        break;
      case EMBEDDEDLIST:
        if (embeddedAsDocument) {
          value = readEmbeddedList(bytes, owner);
        } else {
          value = deserializeEmbeddedCollectionAsCollectionOfBytes(bytes, schema);
        }
        break;
      case LINKSET:
        ORecordLazySet collectionSet = null;
        if (!justRunThrough) {
          collectionSet = new ORecordLazySet(owner);
        }
        value = readLinkCollection(bytes, collectionSet, justRunThrough);
        break;
      case LINKLIST:
        ORecordLazyList collectionList = null;
        if (!justRunThrough) {
          collectionList = new ORecordLazyList(owner);
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
        value = readLinkMap(bytes, owner, justRunThrough);
        break;
      case EMBEDDEDMAP:
        if (embeddedAsDocument) {
          value = readEmbeddedMap(bytes, owner);
        } else {
          value = deserializeEmbeddedMapAsMapOfBytes(bytes, schema);
        }
        break;
      case DECIMAL:
        value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        ORidBag bag = readRidbag(bytes);
        bag.setOwner(owner);
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
            else value = stream;
          } else {
            OResultBinary retVal =
                new OResultBinary(schema, bytesRepresentation, 0, bytesRepresentation.length, this);
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
      if (prop != null) type = prop.getType();
    }
    if (type == null || OType.ANY == type) type = OType.getTypeByValue(entry.value);
    return type;
  }

  protected int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int serializeValue(
      final BytesContainer bytes,
      Object value,
      final OType type,
      final OType linkedType,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
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
        } else pointer = OVarIntSerializer.write(bytes, ((Date) value).getTime());
        break;
      case DATE:
        long dateValue;
        if (value instanceof Number) {
          dateValue = ((Number) value).longValue();
        } else dateValue = ((Date) value).getTime();
        dateValue =
            convertDayToTimezone(
                ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
        pointer = OVarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
        break;
      case EMBEDDED:
        pointer = bytes.offset;
        if (value instanceof ODocumentSerializable) {
          ODocument cur = ((ODocumentSerializable) value).toDocument();
          cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
          serializeWithClassName(cur, bytes);
        } else {
          serializeWithClassName((ODocument) value, bytes);
        }
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray())
          pointer =
              writeEmbeddedCollection(
                  bytes, Arrays.asList(OMultiValue.array(value)), linkedType, schema, encryption);
        else
          pointer =
              writeEmbeddedCollection(bytes, (Collection<?>) value, linkedType, schema, encryption);
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
        pointer = writeEmbeddedMap(bytes, (Map<Object, Object>) value, schema, encryption);
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

  protected int writeEmbeddedCollection(
      final BytesContainer bytes,
      final Collection<?> value,
      final OType linkedType,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
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
      if (linkedType == null || linkedType == OType.ANY) type = getTypeFromValueEmbedded(itemValue);
      else type = linkedType;
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, itemValue, type, null, schema, encryption);
      } else {
        throw new OSerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the ODocument binary serializer");
      }
    }
    return pos;
  }

  protected List deserializeEmbeddedCollectionAsCollectionOfBytes(
      final BytesContainer bytes, OImmutableSchema schema) {
    List retVal = new ArrayList();
    List<RecordInfo> fieldsInfo = getPositionsFromEmbeddedCollection(bytes, schema);
    for (RecordInfo fieldInfo : fieldsInfo) {
      if (fieldInfo.fieldType.isEmbedded()) {
        OResultBinary result =
            new OResultBinary(
                schema, bytes.bytes, fieldInfo.fieldStartOffset, fieldInfo.fieldLength, this);
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

  protected Map<String, Object> deserializeEmbeddedMapAsMapOfBytes(
      final BytesContainer bytes, OImmutableSchema schema) {
    Map<String, Object> retVal = new TreeMap<>();
    List<MapRecordInfo> positionsWithLengths = getPositionsFromEmbeddedMap(bytes, schema);
    for (MapRecordInfo recordInfo : positionsWithLengths) {
      String key = recordInfo.key;
      Object value;
      if (recordInfo.fieldType != null && recordInfo.fieldType.isEmbedded()) {
        value =
            new OResultBinary(
                schema, bytes.bytes, recordInfo.fieldStartOffset, recordInfo.fieldLength, this);
      } else if (recordInfo.fieldStartOffset != 0) {
        int currentOffset = bytes.offset;
        bytes.offset = recordInfo.fieldStartOffset;
        value = deserializeValue(bytes, recordInfo.fieldType, null);
        bytes.offset = currentOffset;
      } else {
        value = null;
      }
      retVal.put(key, value);
    }
    return retVal;
  }

  protected Object deserializeEmbeddedAsDocument(
      final BytesContainer bytes, final ORecordElement owner) {
    Object value = new ODocumentEmbedded();
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
    } else ODocumentInternal.addOwner((ODocument) value, owner);
    return value;
  }

  // returns begin position and length for each value in embedded collection
  private List<RecordInfo> getPositionsFromEmbeddedCollection(
      final BytesContainer bytes, OImmutableSchema schema) {
    List<RecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);
    // read collection type
    readByte(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      // read element

      // read data type
      OType dataType = readOType(bytes, false);
      int fieldStart = bytes.offset;

      RecordInfo fieldInfo = new RecordInfo();
      fieldInfo.fieldStartOffset = fieldStart;
      fieldInfo.fieldType = dataType;

      // TODO find better way to skip data bytes;
      deserializeValue(bytes, dataType, null, true, -1, true, schema);
      fieldInfo.fieldLength = bytes.offset - fieldStart;
      retList.add(fieldInfo);
    }

    return retList;
  }

  protected OResultBinary deserializeEmbeddedAsBytes(
      final BytesContainer bytes, int valueLength, OImmutableSchema schema) {
    int startOffset = bytes.offset;
    return new OResultBinary(schema, bytes.bytes, startOffset, valueLength, this);
  }

  protected Collection<?> readEmbeddedSet(final BytesContainer bytes, final ORecordElement owner) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes, false);

    if (type == OType.ANY) {
      final OTrackedSet found = new OTrackedSet<>(owner);
      for (int i = 0; i < items; i++) {
        OType itemType = readOType(bytes, false);
        if (itemType == OType.ANY) found.addInternal(null);
        else found.addInternal(deserializeValue(bytes, itemType, found));
      }
      return found;
    }
    // TODO: manage case where type is known
    return null;
  }

  protected Collection<?> readEmbeddedList(final BytesContainer bytes, final ORecordElement owner) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes, false);

    if (type == OType.ANY) {
      final OTrackedList found = new OTrackedList<>(owner);
      for (int i = 0; i < items; i++) {
        OType itemType = readOType(bytes, false);
        if (itemType == OType.ANY) found.addInternal(null);
        else found.addInternal(deserializeValue(bytes, itemType, found));
      }
      return found;
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
}
