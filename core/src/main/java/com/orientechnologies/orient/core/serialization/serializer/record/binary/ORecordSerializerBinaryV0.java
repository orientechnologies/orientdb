/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.NULL_RECORD_ID;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.bytesFromString;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.getGlobalProperty;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.getTypeFromValueEmbedded;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readBinary;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readInteger;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readLinkCollection;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readLong;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readOType;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readOptimizedLink;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readString;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.stringFromBytesIntern;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeBinary;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeLinkCollection;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeNullLink;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeOType;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeOptimizedLink;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.writeString;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
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
import com.orientechnologies.orient.core.id.ORecordId;
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

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  private final OBinaryComparatorV0 comparator = new OBinaryComparatorV0();

  public ORecordSerializerBinaryV0() {}

  @Override
  public OBinaryComparator getComparator() {
    return comparator;
  }

  @Override
  public void deserializePartial(
      final ODocument document, final BytesContainer bytes, final String[] iFields) {
    final String className = readString(bytes);
    if (className.length() != 0) ODocumentInternal.fillClassNameIfNeeded(document, className);

    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i) fields[i] = bytesFromString(iFields[i]);

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
          if (fields[i] != null && fields[i].length == len) {
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
        Tuple<Integer, OType> pointerAndType = getPointerAndTypeFromCurrentPosition(bytes);
        valuePos = pointerAndType.getFirstVal();
        type = pointerAndType.getSecondVal();
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
        if (prop.getType() != OType.ANY) type = prop.getType();
        else type = readOType(bytes, false);
      }

      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        final Object value = deserializeValue(bytes, type, document);
        bytes.offset = headerCursor;
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else ODocumentInternal.rawField(document, fieldName, null, null);

      if (++unmarshalledFields == iFields.length)
        // ALL REQUESTED FIELDS UNMARSHALLED: EXIT
        break;
    }
  }

  @Override
  public OBinaryField deserializeField(
      BytesContainer bytes,
      OClass iClass,
      String iFieldName,
      boolean embedded,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final byte[] field = iFieldName.getBytes();

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
          Tuple<Integer, OType> pointerAndType = getPointerAndTypeFromCurrentPosition(bytes);
          final int valuePos = pointerAndType.getFirstVal();
          final OType type = pointerAndType.getSecondVal();

          if (valuePos == 0) return null;

          if (!match) continue;

          if (!getComparator().isBinaryComparable(type)) return null;

          bytes.offset = valuePos;
          return new OBinaryField(iFieldName, type, bytes, null);
        }

        // SKIP IT
        bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = schema.getGlobalPropertyById(id);
        if (iFieldName.equals(prop.getName())) {
          final int valuePos = readInteger(bytes);
          final OType type;
          if (prop.getType() != OType.ANY) type = prop.getType();
          else type = readOType(bytes, false);

          if (valuePos == 0) return null;

          if (!getComparator().isBinaryComparable(type)) return null;

          bytes.offset = valuePos;

          final OProperty classProp = iClass.getProperty(iFieldName);
          return new OBinaryField(
              iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null);
        }
        bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
      }
    }
  }

  public void deserializeWithClassName(final ODocument document, final BytesContainer bytes) {
    deserialize(document, bytes);
  }

  @Override
  public void deserialize(final ODocument document, final BytesContainer bytes) {
    final String className = readString(bytes);
    if (className.length() != 0) ODocumentInternal.fillClassNameIfNeeded(document, className);

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
        fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
        bytes.skip(len);
        Tuple<Integer, OType> pointerAndType = getPointerAndTypeFromCurrentPosition(bytes);
        valuePos = pointerAndType.getFirstVal();
        type = pointerAndType.getSecondVal();
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        prop = getGlobalProperty(document, len);
        fieldName = prop.getName();
        valuePos = readInteger(bytes);
        if (prop.getType() != OType.ANY) type = prop.getType();
        else type = readOType(bytes, false);
      }

      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }

      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        final Object value = deserializeValue(bytes, type, document);
        if (bytes.offset > last) last = bytes.offset;
        bytes.offset = headerCursor;
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else ODocumentInternal.rawField(document, fieldName, null, null);
    }

    ORecordInternal.clearSource(document);

    if (last > bytes.offset) bytes.offset = last;
  }

  @Override
  public String[] getFieldNames(ODocument reference, final BytesContainer bytes, boolean embedded) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final List<String> result = new ArrayList<>();

    String fieldName;
    while (true) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len == 0) {
        // SCAN COMPLETED
        break;
      } else if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
        result.add(fieldName);

        // SKIP THE REST
        bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);
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
        bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
      }
    }

    return result.toArray(new String[result.size()]);
  }

  public void serialize(final ODocument document, final BytesContainer bytes) {
    OImmutableSchema schema = ODocumentInternal.getImmutableSchema(document);
    OPropertyEncryption encryption = ODocumentInternal.getPropertyEncryption(document);
    serialize(document, bytes, schema, encryption);
  }

  public void serialize(
      final ODocument document,
      final BytesContainer bytes,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {

    final OClass clazz = serializeClass(document, bytes);

    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;

    final Set<Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    final int[] pos = new int[fields.size()];

    int i = 0;

    final Entry<String, ODocumentEntry>[] values = new Entry[fields.size()];
    for (Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      if (!docEntry.exists()) continue;
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(entry.getKey());
        if (prop != null && docEntry.type == prop.getType()) docEntry.property = prop;
      }

      if (docEntry.property != null) {
        OVarIntSerializer.write(bytes, (docEntry.property.getId() + 1) * -1);
        if (docEntry.property.getType() != OType.ANY)
          pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE);
        else pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
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
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the ODocument binary serializer");
        }
        pointer =
            serializeValue(
                bytes,
                value,
                type,
                getLinkedType(document, type, values[i].getKey()),
                schema,
                encryption);
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, bytes.bytes, pos[i]);
        if (values[i].getValue().property == null
            || values[i].getValue().property.getType() == OType.ANY)
          writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }
  }

  @Override
  public Object deserializeValue(
      final BytesContainer bytes, final OType type, final ORecordElement owner) {
    ORecordElement doc = owner;
    while (!(doc instanceof ODocument) && doc != null) {
      doc = doc.getOwner();
    }
    OImmutableSchema schema = ODocumentInternal.getImmutableSchema((ODocument) doc);
    return deserializeValue(bytes, type, owner, true, -1, false, schema);
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

  protected List<MapRecordInfo> getPositionsFromEmbeddedMap(
      final BytesContainer bytes, OImmutableSchema schema) {
    List<MapRecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      OType keyType = readOType(bytes, false);
      String key = readString(bytes);
      int valuePos = readInteger(bytes);
      OType valueType = readOType(bytes, false);
      MapRecordInfo recordInfo = new MapRecordInfo();
      recordInfo.fieldStartOffset = valuePos;
      recordInfo.fieldType = valueType;
      recordInfo.key = key;
      recordInfo.keyType = keyType;
      int currentOffset = bytes.offset;
      bytes.offset = valuePos;

      // get field length
      bytes.offset = valuePos;
      deserializeValue(bytes, valueType, null, true, -1, true, schema);
      recordInfo.fieldLength = bytes.offset - valuePos;

      bytes.offset = currentOffset;
      retList.add(recordInfo);
    }

    return retList;
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

  protected OResultBinary deserializeEmbeddedAsBytes(
      final BytesContainer bytes, int valueLength, OImmutableSchema schema) {
    int startOffset = bytes.offset;
    return new OResultBinary(schema, bytes.bytes, startOffset, valueLength, this);
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
        value = readLinkMap(bytes, owner, justRunThrough, schema);
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

  protected ORidBag readRidbag(BytesContainer bytes) {
    ORidBag bag = new ORidBag();
    bag.fromStream(bytes);
    return bag;
  }

  protected OClass serializeClass(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null) writeString(bytes, clazz.getName());
    else writeEmptyString(bytes);
    return clazz;
  }

  protected int writeLinkMap(final BytesContainer bytes, final Map<Object, OIdentifiable> map) {
    final boolean disabledAutoConversion =
        map instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

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
        if (entry.getValue() == null) writeNullLink(bytes);
        else writeOptimizedLink(bytes, entry.getValue());
      }
      return fullPos;

    } finally {
      if (disabledAutoConversion) ((ORecordLazyMultiValue) map).setAutoConvertToRecord(true);
    }
  }

  protected Map<Object, OIdentifiable> readLinkMap(
      final BytesContainer bytes,
      final ORecordElement owner,
      boolean justRunThrough,
      OImmutableSchema schema) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    ORecordLazyMap result = null;
    if (!justRunThrough) result = new ORecordLazyMap(owner);
    while ((size--) > 0) {
      final OType keyType = readOType(bytes, justRunThrough);
      final Object key = deserializeValue(bytes, keyType, result, true, -1, justRunThrough, schema);
      final ORecordId value = readOptimizedLink(bytes, justRunThrough);
      if (value.equals(NULL_RECORD_ID)) result.putInternal(key, null);
      else result.putInternal(key, value);
    }
    return result;
  }

  protected Object readEmbeddedMap(final BytesContainer bytes, final ORecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap<Object> result = new OTrackedMap<>(owner);

    int last = 0;
    while ((size--) > 0) {
      OType keyType = readOType(bytes, false);
      Object key = deserializeValue(bytes, keyType, result);
      final int valuePos = readInteger(bytes);
      final OType type = readOType(bytes, false);
      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        Object value = deserializeValue(bytes, type, result);
        if (bytes.offset > last) last = bytes.offset;
        bytes.offset = headerCursor;
        result.putInternal(key, value);
      } else result.putInternal(key, null);
    }
    if (last > bytes.offset) bytes.offset = last;
    return result;
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

  protected OType getLinkedType(ODocument document, OType type, String key) {
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
          serialize(cur, bytes, schema, encryption);
        } else {
          serialize((ODocument) value, bytes, schema, encryption);
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

  protected int writeRidBag(BytesContainer bytes, ORidBag ridbag) {
    return ridbag.toStream(bytes);
  }

  @SuppressWarnings("unchecked")
  protected int writeEmbeddedMap(
      BytesContainer bytes,
      Map<Object, Object> map,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
    final int[] pos = new int[map.size()];
    int i = 0;
    Entry<Object, Object>[] values = new Entry[map.size()];
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
      final Object value = values[i].getValue();
      if (value != null) {
        final OType type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the ODocument binary serializer");
        }
        int pointer = serializeValue(bytes, value, type, null, schema, encryption);
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, bytes.bytes, pos[i]);
        writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      } else {
        // signal for null value
        OIntegerSerializer.INSTANCE.serializeLiteral(0, bytes.bytes, pos[i]);
      }
    }
    return fullPos;
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

  @Override
  public boolean isSerializingClassNameByDefault() {
    return true;
  }

  protected void skipClassName(BytesContainer bytes) {
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);
  }

  private int getEmbeddedFieldSize(
      BytesContainer bytes, int currentFieldDataPos, OType type, OImmutableSchema schema) {
    int startOffset = bytes.offset;
    try {
      // try to read next position in header if exists
      int len = OVarIntSerializer.readAsInteger(bytes);
      if (len != 0) {
        if (len > 0) {
          // skip name bytes
          bytes.skip(len);
        }
        int nextFieldPos = readInteger(bytes);
        return nextFieldPos - currentFieldDataPos;
      }

      // this means that this is last field so field length have to be calculated
      // by deserializing the value
      bytes.offset = currentFieldDataPos;
      deserializeValue(bytes, type, new ODocument(), true, -1, true, schema);
      int fieldDataLength = bytes.offset - currentFieldDataPos;
      return fieldDataLength;
    } finally {
      bytes.offset = startOffset;
    }
  }

  protected <RET> RET deserializeFieldTypedLoopAndReturn(
      BytesContainer bytes, String iFieldName, OImmutableSchema schema) {
    final byte[] field = iFieldName.getBytes();

    while (true) {
      int len = OVarIntSerializer.readAsInteger(bytes);

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
          Tuple<Integer, OType> pointerAndType = getPointerAndTypeFromCurrentPosition(bytes);
          int valuePos = pointerAndType.getFirstVal();
          OType type = pointerAndType.getSecondVal();

          if (valuePos == 0) return null;

          if (!match) continue;

          // find start of the next field offset so current field byte length can be calculated
          // actual field byte length is only needed for embedded fields
          int fieldDataLength = -1;
          if (type.isEmbedded()) {
            fieldDataLength = getEmbeddedFieldSize(bytes, valuePos, type, schema);
          }

          bytes.offset = valuePos;
          Object value = deserializeValue(bytes, type, null, false, fieldDataLength, false, schema);
          return (RET) value;
        }

        // skip Pointer and data type
        bytes.skip(len + OIntegerSerializer.INT_SIZE + 1);

      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = schema.getGlobalPropertyById(id);
        if (iFieldName.equals(prop.getName())) {
          final int valuePos = readInteger(bytes);
          OType type;
          if (prop.getType() != OType.ANY) type = prop.getType();
          else type = readOType(bytes, false);

          int fieldDataLength = -1;
          if (type.isEmbedded()) {
            fieldDataLength = getEmbeddedFieldSize(bytes, valuePos, type, schema);
          }

          if (valuePos == 0) return null;

          bytes.offset = valuePos;

          Object value = deserializeValue(bytes, type, null, false, fieldDataLength, false, schema);
          return (RET) value;
        }
        bytes.skip(OIntegerSerializer.INT_SIZE + (prop.getType() != OType.ANY ? 0 : 1));
      }
    }
  }

  @Override
  public <RET> RET deserializeFieldTyped(
      BytesContainer bytes,
      String iFieldName,
      boolean isEmbedded,
      OImmutableSchema schema,
      OPropertyEncryption encryption) {
    // SKIP CLASS NAME
    skipClassName(bytes);
    return deserializeFieldTypedLoopAndReturn(bytes, iFieldName, schema);
  }

  public Tuple<Integer, OType> getPointerAndTypeFromCurrentPosition(BytesContainer bytes) {
    int valuePos = readInteger(bytes);
    OType type = readOType(bytes, false);
    return new Tuple<>(valuePos, type);
  }

  @Override
  public void deserializeDebug(
      BytesContainer bytes,
      ODatabaseDocumentInternal db,
      ORecordSerializationDebug debugInfo,
      OImmutableSchema schema) {

    debugInfo.properties = new ArrayList<>();
    int last = 0;
    String fieldName;
    int valuePos;
    OType type;
    while (true) {
      ORecordSerializationDebugProperty debugProperty = new ORecordSerializationDebugProperty();
      OGlobalProperty prop = null;
      try {
        final int len = OVarIntSerializer.readAsInteger(bytes);
        if (len != 0) debugInfo.properties.add(debugProperty);
        if (len == 0) {
          // SCAN COMPLETED
          break;
        } else if (len > 0) {
          // PARSE FIELD NAME
          fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
          bytes.skip(len);

          Tuple<Integer, OType> valuePositionAndType = getPointerAndTypeFromCurrentPosition(bytes);
          valuePos = valuePositionAndType.getFirstVal();
          type = valuePositionAndType.getSecondVal();
        } else {
          // LOAD GLOBAL PROPERTY BY ID
          final int id = (len * -1) - 1;
          debugProperty.globalId = id;
          prop = schema.getGlobalPropertyById(id);
          valuePos = readInteger(bytes);
          debugProperty.valuePos = valuePos;
          if (prop != null) {
            fieldName = prop.getName();
            if (prop.getType() != OType.ANY) type = prop.getType();
            else type = readOType(bytes, false);
          } else {
            continue;
          }
        }
        debugProperty.name = fieldName;
        debugProperty.type = type;

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
}
