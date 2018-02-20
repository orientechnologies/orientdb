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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.*;
import java.util.Map.Entry;

public class ORecordSerializerBinaryV1 extends ORecordSerializerBinaryV0{  
   
  @Override
  public void deserializePartial(final ODocument document, final BytesContainer bytes, final String[] iFields, boolean deserializeClassName) {
    
    if (deserializeClassName){
      final String className = readString(bytes);
      if (className.length() != 0)
        ODocumentInternal.fillClassNameIfNeeded(document, className);
    }

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

  @Override
  public OBinaryField deserializeField(final BytesContainer bytes, final OClass iClass, final String iFieldName, boolean deserializeClassName) {
    // SKIP CLASS NAME
    if (deserializeClassName){
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }

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
  public void deserialize(final ODocument document, final BytesContainer bytes, boolean deserializeClassName) {
    
    if (deserializeClassName){
      final String className = readString(bytes);
      if (className.length() != 0)
        ODocumentInternal.fillClassNameIfNeeded(document, className);
    }

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
  public String[] getFieldNames(ODocument reference, final BytesContainer bytes,  boolean readClassName) {
    // SKIP CLASS NAME
    if (readClassName){
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }

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
  public void serialize(final ODocument document, final BytesContainer bytes, final boolean iClassOnly, boolean serializeClassName) {

    final OClass clazz = serializeClass(document, bytes, serializeClassName);
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
        int id = docEntry.property.getId();
        OVarIntSerializer.write(bytes, (id + 1) * -1);
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
   
}
