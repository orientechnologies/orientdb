package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.util.ArrayList;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ORecordSerializerBinaryDebug extends ORecordSerializerBinaryV0 {

  public ORecordSerializationDebug deserializeDebug(final byte[] iSource, ODatabaseDocumentTx db) {
    ORecordSerializationDebug debugInfo = new ORecordSerializationDebug();
    OImmutableSchema schema = ((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot();
    BytesContainer bytes = new BytesContainer(iSource);
    if (bytes.bytes[0] != 0)
      throw new OException("Unsupported binary serialization version");
    bytes.skip(1);
    try {
      final String className = readString(bytes);
      debugInfo.className = className;
    } catch (RuntimeException ex) {
      debugInfo.readingFailure = true;
      debugInfo.readingException = ex;
      debugInfo.failPosition = bytes.offset;
      return debugInfo;
    }

    debugInfo.properties = new ArrayList<ORecordSerializationDebugProperty>();
    int last = 0;
    String fieldName;
    int valuePos;
    OType type;
    while (true) {
      ORecordSerializationDebugProperty debugProperty = new ORecordSerializationDebugProperty();
      OGlobalProperty prop = null;
      try {
        final int len = OVarIntSerializer.readAsInteger(bytes);
        if (len != 0)
          debugInfo.properties.add(debugProperty);
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
          final int id = (len * -1) - 1;
          debugProperty.globalId = id;
          prop = schema.getGlobalPropertyById(id);
          fieldName = prop.getName();
          valuePos = readInteger(bytes);
          if (prop.getType() != OType.ANY)
            type = prop.getType();
          else
            type = readOType(bytes);
        }
        debugProperty.name = fieldName;
        debugProperty.type = type;

        if (valuePos != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = valuePos;
          try {
            debugProperty.value = readSingleValue(bytes, type, new ODocument());
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
        return debugInfo;
      }
    }

    return debugInfo;
  }
}
