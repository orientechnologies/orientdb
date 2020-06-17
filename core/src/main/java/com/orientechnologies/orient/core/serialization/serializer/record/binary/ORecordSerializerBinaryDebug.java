package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readString;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;

public class ORecordSerializerBinaryDebug extends ORecordSerializerBinaryV0 {

  public ORecordSerializationDebug deserializeDebug(
      final byte[] iSource, ODatabaseDocumentInternal db) {
    ORecordSerializationDebug debugInfo = new ORecordSerializationDebug();
    OImmutableSchema schema = ((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot();
    BytesContainer bytes = new BytesContainer(iSource);
    int version = readByte(bytes);

    if (ORecordSerializerBinary.INSTANCE.getSerializer(version).isSerializingClassNameByDefault()) {
      try {
        final String className = readString(bytes);
        debugInfo.className = className;
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
        return debugInfo;
      }
    }

    ORecordSerializerBinary.INSTANCE
        .getSerializer(version)
        .deserializeDebug(bytes, db, debugInfo, schema);
    return debugInfo;
  }
}
