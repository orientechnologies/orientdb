package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ORecordSerializerBinary implements ORecordSerializer {

  public static final String    NAME                = "ORecordSerializerBinary";
  private static final byte     LAST_RECORD_VERSION = 0;

  private ODocumentSerializer[] serializerByVersion;

  public ORecordSerializerBinary() {
    serializerByVersion = new ODocumentSerializer[1];
    serializerByVersion[0] = new ORecordSerializerBinaryV0();
  }

  private void checkTypeODocument(ORecordInternal<?> iRecord) {
    if (!(iRecord instanceof ODocument)) {
      throw new UnsupportedOperationException("The " + ORecordSerializerBinary.NAME + " don't support record of type "
          + iRecord.getClass().getName());
    }
  }

  @Override
  public ORecordInternal<?> fromStream(byte[] iSource, ORecordInternal<?> iRecord, String[] iFields) {
    if (iRecord == null)
      iRecord = new ODocument();
    else
      checkTypeODocument(iRecord);

    BytesContainer container = new BytesContainer(iSource);
    container.read(1);
    serializerByVersion[iSource[0]].deserialize((ODocument) iRecord, container);
    return iRecord;
  }

  @Override
  public byte[] toStream(ORecordInternal<?> iSource, boolean iOnlyDelta) {
    checkTypeODocument(iSource);
    BytesContainer container = new BytesContainer();
    container.bytes[container.alloc((short) 1)] = LAST_RECORD_VERSION;
    serializerByVersion[LAST_RECORD_VERSION].serialize((ODocument) iSource, container);
    return container.bytes;
  }

}
