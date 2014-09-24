package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationSetThreadLocal;

public class ORecordSerializerBinary implements ORecordSerializer {

  public static final String                  NAME                   = "ORecordSerializerBinary";
  public static final ORecordSerializerBinary INSTANCE               = new ORecordSerializerBinary();
  private static final byte                   CURRENT_RECORD_VERSION = 0;

  private ODocumentSerializer[]               serializerByVersion;

  public ORecordSerializerBinary() {
    serializerByVersion = new ODocumentSerializer[1];
    serializerByVersion[0] = new ORecordSerializerBinaryV0();
  }

  @Override
  public int getCurrentVersion() {
    return CURRENT_RECORD_VERSION;
  }

  @Override
  public int getMinSupportedVersion() {
    return CURRENT_RECORD_VERSION;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public ORecord fromStream(final byte[] iSource, ORecord iRecord, final String[] iFields) {
    if (iSource.length == 0)
      return iRecord;
    if (iRecord == null)
      iRecord = new ODocument();
    else
      checkTypeODocument(iRecord);

    BytesContainer container = new BytesContainer(iSource);
    container.skip(1);
    serializerByVersion[iSource[0]].deserialize((ODocument) iRecord, container);
    ODocumentInternal.clearSource((ODocument) iRecord);
    return iRecord;
  }

  @Override
  public byte[] toStream(final ORecord iSource, final boolean iOnlyDelta) {
    checkTypeODocument(iSource);
    if (!OSerializationSetThreadLocal.checkAndAdd((ODocument) iSource))
      return null;
    BytesContainer container = new BytesContainer();
    int pos = container.alloc(1);
    container.bytes[pos] = CURRENT_RECORD_VERSION;
    serializerByVersion[CURRENT_RECORD_VERSION].serialize((ODocument) iSource, container);
    OSerializationSetThreadLocal.removeCheck((ODocument) iSource);
    return container.fitBytes();
  }

  private void checkTypeODocument(final ORecord iRecord) {
    if (!(iRecord instanceof ODocument)) {
      throw new UnsupportedOperationException("The " + ORecordSerializerBinary.NAME + " don't support record of type "
          + iRecord.getClass().getName());
    }
  }

}
