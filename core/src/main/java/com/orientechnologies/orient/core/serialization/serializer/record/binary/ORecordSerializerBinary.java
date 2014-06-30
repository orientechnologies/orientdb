package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.util.Set;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationSetThreadLocal;

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
    if (iSource.length == 0)
      return iRecord;
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
    if (!checkRecursion((ODocument) iSource))
      return null;
    BytesContainer container = new BytesContainer();
    int pos = container.alloc(1);
    container.bytes[pos] = LAST_RECORD_VERSION;
    serializerByVersion[LAST_RECORD_VERSION].serialize((ODocument) iSource, container);
    removeCheck((ODocument) iSource);
    return container.fitBytes();
  }

  private boolean checkRecursion(ODocument document) {
    Set<ODocument> iMarshalledRecords = OSerializationSetThreadLocal.INSTANCE.get();
    // CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
    if (iMarshalledRecords.contains(document)) {
      return false;
    } else
      iMarshalledRecords.add((ODocument) document);
    return true;
  }

  private void removeCheck(ODocument document) {
    OSerializationSetThreadLocal.INSTANCE.get().remove(document);
  }

}
