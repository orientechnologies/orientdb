package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.util.Set;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationSetThreadLocal;

public class ORecordSerializerBinary implements ORecordSerializer {

  public static final String    NAME                = "ORecordSerializerBinary";
  private static final byte     CURRENT_RECORD_VERSION = 0;

  private ODocumentSerializer[] serializerByVersion;

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
  public ORecordInternal<?> fromStream(final byte[] iSource, ORecordInternal<?> iRecord, final String[] iFields) {
    if (iSource.length == 0)
      return iRecord;
    if (iRecord == null)
      iRecord = new ODocument();
    else
      checkTypeODocument(iRecord);

    BytesContainer container = new BytesContainer(iSource);
    container.skip(1);
    serializerByVersion[iSource[0]].deserialize((ODocument) iRecord, container);
    return iRecord;
  }

  @Override
  public byte[] toStream(final ORecordInternal<?> iSource, final boolean iOnlyDelta) {
    checkTypeODocument(iSource);
    if (!checkRecursion((ODocument) iSource))
      return null;
    BytesContainer container = new BytesContainer();
    int pos = container.alloc(1);
    container.bytes[pos] = CURRENT_RECORD_VERSION;
    serializerByVersion[CURRENT_RECORD_VERSION].serialize((ODocument) iSource, container);
    removeCheck((ODocument) iSource);
    return container.fitBytes();
  }

  private void checkTypeODocument(final ORecordInternal<?> iRecord) {
    if (!(iRecord instanceof ODocument)) {
      throw new UnsupportedOperationException("The " + ORecordSerializerBinary.NAME + " don't support record of type "
          + iRecord.getClass().getName());
    }
  }

  private boolean checkRecursion(final ODocument document) {
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
