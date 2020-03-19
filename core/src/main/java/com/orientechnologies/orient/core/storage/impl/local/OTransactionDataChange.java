package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OTransactionDataChange {
  private byte             type;
  private byte             recordType;
  private ORID             id;
  private Optional<byte[]> record;
  private int              version;
  private boolean          contentChanged;

  public OTransactionDataChange(ORecordOperation operation) {
    this.type = operation.type;
    ORecord rec = operation.getRecord();
    this.recordType = ORecordInternal.getRecordType(rec);
    this.id = rec.getIdentity();
    this.version = rec.getVersion();
    switch (operation.type) {
    case ORecordOperation.CREATED:
      this.record = Optional.of(ORecordSerializerNetworkDistributed.INSTANCE.toStream(rec));
      this.contentChanged = ORecordInternal.isContentChanged(rec);
      break;
    case ORecordOperation.UPDATED:
      if (recordType == ODocument.RECORD_TYPE) {
        record = Optional.of(ODocumentSerializerDelta.instance().serializeDelta((ODocument) rec));
      } else {
        record = Optional.of(ORecordSerializerNetworkDistributed.INSTANCE.toStream(rec));
      }
      this.contentChanged = ORecordInternal.isContentChanged(rec);
      break;
    case ORecordOperation.DELETED:
      break;
    }
  }

  private OTransactionDataChange() {

  }

  public void serialize(DataOutput output) throws IOException {
    output.writeByte(type);
    output.writeByte(recordType);
    output.writeInt(id.getClusterId());
    output.writeLong(id.getClusterPosition());
    if (record.isPresent()) {
      output.writeBoolean(true);
      output.writeInt(record.get().length);
      output.write(record.get(), 0, record.get().length);
    } else {
      output.writeBoolean(false);
    }
    output.writeInt(this.version);
    output.writeBoolean(this.contentChanged);
  }

  public static OTransactionDataChange deserialize(DataInput input) throws IOException {
    OTransactionDataChange change = new OTransactionDataChange();
    change.type = input.readByte();
    change.recordType = input.readByte();
    int cluster = input.readInt();
    long position = input.readLong();
    change.id = new ORecordId(cluster, position);
    boolean isThereRecord = input.readBoolean();
    if (isThereRecord) {
      int size = input.readInt();
      byte[] record = new byte[size];
      input.readFully(record);
      change.record = Optional.of(record);
    } else {
      change.record = Optional.empty();
    }
    change.version = input.readInt();
    change.contentChanged = input.readBoolean();
    return change;
  }
}
