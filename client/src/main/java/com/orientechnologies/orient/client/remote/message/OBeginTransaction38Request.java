package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OBeginTransaction38Request implements OBinaryRequest<OBeginTransactionResponse> {

  private int txId;
  private boolean usingLog;
  private boolean hasContent;
  private List<ORecordOperationRequest> operations;
  private List<IndexChange> indexChanges;

  public OBeginTransaction38Request(
      int txId,
      boolean hasContent,
      boolean usingLog,
      Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> indexChanges) {
    super();
    this.txId = txId;
    this.hasContent = hasContent;
    this.usingLog = usingLog;
    this.indexChanges = new ArrayList<>();
    this.operations = new ArrayList<>();

    if (hasContent) {
      for (ORecordOperation txEntry : operations) {
        if (txEntry.type == ORecordOperation.LOADED) continue;
        ORecordOperationRequest request = new ORecordOperationRequest();
        request.setType(txEntry.type);
        request.setVersion(txEntry.getRecord().getVersion());
        request.setId(txEntry.getRecord().getIdentity());
        request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
        switch (txEntry.type) {
          case ORecordOperation.CREATED:
            request.setRecord(
                ORecordSerializerNetworkV37Client.INSTANCE.toStream(txEntry.getRecord()));
            request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
            break;
          case ORecordOperation.UPDATED:
            if (ODocument.RECORD_TYPE == ORecordInternal.getRecordType(txEntry.getRecord())) {
              request.setRecordType(ODocumentSerializerDelta.DELTA_RECORD_TYPE);
              ODocumentSerializerDelta delta = ODocumentSerializerDelta.instance();
              request.setRecord(delta.serializeDelta((ODocument) txEntry.getRecord()));
            } else {
              request.setRecord(ORecordSerializerNetworkV37.INSTANCE.toStream(txEntry.getRecord()));
            }
            request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
            break;
        }
        this.operations.add(request);
      }

      for (Map.Entry<String, OTransactionIndexChanges> change : indexChanges.entrySet()) {
        this.indexChanges.add(new IndexChange(change.getKey(), change.getValue()));
      }
    }
  }

  public OBeginTransaction38Request() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    // from 3.0 the the serializer is bound to the protocol
    ORecordSerializerNetworkV37Client serializer = ORecordSerializerNetworkV37Client.INSTANCE;

    network.writeInt(txId);
    network.writeBoolean(hasContent);
    network.writeBoolean(usingLog);
    if (hasContent) {
      for (ORecordOperationRequest txEntry : operations) {
        writeTransactionEntry(network, txEntry);
      }

      // END OF RECORD ENTRIES
      network.writeByte((byte) 0);

      // SEND MANUAL INDEX CHANGES
      OMessageHelper.writeTransactionIndexChanges(network, serializer, indexChanges);
    }
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    txId = channel.readInt();
    hasContent = channel.readBoolean();
    usingLog = channel.readBoolean();
    operations = new ArrayList<>();
    if (hasContent) {
      byte hasEntry;
      do {
        hasEntry = channel.readByte();
        if (hasEntry == 1) {
          ORecordOperationRequest entry = readTransactionEntry(channel);
          operations.add(entry);
        }
      } while (hasEntry == 1);

      // RECEIVE MANUAL INDEX CHANGES
      this.indexChanges =
          OMessageHelper.readTransactionIndexChanges(
              channel, (ORecordSerializerNetworkV37) serializer);
    } else {
      this.indexChanges = new ArrayList<>();
    }
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_BEGIN;
  }

  @Override
  public OBeginTransactionResponse createResponse() {
    return new OBeginTransactionResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeBeginTransaction38(this);
  }

  @Override
  public String getDescription() {
    return "Begin Transaction";
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public List<IndexChange> getIndexChanges() {
    return indexChanges;
  }

  public int getTxId() {
    return txId;
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public boolean isHasContent() {
    return hasContent;
  }

  static ORecordOperationRequest readTransactionEntry(OChannelDataInput channel)
      throws IOException {
    ORecordOperationRequest entry = new ORecordOperationRequest();
    entry.setType(channel.readByte());
    entry.setId(channel.readRID());
    entry.setRecordType(channel.readByte());
    switch (entry.getType()) {
      case ORecordOperation.CREATED:
        entry.setRecord(channel.readBytes());
        break;
      case ORecordOperation.UPDATED:
        entry.setVersion(channel.readVersion());
        entry.setRecord(channel.readBytes());
        entry.setContentChanged(channel.readBoolean());
        break;
      case ORecordOperation.DELETED:
        entry.setVersion(channel.readVersion());
        break;
      default:
        break;
    }
    return entry;
  }

  static void writeTransactionEntry(
      final OChannelDataOutput iNetwork, final ORecordOperationRequest txEntry) throws IOException {
    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeRID(txEntry.getId());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case ORecordOperation.CREATED:
        iNetwork.writeBytes(txEntry.getRecord());
        break;

      case ORecordOperation.UPDATED:
        iNetwork.writeVersion(txEntry.getVersion());
        iNetwork.writeBytes(txEntry.getRecord());
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case ORecordOperation.DELETED:
        iNetwork.writeVersion(txEntry.getVersion());
        break;
    }
  }
}
