package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperation38Response;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Created by tglman on 30/12/16. */
public class OFetchTransaction38Response implements OBinaryResponse {

  private int txId;
  private List<ORecordOperation38Response> operations;
  private List<IndexChange> indexChanges;

  public OFetchTransaction38Response() {}

  public OFetchTransaction38Response(
      int txId,
      Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> indexChanges,
      Map<ORID, ORID> updatedRids,
      ODatabaseDocumentInternal database) {
    // In some cases the reference are update twice is not yet possible to guess what is the id in
    // the client
    Map<ORID, ORID> reversed =
        updatedRids.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    this.txId = txId;
    this.indexChanges = new ArrayList<>();
    List<ORecordOperation38Response> netOperations = new ArrayList<>();
    for (ORecordOperation txEntry : operations) {
      if (txEntry.type == ORecordOperation.LOADED) continue;
      ORecordOperation38Response request = new ORecordOperation38Response();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRID());
      ORID oldID = reversed.get(txEntry.getRID());
      request.setOldId(oldID != null ? oldID : txEntry.getRID());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      if (txEntry.type == ORecordOperation.UPDATED && txEntry.getRecord() instanceof ODocument) {
        ODocument doc = (ODocument) txEntry.getRecord();
        OStorageOperationResult<ORawBuffer> result =
            database.getStorage().readRecord((ORecordId) doc.getIdentity(), "", false, false, null);
        ODocument docFromPersistence = new ODocument(doc.getIdentity());
        docFromPersistence.fromStream(result.getResult().getBuffer());
        request.setOriginal(
            ORecordSerializerNetworkV37Client.INSTANCE.toStream(docFromPersistence));
        ODocumentSerializerDelta delta = ODocumentSerializerDelta.instance();
        request.setRecord(delta.serializeDelta(doc));
      } else {
        request.setRecord(ORecordSerializerNetworkV37Client.INSTANCE.toStream(txEntry.getRecord()));
      }
      request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
      netOperations.add(request);
    }
    this.operations = netOperations;

    for (Map.Entry<String, OTransactionIndexChanges> change : indexChanges.entrySet()) {
      this.indexChanges.add(new IndexChange(change.getKey(), change.getValue()));
    }
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(txId);

    for (ORecordOperation38Response txEntry : operations) {
      writeTransactionEntry(channel, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    channel.writeByte((byte) 0);

    // SEND MANUAL INDEX CHANGES
    OMessageHelper.writeTransactionIndexChanges(
        channel, (ORecordSerializerNetworkV37) serializer, indexChanges);
  }

  static void writeTransactionEntry(
      final OChannelDataOutput iNetwork,
      final ORecordOperation38Response txEntry,
      ORecordSerializer serializer)
      throws IOException {
    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeRID(txEntry.getId());
    iNetwork.writeRID(txEntry.getOldId());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case ORecordOperation.CREATED:
        iNetwork.writeBytes(txEntry.getRecord());
        break;

      case ORecordOperation.UPDATED:
        iNetwork.writeVersion(txEntry.getVersion());
        iNetwork.writeBytes(txEntry.getOriginal());
        iNetwork.writeBytes(txEntry.getRecord());
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case ORecordOperation.DELETED:
        iNetwork.writeVersion(txEntry.getVersion());
        iNetwork.writeBytes(txEntry.getRecord());
        break;
    }
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    ORecordSerializerNetworkV37Client serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    txId = network.readInt();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = network.readByte();
      if (hasEntry == 1) {
        ORecordOperation38Response entry = readTransactionEntry(network, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);

    // RECEIVE MANUAL INDEX CHANGES
    this.indexChanges = OMessageHelper.readTransactionIndexChanges(network, serializer);
  }

  static ORecordOperation38Response readTransactionEntry(
      OChannelDataInput channel, ORecordSerializer ser) throws IOException {
    ORecordOperation38Response entry = new ORecordOperation38Response();
    entry.setType(channel.readByte());
    entry.setId(channel.readRID());
    entry.setOldId(channel.readRID());
    entry.setRecordType(channel.readByte());
    switch (entry.getType()) {
      case ORecordOperation.CREATED:
        entry.setRecord(channel.readBytes());
        break;
      case ORecordOperation.UPDATED:
        entry.setVersion(channel.readVersion());
        entry.setOriginal(channel.readBytes());
        entry.setRecord(channel.readBytes());
        entry.setContentChanged(channel.readBoolean());
        break;
      case ORecordOperation.DELETED:
        entry.setVersion(channel.readVersion());
        entry.setRecord(channel.readBytes());
        break;
      default:
        break;
    }
    return entry;
  }

  public int getTxId() {
    return txId;
  }

  public List<ORecordOperation38Response> getOperations() {
    return operations;
  }

  public List<IndexChange> getIndexChanges() {
    return indexChanges;
  }
}
