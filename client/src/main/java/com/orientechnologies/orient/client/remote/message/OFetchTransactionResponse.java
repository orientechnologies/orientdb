package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by tglman on 30/12/16.
 */
public class OFetchTransactionResponse implements OBinaryResponse {

  private int                           txId;
  private List<ORecordOperationRequest> operations;
  private List<IndexChange>             indexChanges;

  public OFetchTransactionResponse() {

  }

  public OFetchTransactionResponse(int txId, Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> indexChanges) {
    this.txId = txId;
    this.indexChanges = new ArrayList<>();
    List<ORecordOperationRequest> netOperations = new ArrayList<>();
    for (ORecordOperation txEntry : operations) {
      if (txEntry.type == ORecordOperation.LOADED)
        continue;
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRecord().getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED:
        request.setRecord(txEntry.getRecord());
        request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
        break;
      }
      netOperations.add(request);
    }
    this.operations = netOperations;

    for (Map.Entry<String, OTransactionIndexChanges> change : indexChanges.entrySet()) {
      this.indexChanges.add(new IndexChange(change.getKey(), change.getValue()));
    }
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    channel.writeInt(txId);

    for (ORecordOperationRequest txEntry : operations) {
      OMessageHelper.writeTransactionEntry(channel, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    channel.writeByte((byte) 0);

    // SEND MANUAL INDEX CHANGES
    OMessageHelper.writeTransactionIndexChanges(channel, (ORecordSerializerNetwork) serializer, indexChanges);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    ORecordSerializerNetwork serializer = ORecordSerializerNetwork.INSTANCE;
    txId = network.readInt();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = network.readByte();
      if (hasEntry == 1) {
        ORecordOperationRequest entry = OMessageHelper.readTransactionEntry(network, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);

    // RECEIVE MANUAL INDEX CHANGES
    this.indexChanges = OMessageHelper.readTransactionIndexChanges(network, (ORecordSerializerNetwork) serializer);
  }

  public int getTxId() {
    return txId;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public List<IndexChange> getIndexChanges() {
    return indexChanges;
  }
}
