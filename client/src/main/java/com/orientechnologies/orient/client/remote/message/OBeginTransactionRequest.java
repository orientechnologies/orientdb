package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.*;

public class OBeginTransactionRequest implements OBinaryRequest<OBinaryResponse> {

  public class IndexChange {

    public IndexChange(String name, OTransactionIndexChanges keyChanges) {
      this.name = name;
      this.keyChanges = keyChanges;
    }

    private String                   name;
    private OTransactionIndexChanges keyChanges;

    public String getName() {
      return name;
    }

    public OTransactionIndexChanges getKeyChanges() {
      return keyChanges;
    }
  }

  private int                           txId;
  private boolean                       usingLong;
  private List<ORecordOperationRequest> operations;
  private List<IndexChange>             changes;

  public OBeginTransactionRequest(int txId, boolean usingLong, Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> changes) {
    super();
    this.txId = txId;
    this.usingLong = usingLong;
    this.changes = new ArrayList<>();

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

    for (Map.Entry<String, OTransactionIndexChanges> change : changes.entrySet()) {
      this.changes.add(new IndexChange(change.getKey(), change.getValue()));
    }

  }

  public OBeginTransactionRequest() {
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    //from 3.0 the the serializer is bound to the protocol
    ORecordSerializerNetwork serializer = ORecordSerializerNetwork.INSTANCE;

    network.writeInt(txId);
    network.writeBoolean(usingLong);

    for (ORecordOperationRequest txEntry : operations) {
      OMessageHelper.writeTransactionEntry(network, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    network.writeByte((byte) 0);

    // SEND MANUAL INDEX CHANGES
    network.writeInt(changes.size());
    for (IndexChange indexChange : changes) {
      network.writeString(indexChange.name);
      network.writeBoolean(indexChange.keyChanges.cleared);
      if (!indexChange.keyChanges.cleared) {
        network.writeInt(indexChange.keyChanges.changesPerKey.size());
        for (OTransactionIndexChangesPerKey change : indexChange.keyChanges.changesPerKey.values()) {
          OType type = OType.getTypeByValue(change.key);
          byte[] value = ((ORecordSerializerNetwork) serializer).serializeValue(change.key, type);
          network.writeByte((byte) type.getId());
          network.writeBytes(value);
          network.writeInt(change.entries.size());
          for (OTransactionIndexEntry perKeyChange : change.entries) {
            network.writeInt(perKeyChange.operation.ordinal());
            network.writeRID(perKeyChange.value.getIdentity());
          }
        }
      }
    }
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    assert serializer instanceof ORecordSerializerNetwork;
    txId = channel.readInt();
    usingLong = channel.readBoolean();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        ORecordOperationRequest entry = OMessageHelper.readTransactionEntry(channel, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);

    // RECEIVE MANUAL INDEX CHANGES
    changes = new ArrayList<>();
    int val = channel.readInt();
    while (val-- > 0) {
      String indexName = channel.readString();
      boolean cleared = channel.readBoolean();
      OTransactionIndexChanges entry = new OTransactionIndexChanges();
      entry.cleared = cleared;
      if (!cleared) {
        int changeCount = channel.readInt();
        NavigableMap<Object, OTransactionIndexChangesPerKey> entries = new TreeMap<>();
        while (changeCount-- > 0) {
          byte bt = channel.readByte();
          OType type = OType.getById(bt);
          Object key = ((ORecordSerializerNetwork) serializer).deserializeValue(channel.readBytes(), type);
          OTransactionIndexChangesPerKey changesPerKey = new OTransactionIndexChangesPerKey(key);
          int keyChangeCount = channel.readInt();
          while (keyChangeCount-- > 0) {
            int op = channel.readInt();
            ORecordId id = channel.readRID();
            changesPerKey.add(id, OPERATION.values()[op]);
          }
          entries.put(changesPerKey.key, changesPerKey);
        }
        entry.changesPerKey = entries;
      }
      changes.add(new IndexChange(indexName, entry));
    }
  }

  @Override
  public byte getCommand() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public OBinaryResponse createResponse() {
    return new OBeginTransactionResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeBeginTransaction(this);
  }

  @Override
  public String getDescription() {
    return "Begin Transaction";
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public List<IndexChange> getChanges() {
    return changes;
  }

  public int getTxId() {
    return txId;
  }

  public boolean isUsingLong() {
    return usingLong;
  }
}
