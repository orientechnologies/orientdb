package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Created by tglman on 28/12/16. */
public class OBeginTransactionResponse implements OBinaryResponse {

  private int txId;
  private Map<ORID, ORID> updatedIds;

  public OBeginTransactionResponse(int txId, Map<ORID, ORID> updatedIds) {
    this.txId = txId;
    this.updatedIds = updatedIds;
  }

  public OBeginTransactionResponse() {}

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(txId);
    channel.writeInt(updatedIds.size());
    for (Map.Entry<ORID, ORID> ids : updatedIds.entrySet()) {
      channel.writeRID(ids.getKey());
      channel.writeRID(ids.getValue());
    }
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    txId = network.readInt();
    int size = network.readInt();
    updatedIds = new HashMap<>(size);
    while (size-- > 0) {
      ORID key = network.readRID();
      ORID value = network.readRID();
      updatedIds.put(key, value);
    }
  }

  public int getTxId() {
    return txId;
  }

  public Map<ORID, ORID> getUpdatedIds() {
    return updatedIds;
  }
}
