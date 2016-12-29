package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

/**
 * Created by tglman on 28/12/16.
 */
public class OBeginTransactionResponse implements OBinaryResponse {

  private int txId;

  public OBeginTransactionResponse(int txId) {
    this.txId = txId;
  }

  public OBeginTransactionResponse() {
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    channel.writeInt(txId);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    txId = network.readInt();
  }

  public int getTxId() {
    return txId;
  }
}
