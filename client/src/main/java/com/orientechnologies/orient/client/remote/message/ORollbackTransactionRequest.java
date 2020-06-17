package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 30/01/17. */
public class ORollbackTransactionRequest implements OBinaryRequest<ORollbackTransactionResponse> {
  private int txId;

  public ORollbackTransactionRequest() {}

  public ORollbackTransactionRequest(int id) {
    this.txId = id;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeInt(txId);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    txId = channel.readInt();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_ROLLBACK;
  }

  @Override
  public ORollbackTransactionResponse createResponse() {
    return new ORollbackTransactionResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeRollback(this);
  }

  @Override
  public String getDescription() {
    return "Transaction Rollback";
  }
}
