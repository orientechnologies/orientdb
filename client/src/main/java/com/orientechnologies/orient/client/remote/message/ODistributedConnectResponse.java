package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 21/06/17. */
public class ODistributedConnectResponse implements OBinaryResponse {

  private int sessionId;
  private byte[] token;
  private int distributedProtocolVersion;

  public ODistributedConnectResponse(int sessionId, byte[] token, int distributedProtocolVersion) {
    this.sessionId = sessionId;
    this.token = token;
    this.distributedProtocolVersion = distributedProtocolVersion;
  }

  public ODistributedConnectResponse() {}

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    channel.writeInt(distributedProtocolVersion);
    channel.writeBytes(token);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    this.sessionId = network.readInt();
    distributedProtocolVersion = network.readInt();
    token = network.readBytes();
  }

  public int getDistributedProtocolVersion() {
    return distributedProtocolVersion;
  }

  public int getSessionId() {
    return sessionId;
  }

  public byte[] getToken() {
    return token;
  }
}
