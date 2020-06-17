package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OConnectResponse implements OBinaryResponse {
  private int sessionId;
  private byte[] sessionToken;

  public OConnectResponse() {}

  public OConnectResponse(int sessionId, byte[] token) {
    this.sessionId = sessionId;
    this.sessionToken = token;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    if (protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      channel.writeBytes(sessionToken);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
    sessionToken = network.readBytes();
    session
        .getServerSession(((OChannelBinaryAsynchClient) network).getServerURL())
        .setSession(sessionId, sessionToken);
  }
}
