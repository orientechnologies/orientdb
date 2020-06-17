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

public class OServerInfoRequest implements OBinaryRequest<OServerInfoResponse> {

  public OServerInfoRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {}

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {}

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_SERVER_INFO;
  }

  @Override
  public String requiredServerRole() {
    return "server.info";
  }

  @Override
  public boolean requireServerUser() {
    return true;
  }

  @Override
  public String getDescription() {
    return "Server Info";
  }

  @Override
  public OServerInfoResponse createResponse() {
    return new OServerInfoResponse();
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor ex) {
    return ex.executeServerInfo(this);
  }
}
