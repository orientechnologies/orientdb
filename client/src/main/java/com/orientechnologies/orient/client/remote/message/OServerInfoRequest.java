package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OServerInfoRequest implements OBinaryRequest<OServerInfoResponse> {

  public OServerInfoRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {

  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {

  }

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