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

public class OConnect37Request implements OBinaryRequest<OConnectResponse> {
  private String username;
  private String password;
  private String  clientId     = null;
  private boolean tokenBased   = true;
  private boolean supportPush  = true;
  private boolean collectStats = true;

  public OConnect37Request(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public OConnect37Request() {
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(clientId);

    network.writeBoolean(tokenBased);
    network.writeBoolean(supportPush);
    network.writeBoolean(collectStats);

    network.writeString(username);
    network.writeString(password);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {

    clientId = channel.readString();

    tokenBased = channel.readBoolean();

    supportPush = channel.readBoolean();
    collectStats = channel.readBoolean();

    username = channel.readString();
    password = channel.readString();

  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_CONNECT;
  }

  @Override
  public String getDescription() {
    return "Connect";
  }

  public String getClientId() {
    return clientId;
  }



  public String getPassword() {
    return password;
  }


  public String getUsername() {
    return username;
  }


  public boolean isCollectStats() {
    return collectStats;
  }

  public boolean isSupportPush() {
    return supportPush;
  }

  public boolean isTokenBased() {
    return tokenBased;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public OConnectResponse createResponse() {
    return new OConnectResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeConnect37(this);
  }

}