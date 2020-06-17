package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OConnectRequest implements OBinaryRequest<OConnectResponse> {
  private String username;
  private String password;
  private String driverName = OStorageRemote.DRIVER_NAME;
  private String driverVersion = OConstants.getRawVersion();
  private short protocolVersion = OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
  private String clientId = null;
  private String recordFormat = ORecordSerializerNetworkV37.NAME;
  private boolean tokenBased = true;
  private boolean supportPush = true;
  private boolean collectStats = true;

  public OConnectRequest(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public OConnectRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(driverName);
    network.writeString(driverVersion);
    network.writeShort(protocolVersion);
    network.writeString(clientId);

    network.writeString(recordFormat);
    network.writeBoolean(tokenBased);
    network.writeBoolean(supportPush);
    network.writeBoolean(collectStats);

    network.writeString(username);
    network.writeString(password);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {

    driverName = channel.readString();
    driverVersion = channel.readString();
    this.protocolVersion = channel.readShort();
    clientId = channel.readString();
    recordFormat = channel.readString();

    if (this.protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      tokenBased = channel.readBoolean();
    else tokenBased = false;

    if (this.protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_33) {
      supportPush = channel.readBoolean();
      collectStats = channel.readBoolean();
    } else {
      supportPush = true;
      collectStats = true;
    }

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

  public String getDriverName() {
    return driverName;
  }

  public String getDriverVersion() {
    return driverVersion;
  }

  public String getPassword() {
    return password;
  }

  public short getProtocolVersion() {
    return protocolVersion;
  }

  public String getUsername() {
    return username;
  }

  public String getRecordFormat() {
    return recordFormat;
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
    return executor.executeConnect(this);
  }
}
