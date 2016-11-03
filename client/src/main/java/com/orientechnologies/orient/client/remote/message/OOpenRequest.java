package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OOpenRequest implements OBinaryRequest<OOpenResponse> {
  private String  driverName      = OStorageRemote.DRIVER_NAME;
  private String  driverVersion   = OConstants.ORIENT_VERSION;
  private short   protocolVersion = OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
  private String  clientId        = null;
  private String  recordFormat    = ORecordSerializerFactory.instance().getDefaultRecordSerializer().toString();
  private boolean useToken        = true;
  private boolean supportsPush    = true;
  private boolean collectStats    = true;
  private String  databaseName;
  private String  userName;
  private String  userPassword;
  private String  dbType;

  public OOpenRequest(String databaseName, String userName, String userPassword) {
    this.databaseName = databaseName;
    this.userName = userName;
    this.userPassword = userPassword;
  }

  public OOpenRequest() {

  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(driverName);
    network.writeString(driverVersion);
    network.writeShort((short) protocolVersion);
    network.writeString(clientId);
    network.writeString(recordFormat);
    network.writeBoolean(useToken);
    network.writeBoolean(supportsPush);
    network.writeBoolean(collectStats);
    network.writeString(databaseName);
    network.writeString(userName);
    network.writeString(userPassword);
  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {

    driverName = channel.readString();
    driverVersion = channel.readString();
    this.protocolVersion = channel.readShort();
    clientId = channel.readString();
    this.recordFormat = channel.readString();

    if (this.protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      useToken = channel.readBoolean();
    else
      useToken = false;
    if (this.protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_33) {
      supportsPush = channel.readBoolean();
      collectStats = channel.readBoolean();
    } else {
      supportsPush = true;
      collectStats = true;
    }
    databaseName = channel.readString();
    if (this.protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_32)
      dbType = channel.readString();
    userName = channel.readString();
    userPassword = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DB_OPEN;
  }

  @Override
  public String getDescription() {
    return "Open Database";
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getUserName() {
    return userName;
  }

  public String getUserPassword() {
    return userPassword;
  }

  public String getDriverName() {
    return driverName;
  }

  public String getDriverVersion() {
    return driverVersion;
  }

  public String getClientId() {
    return clientId;
  }

  public short getProtocolVersion() {
    return protocolVersion;
  }

  public String getRecordFormat() {
    return recordFormat;
  }

  public boolean isCollectStats() {
    return collectStats;
  }

  public boolean isSupportsPush() {
    return supportsPush;
  }

  public boolean isUseToken() {
    return useToken;
  }

  @Override
  public OOpenResponse createResponse() {
    return new OOpenResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeDatabaseOpen(this);
  }

}