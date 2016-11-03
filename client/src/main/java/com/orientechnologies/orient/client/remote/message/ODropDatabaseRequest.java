package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class ODropDatabaseRequest implements OBinaryRequest<ODropDatabaseResponse> {
  private String databaseName;
  private String storageType;

  public ODropDatabaseRequest(String databaseName, String storageType) {
    this.databaseName = databaseName;
    this.storageType = storageType;
  }

  public ODropDatabaseRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(databaseName);
    network.writeString(storageType);
  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    databaseName = channel.readString();
    storageType = channel.readString();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getStorageType() {
    return storageType;
  }

  @Override
  public boolean requireServerUser() {
    return true;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public String requiredServerRole() {
    return "database.drop";
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DB_DROP;
  }

  @Override
  public String getDescription() {
    return "Drop Database";
  }

  @Override
  public ODropDatabaseResponse createResponse() {
    return new ODropDatabaseResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor ex) {
    return ex.executeDropDatabase(this);
  }

}