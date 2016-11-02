package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OReleaseDatabaseRequest implements OBinaryRequest<OReleaseDatabaseResponse> {
  private String name;
  private String storageType;

  public OReleaseDatabaseRequest(String name, String storageType) {
    this.name = name;
    this.storageType = storageType;
  }

  public OReleaseDatabaseRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(name);
    network.writeString(storageType);
  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    name = channel.readString();
    storageType = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DB_RELEASE;
  }

  @Override
  public String requiredServerRole() {
    return "database.release";
  }

  @Override
  public boolean requireServerUser() {
    return true;
  }

  @Override
  public String getDescription() {
    return "Release Database";
  }

  public String getName() {
    return name;
  }

  public String getStorageType() {
    return storageType;
  }

  @Override
  public OReleaseDatabaseResponse createResponse() {
    return new OReleaseDatabaseResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeReleaseDatabase(this);
  }

}