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

public class OReleaseDatabaseRequest implements OBinaryRequest<OReleaseDatabaseResponse> {
  private String name;
  private String storageType;

  public OReleaseDatabaseRequest(String name, String storageType) {
    this.name = name;
    this.storageType = storageType;
  }

  public OReleaseDatabaseRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(name);
    network.writeString(storageType);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
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
  public boolean requireDatabaseSession() {
    return false;
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
