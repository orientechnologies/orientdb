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

public class OFreezeDatabaseRequest implements OBinaryRequest<OFreezeDatabaseResponse> {
  private String name;
  private String type;

  public OFreezeDatabaseRequest(String name, String type) {
    super();
    this.name = name;
    this.type = type;
  }

  public OFreezeDatabaseRequest() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(name);
    network.writeString(type);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    name = channel.readString();
    type = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DB_FREEZE;
  }

  @Override
  public String requiredServerRole() {
    return "database.freeze";
  }

  @Override
  public boolean requireServerUser() {
    return true;
  }

  @Override
  public String getDescription() {
    return "Freeze Database";
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public OFreezeDatabaseResponse createResponse() {
    return new OFreezeDatabaseResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeFreezeDatabase(this);
  }
}
