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

/** Created by tglman on 21/06/17. */
public class ODistributedConnectRequest implements OBinaryRequest<ODistributedConnectResponse> {

  private int distributedProtocolVersion;
  private String username;
  private String password;

  public ODistributedConnectRequest() {}

  public ODistributedConnectRequest(
      int distributedProtocolVersion, String username, String password) {
    this.distributedProtocolVersion = distributedProtocolVersion;
    this.username = username;
    this.password = password;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeInt(distributedProtocolVersion);
    network.writeString(username);
    network.writeString(password);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    distributedProtocolVersion = channel.readInt();
    username = channel.readString();
    password = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.DISTRIBUTED_CONNECT;
  }

  @Override
  public ODistributedConnectResponse createResponse() {
    return new ODistributedConnectResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeDistributedConnect(this);
  }

  @Override
  public String getDescription() {
    return "distributed connect";
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public int getDistributedProtocolVersion() {
    return distributedProtocolVersion;
  }
}
