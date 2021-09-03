package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OOpenResponse implements OBinaryResponse {
  private int sessionId;
  private byte[] sessionToken;
  private int[] clusterIds;
  private String[] clusterNames;

  private byte[] distributedConfiguration;
  private String serverVersion;

  public OOpenResponse() {}

  public OOpenResponse(
      int sessionId,
      byte[] sessionToken,
      int[] clusterIds,
      String[] clusterNames,
      byte[] distriConf,
      String version) {
    this.sessionId = sessionId;
    this.sessionToken = sessionToken;
    this.clusterIds = clusterIds;
    this.clusterNames = clusterNames;
    this.distributedConfiguration = distriConf;
    this.serverVersion = version;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    if (protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      channel.writeBytes(sessionToken);

    OMessageHelper.writeClustersArray(
        channel, new ORawPair<>(clusterNames, clusterIds), protocolVersion);
    channel.writeBytes(distributedConfiguration);
    channel.writeString(serverVersion);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
    sessionToken = network.readBytes();
    final ORawPair<String[], int[]> clusters = OMessageHelper.readClustersArray(network);
    distributedConfiguration = network.readBytes();
    serverVersion = network.readString();
  }

  public int getSessionId() {
    return sessionId;
  }

  public byte[] getSessionToken() {
    return sessionToken;
  }

  public int[] getClusterIds() {
    return clusterIds;
  }

  public String[] getClusterNames() {
    return clusterNames;
  }

  public byte[] getDistributedConfiguration() {
    return distributedConfiguration;
  }
}
