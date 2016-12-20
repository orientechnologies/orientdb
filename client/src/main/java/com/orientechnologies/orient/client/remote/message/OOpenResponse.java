package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.Collection;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

public class OOpenResponse implements OBinaryResponse {
  private int        sessionId;
  private byte[]     sessionToken;
  private OCluster[] clusterIds;
  private byte[]     distributedConfiguration;
  private String     serverVersion;

  public OOpenResponse() {
  }

  public OOpenResponse(int sessionId, byte[] sessionToken, Collection<? extends OCluster> clusters, byte[] distriConf,
      String version) {
    this.sessionId = sessionId;
    this.sessionToken = sessionToken;
    this.clusterIds = clusters.toArray(new OCluster[clusters.size()]);
    this.distributedConfiguration = distriConf;
    this.serverVersion = version;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeInt(sessionId);
    if (protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      channel.writeBytes(sessionToken);

    OMessageHelper.writeClustersArray(channel, clusterIds, protocolVersion);
    channel.writeBytes(distributedConfiguration);
    channel.writeString(serverVersion);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
    sessionToken = network.readBytes();
    clusterIds = OMessageHelper.readClustersArray(network);
    distributedConfiguration = network.readBytes();
    serverVersion = network.readString();
  }

  public int getSessionId() {
    return sessionId;
  }

  public byte[] getSessionToken() {
    return sessionToken;
  }

  public OCluster[] getClusterIds() {
    return clusterIds;
  }

  public byte[] getDistributedConfiguration() {
    return distributedConfiguration;
  }

}