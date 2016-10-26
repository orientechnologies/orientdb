package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.Collection;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OOpenResponse implements OBinaryResponse<Void> {
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
  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeInt(sessionId);
    if (protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      channel.writeBytes(sessionToken);

    OBinaryProtocolHelper.writeClustersArray(channel, clusterIds, protocolVersion);
    channel.writeBytes(distributedConfiguration);
    channel.writeString(serverVersion);
  }

  @Override
  public Void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
    sessionToken = network.readBytes();
    clusterIds = OBinaryProtocolHelper.readClustersArray(network);
    distributedConfiguration = network.readBytes();
    serverVersion = network.readString();
    return null;
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