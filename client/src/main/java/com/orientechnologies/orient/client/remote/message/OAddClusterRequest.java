package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OAddClusterRequest implements OBinaryRequest {
  private final int    iRequestedId;
  private final String iClusterName;

  public OAddClusterRequest(int iRequestedId, String iClusterName) {
    this.iRequestedId = iRequestedId;
    this.iClusterName = iClusterName;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeString(iClusterName);
    network.writeShort((short) iRequestedId);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD;
  }
}