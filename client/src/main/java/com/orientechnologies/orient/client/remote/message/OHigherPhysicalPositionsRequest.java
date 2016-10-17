package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OHigherPhysicalPositionsRequest implements OBinaryRequest {
  private final int               iClusterId;
  private final OPhysicalPosition iClusterPosition;

  public OHigherPhysicalPositionsRequest(int iClusterId, OPhysicalPosition iClusterPosition) {
    this.iClusterId = iClusterId;
    this.iClusterPosition = iClusterPosition;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeInt(iClusterId);
    network.writeLong(iClusterPosition.clusterPosition);

  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER;
  }
}