package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OFloorPhysicalPositionsRequest implements OBinaryRequest {
  private final OPhysicalPosition physicalPosition;
  private final int               clusterId;

  public OFloorPhysicalPositionsRequest(OPhysicalPosition physicalPosition, int clusterId) {
    this.physicalPosition = physicalPosition;
    this.clusterId = clusterId;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeInt(clusterId);
    network.writeLong(physicalPosition.clusterPosition);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR;
  }
}