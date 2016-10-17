package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OLowerPhysicalPositionsRequest implements OBinaryRequest {
  private final OPhysicalPosition physicalPosition;
  private final int               iClusterId;

  public OLowerPhysicalPositionsRequest(OPhysicalPosition physicalPosition, int iClusterId) {
    this.physicalPosition = physicalPosition;
    this.iClusterId = iClusterId;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeInt(iClusterId);
    network.writeLong(physicalPosition.clusterPosition);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER;
  }
}