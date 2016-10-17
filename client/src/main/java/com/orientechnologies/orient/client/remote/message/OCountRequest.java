package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OCountRequest implements OBinaryRequest {
  private final int[]   iClusterIds;
  private final boolean countTombstones;

  public OCountRequest(int[] iClusterIds, boolean countTombstones) {
    this.iClusterIds = iClusterIds;
    this.countTombstones = countTombstones;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeShort((short) iClusterIds.length);
    for (int iClusterId : iClusterIds)
      network.writeShort((short) iClusterId);

    network.writeByte(countTombstones ? (byte) 1 : (byte) 0);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT;
  }
}