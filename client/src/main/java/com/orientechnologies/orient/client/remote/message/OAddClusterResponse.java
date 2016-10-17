package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public final class OAddClusterResponse implements OBinaryResponse<Integer> {
  @Override
  public Integer read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    final int clusterId = network.readShort();
    return clusterId;
  }
}