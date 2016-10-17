package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public final class ODropClusterResponse implements OBinaryResponse<Boolean> {
  @Override
  public Boolean read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    byte result = 0;
    result = network.readByte();
    if (result == 1) {
      return true;
    }
    return false;
  }
}