package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public final class ODeleteRecordResponse implements OBinaryResponse<Boolean> {
  @Override
  public Boolean read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    return network.readByte() == 1;
  }
}