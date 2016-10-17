package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public class OSBTGetRealBagSizeResponse implements OBinaryResponse<Integer> {
  @Override
  public Integer read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    return network.readInt();
  }
}