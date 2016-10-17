package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public class OGetClusterDataRangeResponse implements OBinaryResponse<long[]> {
  @Override
  public long[] read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    return new long[] { network.readLong(), network.readLong() };
  }
}