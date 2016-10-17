package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public class OIncrementalBackupResponse implements OBinaryResponse<String> {
  @Override
  public String read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    String fileName = network.readString();
    return fileName;
  }
}