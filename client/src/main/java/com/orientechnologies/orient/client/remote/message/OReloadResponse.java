package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OCluster;

public class OReloadResponse implements OBinaryResponse<OCluster[]> {
  @Override
  public OCluster[] read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    return OStorageRemote.readDatabaseInformation(network);
  }
}