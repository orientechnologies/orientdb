package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;

public class OSBTCreateTreeResponse implements OBinaryResponse<OBonsaiCollectionPointer> {
  private OCollectionNetworkSerializer networkSerializer;

  public OSBTCreateTreeResponse(OCollectionNetworkSerializer networkSerializer) {
    this.networkSerializer = networkSerializer;
  }

  @Override
  public OBonsaiCollectionPointer read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    return networkSerializer.readCollectionPointer(network);
  }
}