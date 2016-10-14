package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;

public class OUpdateRecordResponse implements OBinaryResponse<Integer> {
  private final OSBTreeCollectionManager collectionManager;

  public OUpdateRecordResponse(OSBTreeCollectionManager collectionManager) {
    this.collectionManager = collectionManager;
  }

  @Override
  public Integer read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    Integer r = network.readVersion();
    OStorageRemote.readCollectionChanges(network, collectionManager);
    return r;
  }
}