package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OSBTFetchEntriesMajorRequest implements OBinaryRequest {
  private final boolean            inclusive;
  private final byte[]             keyStream;
  private OBonsaiCollectionPointer pointer;

  public OSBTFetchEntriesMajorRequest(boolean inclusive, byte[] keyStream, OBonsaiCollectionPointer pointer) {
    this.inclusive = inclusive;
    this.keyStream = keyStream;
    this.pointer = pointer;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, pointer);
    network.writeBytes(keyStream);
    network.writeBoolean(inclusive);
    network.writeInt(128);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR;
  }
}