package com.orientechnologies.orient.client.remote;

import java.io.IOException;

import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;

public class OCollectionNetworkSerializer {
  public OCollectionNetworkSerializer() {
  }

  OBonsaiCollectionPointer readCollectionPointer(OChannelBinaryAsynchClient client) throws IOException {
    final long fileId = client.readLong();
    final OBonsaiBucketPointer rootPointer = readBonsaiBucketPointer(client);
    return new OBonsaiCollectionPointer(fileId, rootPointer);
  }

  OBonsaiBucketPointer readBonsaiBucketPointer(OChannelBinaryAsynchClient client) throws IOException {
    long pageIndex = client.readLong();
    int pageOffset = client.readInt();
    return new OBonsaiBucketPointer(pageIndex, pageOffset);
  }
}
