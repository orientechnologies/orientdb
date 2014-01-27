package com.orientechnologies.orient.client.remote;

import java.io.IOException;

import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;

public class OCollectionNetworkSerializer {
  public static final OCollectionNetworkSerializer INSTANCE = new OCollectionNetworkSerializer();

  public OCollectionNetworkSerializer() {
  }

  public OBonsaiCollectionPointer readCollectionPointer(OChannelBinaryAsynchClient client) throws IOException {
    final long fileId = client.readLong();
    final OBonsaiBucketPointer rootPointer = readBonsaiBucketPointer(client);
    return new OBonsaiCollectionPointer(fileId, rootPointer);
  }

  private OBonsaiBucketPointer readBonsaiBucketPointer(OChannelBinaryAsynchClient client) throws IOException {
    long pageIndex = client.readLong();
    int pageOffset = client.readInt();
    return new OBonsaiBucketPointer(pageIndex, pageOffset);
  }

  public void writeCollectionPointer(OChannelBinary client, OBonsaiCollectionPointer treePointer) throws IOException {
    client.writeLong(treePointer.getFileId());
    client.writeLong(treePointer.getRootPointer().getPageIndex());
    client.writeInt(treePointer.getRootPointer().getPageOffset());
  }
}
