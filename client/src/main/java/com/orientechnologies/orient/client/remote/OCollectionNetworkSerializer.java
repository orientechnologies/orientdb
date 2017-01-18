/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.io.IOException;

public class OCollectionNetworkSerializer {
  public static final OCollectionNetworkSerializer INSTANCE = new OCollectionNetworkSerializer();

  public OCollectionNetworkSerializer() {
  }

  public OBonsaiCollectionPointer readCollectionPointer(final OChannelBinary client) throws IOException {
    final long fileId = client.readLong();
    final OBonsaiBucketPointer rootPointer = readBonsaiBucketPointer(client);
    return new OBonsaiCollectionPointer(fileId, rootPointer);
  }

  private OBonsaiBucketPointer readBonsaiBucketPointer(OChannelBinary client) throws IOException {
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
