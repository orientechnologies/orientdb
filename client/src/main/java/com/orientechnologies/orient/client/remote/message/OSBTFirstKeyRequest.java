/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OSBTFirstKeyRequest implements OBinaryRequest<OSBTFirstKeyResponse> {
  private OBonsaiCollectionPointer collectionPointer;

  public OSBTFirstKeyRequest(OBonsaiCollectionPointer collectionPointer) {
    this.collectionPointer = collectionPointer;
  }

  public OSBTFirstKeyRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, collectionPointer);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY;
  }

  @Override
  public String getDescription() {
    return "SB-Tree bonsai get first key";
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  @Override
  public OSBTFirstKeyResponse createResponse() {
    return new OSBTFirstKeyResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSBTFirstKey(this);
  }
}
