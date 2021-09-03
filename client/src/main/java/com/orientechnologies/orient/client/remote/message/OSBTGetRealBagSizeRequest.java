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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Map;

public class OSBTGetRealBagSizeRequest implements OBinaryRequest<OSBTGetRealBagSizeResponse> {

  private OBonsaiCollectionPointer collectionPointer;
  private Map<OIdentifiable, Change> changes;
  private OBinarySerializer<OIdentifiable> keySerializer;

  public OSBTGetRealBagSizeRequest() {}

  public OSBTGetRealBagSizeRequest(
      OBinarySerializer<OIdentifiable> keySerializer,
      OBonsaiCollectionPointer collectionPointer,
      Map<OIdentifiable, Change> changes) {
    this.collectionPointer = collectionPointer;
    this.changes = changes;
    this.keySerializer = keySerializer;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, collectionPointer);
    final ChangeSerializationHelper changeSerializer = ChangeSerializationHelper.INSTANCE;
    final byte[] stream =
        new byte
            [OIntegerSerializer.INT_SIZE
                + changeSerializer.getChangesSerializedSize(changes.size())];
    changeSerializer.serializeChanges(changes, keySerializer, stream, 0);
    network.writeBytes(stream);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    byte[] stream = channel.readBytes();
    final ChangeSerializationHelper changeSerializer = ChangeSerializationHelper.INSTANCE;
    changes = changeSerializer.deserializeChanges(stream, 0);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE;
  }

  @Override
  public String getDescription() {
    return "RidBag get size";
  }

  public Map<OIdentifiable, Change> getChanges() {
    return changes;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  @Override
  public OSBTGetRealBagSizeResponse createResponse() {
    return new OSBTGetRealBagSizeResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSBTGetRealSize(this);
  }
}
