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

public class OSBTFetchEntriesMajorRequest<K, V>
    implements OBinaryRequest<OSBTFetchEntriesMajorResponse<K, V>> {
  private boolean inclusive;
  private byte[] keyStream;
  private OBonsaiCollectionPointer pointer;
  private int pageSize;
  private OBinarySerializer<K> keySerializer;
  private OBinarySerializer<V> valueSerializer;

  public OSBTFetchEntriesMajorRequest(
      boolean inclusive,
      byte[] keyStream,
      OBonsaiCollectionPointer pointer,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer) {
    this.inclusive = inclusive;
    this.keyStream = keyStream;
    this.pointer = pointer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public OSBTFetchEntriesMajorRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, pointer);
    network.writeBytes(keyStream);
    network.writeBoolean(inclusive);
    network.writeInt(128);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    this.pointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    this.keyStream = channel.readBytes();
    this.inclusive = channel.readBoolean();
    if (protocolVersion >= 21) this.pageSize = channel.readInt();
    else this.pageSize = 128;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR;
  }

  @Override
  public String getDescription() {
    return "SB-Tree bonsai get values major";
  }

  public byte[] getKeyStream() {
    return keyStream;
  }

  public OBonsaiCollectionPointer getPointer() {
    return pointer;
  }

  public boolean isInclusive() {
    return inclusive;
  }

  public int getPageSize() {
    return pageSize;
  }

  @Override
  public OSBTFetchEntriesMajorResponse<K, V> createResponse() {
    return new OSBTFetchEntriesMajorResponse<>(keySerializer, valueSerializer);
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSBTFetchEntriesMajor(this);
  }
}
