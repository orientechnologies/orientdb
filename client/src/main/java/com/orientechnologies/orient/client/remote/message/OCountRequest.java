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
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public final class OCountRequest implements OBinaryRequest<OCountResponse> {
  private int[] clusterIds;
  private boolean countTombstones;

  public OCountRequest(int[] iClusterIds, boolean countTombstones) {
    this.clusterIds = iClusterIds;
    this.countTombstones = countTombstones;
  }

  public OCountRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeShort((short) clusterIds.length);
    for (int iClusterId : clusterIds) network.writeShort((short) iClusterId);

    network.writeByte(countTombstones ? (byte) 1 : (byte) 0);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    int nclusters = channel.readShort();
    clusterIds = new int[nclusters];
    for (int i = 0; i < clusterIds.length; i++) {
      clusterIds[i] = channel.readShort();
    }
    countTombstones = channel.readByte() != 0;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT;
  }

  @Override
  public String getDescription() {
    return "Count cluster elements";
  }

  public int[] getClusterIds() {
    return clusterIds;
  }

  public boolean isCountTombstones() {
    return countTombstones;
  }

  @Override
  public OCountResponse createResponse() {
    return new OCountResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCountCluster(this);
  }
}
