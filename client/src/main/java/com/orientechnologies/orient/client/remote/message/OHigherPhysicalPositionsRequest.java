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
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OHigherPhysicalPositionsRequest
    implements OBinaryRequest<OHigherPhysicalPositionsResponse> {
  private int clusterId;
  private OPhysicalPosition clusterPosition;

  public OHigherPhysicalPositionsRequest(int iClusterId, OPhysicalPosition iClusterPosition) {
    this.clusterId = iClusterId;
    this.clusterPosition = iClusterPosition;
  }

  public OHigherPhysicalPositionsRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeInt(clusterId);
    network.writeLong(clusterPosition.clusterPosition);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    clusterId = channel.readInt();
    clusterPosition = new OPhysicalPosition(channel.readLong());
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER;
  }

  @Override
  public String getDescription() {
    return "Retrieve higher positions";
  }

  public int getClusterId() {
    return clusterId;
  }

  public OPhysicalPosition getClusterPosition() {
    return clusterPosition;
  }

  @Override
  public OHigherPhysicalPositionsResponse createResponse() {
    return new OHigherPhysicalPositionsResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeHigherPosition(this);
  }
}
