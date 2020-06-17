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

public class OLowerPhysicalPositionsRequest
    implements OBinaryRequest<OLowerPhysicalPositionsResponse> {
  private OPhysicalPosition physicalPosition;
  private int iClusterId;

  public OLowerPhysicalPositionsRequest(OPhysicalPosition physicalPosition, int iClusterId) {
    this.physicalPosition = physicalPosition;
    this.iClusterId = iClusterId;
  }

  public OLowerPhysicalPositionsRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeInt(iClusterId);
    network.writeLong(physicalPosition.clusterPosition);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    this.iClusterId = channel.readInt();
    this.physicalPosition = new OPhysicalPosition(channel.readLong());
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER;
  }

  @Override
  public String getDescription() {
    return "Retrieve lower positions";
  }

  public int getiClusterId() {
    return iClusterId;
  }

  public OPhysicalPosition getPhysicalPosition() {
    return physicalPosition;
  }

  @Override
  public OLowerPhysicalPositionsResponse createResponse() {
    return new OLowerPhysicalPositionsResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeLowerPosition(this);
  }
}
