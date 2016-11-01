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

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OAddClusterRequest implements OBinaryRequest<OAddClusterResponse> {
  private int    requestedId = -1;
  private String clusterName;

  public OAddClusterRequest(int iRequestedId, String iClusterName) {
    this.requestedId = iRequestedId;
    this.clusterName = iClusterName;
  }

  public OAddClusterRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(clusterName);
    network.writeShort((short) requestedId);
  }

  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    String type = "";
    if (protocolVersion < 24)
      type = channel.readString();

    this.clusterName = channel.readString();

    if (protocolVersion < 24 || type.equalsIgnoreCase("PHYSICAL"))
      // Skipping location is just for compatibility
      channel.readString();

    if (protocolVersion < 24)
      // Skipping data segment name is just for compatibility
      channel.readString();

    this.requestedId = channel.readShort();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD;
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getRequestedId() {
    return requestedId;
  }

  @Override
  public OAddClusterResponse createResponse() {
    return new OAddClusterResponse();
  }
  
}