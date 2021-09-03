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

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OReloadResponse implements OBinaryResponse {

  private String[] clusterNames;
  private int[] clusterIds;

  public OReloadResponse() {}

  public OReloadResponse(String[] clusterNames, int[] clusterIds) {
    this.clusterNames = clusterNames;
    this.clusterIds = clusterIds;
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    final ORawPair<String[], int[]> clusters = OMessageHelper.readClustersArray(network);
    clusterNames = clusters.first;
    clusterIds = clusters.second;
  }

  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    OMessageHelper.writeClustersArray(
        channel, new ORawPair<>(clusterNames, clusterIds), protocolVersion);
  }

  public String[] getClusterNames() {
    return clusterNames;
  }

  public int[] getClusterIds() {
    return clusterIds;
  }
}
