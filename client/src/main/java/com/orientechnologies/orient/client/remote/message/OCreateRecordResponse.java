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
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

public class OCreateRecordResponse implements OBinaryResponse<OPhysicalPosition> {
  private final ORecordId                iRid;
  private final OPhysicalPosition        ppos;
  private final OSBTreeCollectionManager collectionManager;
  private final int                      iMode;

  public OCreateRecordResponse(ORecordId iRid, OPhysicalPosition ppos, OSBTreeCollectionManager collectionManager, int iMode) {
    this.iRid = iRid;
    this.ppos = ppos;
    this.collectionManager = collectionManager;
    this.iMode = iMode;
  }

  @Override
  public OPhysicalPosition read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    short clusterId = network.readShort();
    ppos.clusterPosition = network.readLong();
    ppos.recordVersion = network.readVersion();
    // THIS IS A COMPATIBILITY FIX TO AVOID TO FILL THE CLUSTER ID IN CASE OF ASYNC
    if (iMode == 0) {
      iRid.clusterId = clusterId;
      iRid.clusterPosition = ppos.clusterPosition;
    }
    OStorageRemote.readCollectionChanges(network, collectionManager);
    return ppos;
  }
}