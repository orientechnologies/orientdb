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