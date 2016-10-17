package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransaction;

public final class OCommitResponse implements OBinaryResponse<Void> {
  private final OTransaction iTx;

  public OCommitResponse(OTransaction iTx) {
    this.iTx = iTx;
  }

  @Override
  public Void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    final int createdRecords = network.readInt();
    ORecordId currentRid;
    ORecordId createdRid;
    for (int i = 0; i < createdRecords; i++) {
      currentRid = network.readRID();
      createdRid = network.readRID();

      iTx.updateIdentityAfterCommit(currentRid, createdRid);
    }

    final int updatedRecords = network.readInt();
    ORecordId rid;
    for (int i = 0; i < updatedRecords; ++i) {
      rid = network.readRID();
      int version = network.readVersion();
      ORecordOperation rop = iTx.getRecordEntry(rid);
      if (rop != null) {
        if (version > rop.getRecord().getVersion() + 1)
          // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
          rop.getRecord().unload();
        ORecordInternal.setVersion(rop.getRecord(), version);
      }
    }

    OStorageRemote.readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());
    return null;
  }
}