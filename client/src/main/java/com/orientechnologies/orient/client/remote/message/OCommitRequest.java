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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OCommitRequest implements OBinaryRequest {
  private final OTransaction iTx;

  public OCommitRequest(OTransaction iTx) {
    this.iTx = iTx;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeInt(iTx.getId());
    network.writeByte((byte) (iTx.isUsingLog() ? 1 : 0));

    for (ORecordOperation txEntry : iTx.getAllRecordEntries()) {
      commitEntry(network, txEntry);
    }

    // END OF RECORD ENTRIES
    network.writeByte((byte) 0);

    // SEND EMPTY TX CHANGES, TRACKING MADE SERVER SIDE
    network.writeBytes(iTx.getIndexChanges().toStream());

  }

  private void commitEntry(final OChannelBinaryAsynchClient iNetwork, final ORecordOperation txEntry) throws IOException {
    if (txEntry.type == ORecordOperation.LOADED)
      // JUMP LOADED OBJECTS
      return;

    // SERIALIZE THE RECORD IF NEEDED. THIS IS DONE HERE TO CATCH EXCEPTION AND SEND A -1 AS ERROR TO THE SERVER TO SIGNAL THE
    // ABORT
    // OF TX COMMIT
    byte[] stream = null;
    try {
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED:
        stream = txEntry.getRecord().toStream();
        break;
      }
    } catch (Exception e) {
      // ABORT TX COMMIT
      iNetwork.writeByte((byte) -1);
      throw OException.wrapException(new OTransactionException("Error on transaction commit"), e);
    }

    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.type);
    iNetwork.writeRID(txEntry.getRecord().getIdentity());
    iNetwork.writeByte(ORecordInternal.getRecordType(txEntry.getRecord()));

    switch (txEntry.type) {
    case ORecordOperation.CREATED:
      iNetwork.writeBytes(stream);
      break;

    case ORecordOperation.UPDATED:
      iNetwork.writeVersion(txEntry.getRecord().getVersion());
      iNetwork.writeBytes(stream);
      if (iNetwork.getSrvProtocolVersion() >= 23)
        iNetwork.writeBoolean(ORecordInternal.isContentChanged(txEntry.getRecord()));
      break;

    case ORecordOperation.DELETED:
      iNetwork.writeVersion(txEntry.getRecord().getVersion());
      break;
    }
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_COMMIT;
  }
}