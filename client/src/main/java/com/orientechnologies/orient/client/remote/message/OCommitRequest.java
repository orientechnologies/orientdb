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
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OCommitRequest implements OBinaryRequest<OCommitResponse> {
  public class ORecordOperationRequest {
    private byte    type;
    private byte    recordType;
    private ORID    id;
    private byte[]  record;
    private int     version;
    private boolean contentChanged;

    public ORID getId() {
      return id;
    }

    public byte[] getRecord() {
      return record;
    }

    public byte getRecordType() {
      return recordType;
    }

    public byte getType() {
      return type;
    }

    public int getVersion() {
      return version;
    }

    public boolean isContentChanged() {
      return contentChanged;
    }
  }

  private int                           txId;
  private boolean                       usingLong;
  private List<ORecordOperationRequest> operations;
  private ODocument                     indexChanges;

  public OCommitRequest(int txId, boolean usingLog, Iterable<ORecordOperation> operations, ODocument indexChanges) {
    this.txId = txId;
    this.usingLong = usingLog;
    this.indexChanges = indexChanges;
    List<ORecordOperationRequest> netOperations = new ArrayList<>();
    for (ORecordOperation txEntry : operations) {
      if (txEntry.type == ORecordOperation.LOADED)
        continue;
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.type = txEntry.type;
      request.version = txEntry.getRecord().getVersion();
      request.id = txEntry.getRecord().getIdentity();
      request.recordType = ORecordInternal.getRecordType(txEntry.getRecord());
      try {
        switch (txEntry.type) {
        case ORecordOperation.CREATED:
        case ORecordOperation.UPDATED:
          request.record = txEntry.getRecord().toStream();
          request.contentChanged = ORecordInternal.isContentChanged(txEntry.getRecord());
          break;
        }
      } catch (Exception e) {
        // ABORT TX COMMIT
        throw OException.wrapException(new OTransactionException("Error on transaction commit"), e);
      }
      netOperations.add(request);
    }
    this.operations = netOperations;

  }

  public OCommitRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {

    network.writeInt(txId);
    network.writeBoolean(usingLong);

    for (ORecordOperationRequest txEntry : operations) {
      commitEntry(network, txEntry);
    }

    // END OF RECORD ENTRIES
    network.writeByte((byte) 0);

    // SEND EMPTY TX CHANGES, TRACKING MADE SERVER SIDE
    network.writeBytes(indexChanges.toStream());

  }

  private void commitEntry(final OChannelBinaryAsynchClient iNetwork, final ORecordOperationRequest txEntry) throws IOException {
    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.type);
    iNetwork.writeRID(txEntry.id);
    iNetwork.writeByte(txEntry.recordType);

    switch (txEntry.type) {
    case ORecordOperation.CREATED:
      iNetwork.writeBytes(txEntry.record);
      break;

    case ORecordOperation.UPDATED:
      iNetwork.writeVersion(txEntry.version);
      iNetwork.writeBytes(txEntry.record);
      iNetwork.writeBoolean(txEntry.contentChanged);
      break;

    case ORecordOperation.DELETED:
      iNetwork.writeVersion(txEntry.version);
      break;
    }
  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    txId = channel.readInt();
    usingLong = channel.readBoolean();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        ORecordOperationRequest entry = new ORecordOperationRequest();
        entry.type = channel.readByte();
        entry.id = channel.readRID();
        entry.recordType = channel.readByte();
        switch (entry.type) {
        case ORecordOperation.CREATED:
          entry.record = channel.readBytes();
          break;
        case ORecordOperation.UPDATED:
          entry.version = channel.readVersion();
          entry.record = channel.readBytes();
          entry.contentChanged = channel.readBoolean();
          break;
        case ORecordOperation.DELETED:
          entry.version = channel.readVersion();
          break;
        default:
          break;
        }
        operations.add(entry);
      }
    } while (hasEntry == 1);

    indexChanges = new ODocument(channel.readBytes());

  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_COMMIT;
  }

  @Override
  public String getDescription() {
    return "Transaction commit";
  }

  public ODocument getIndexChanges() {
    return indexChanges;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public int getTxId() {
    return txId;
  }

  public boolean isUsingLong() {
    return usingLong;
  }

  @Override
  public OCommitResponse createResponse() {
    return new OCommitResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCommit(this);
  }
}