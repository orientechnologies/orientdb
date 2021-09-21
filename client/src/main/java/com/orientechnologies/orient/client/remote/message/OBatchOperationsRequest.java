/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Created by Enrico Risa on 15/05/2017. */
public class OBatchOperationsRequest implements OBinaryRequest<OBatchOperationsResponse> {
  private int txId;
  private List<ORecordOperationRequest> operations;

  public OBatchOperationsRequest(int txId, Iterable<ORecordOperation> operations) {
    ORecordSerializerNetworkV37Client serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    this.txId = txId;
    List<ORecordOperationRequest> netOperations = new ArrayList<>();
    for (ORecordOperation txEntry : operations) {
      if (txEntry.type == ORecordOperation.LOADED) continue;
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRecord().getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      switch (txEntry.type) {
        case ORecordOperation.CREATED:
        case ORecordOperation.UPDATED:
          request.setRecord(serializer.toStream(txEntry.getRecord()));
          request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
          break;
      }
      netOperations.add(request);
    }
    this.operations = netOperations;
  }

  public OBatchOperationsRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    ORecordSerializerNetworkV37Client serializer = ORecordSerializerNetworkV37Client.INSTANCE;

    network.writeInt(txId);
    for (ORecordOperationRequest txEntry : operations) {
      network.writeByte((byte) 1);
      OMessageHelper.writeTransactionEntry(network, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    network.writeByte((byte) 0);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    txId = channel.readInt();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        ORecordOperationRequest entry = OMessageHelper.readTransactionEntry(channel, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_BATCH_OPERATIONS;
  }

  @Override
  public OBatchOperationsResponse createResponse() {
    return new OBatchOperationsResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeBatchOperations(this);
  }

  @Override
  public String getDescription() {
    return "Batch Request for Create/Update/Delete";
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public int getTxId() {
    return txId;
  }
}
