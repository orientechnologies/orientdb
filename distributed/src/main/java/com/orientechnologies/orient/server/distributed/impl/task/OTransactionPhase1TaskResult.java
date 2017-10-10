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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.core.serialization.OStreamableHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tglman
 */
public class OTransactionPhase1TaskResult implements OStreamable {

  private OTransactionResultPayload resultPayload;

  public OTransactionPhase1TaskResult() {

  }

  public OTransactionPhase1TaskResult(OTransactionResultPayload resultPayload) {
    this.resultPayload = resultPayload;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeInt(resultPayload.getResponseType());
    switch (resultPayload.getResponseType()) {
    case OTxSuccess.ID:
    case OTxLockTimeout.ID:
      break;
    case OTxConcurrentCreate.ID:
      OTxConcurrentCreate cc = (OTxConcurrentCreate) resultPayload;
      out.writeInt(cc.getRecordId().getClusterId());
      out.writeLong(cc.getRecordId().getClusterPosition());
      break;
    case OTxConcurrentModification.ID:
      OTxConcurrentModification pl = (OTxConcurrentModification) resultPayload;
      out.writeInt(pl.getRecordId().getClusterId());
      out.writeLong(pl.getRecordId().getClusterPosition());
      out.writeInt(pl.getVersion());
      break;
    case OTxException.ID:
      OTxException pl2 = (OTxException) resultPayload;
      OStreamableHelper.toStream(out, pl2.getException());
      break;
    case OTxUniqueIndex.ID:
      OTxUniqueIndex pl3 = (OTxUniqueIndex) resultPayload;
      //RID
      out.writeInt(pl3.getRecordId().getClusterId());
      out.writeLong(pl3.getRecordId().getClusterPosition());
      //index name
      String indexName = pl3.getIndex();
      byte[] indexNameBytes = indexName.getBytes();
      out.writeInt(indexNameBytes.length);
      out.write(indexNameBytes);
      //index key
      OType type = OType.getTypeByValue(pl3.getKey());
      out.writeInt(type.getId());
      byte[] keyBytes = ORecordSerializerNetworkV37.INSTANCE.serializeValue(pl3.getKey(), type);
      out.writeInt(keyBytes.length);
      out.write(keyBytes);
      break;

    }
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    int type = in.readInt();
    switch (type) {
    case OTxSuccess.ID:
      this.resultPayload = new OTxSuccess();
      break;
    case OTxLockTimeout.ID:
      this.resultPayload = new OTxLockTimeout();
      break;
    case OTxConcurrentModification.ID:
      ORecordId rid = new ORecordId(in.readInt(), in.readLong());
      int version = in.readInt();
      this.resultPayload = new OTxConcurrentModification(rid, version);
      break;
    case OTxConcurrentCreate.ID:
      ORecordId failedRid = new ORecordId(in.readInt(), in.readLong());
      this.resultPayload = new OTxConcurrentCreate(failedRid);
      break;
    case OTxException.ID:
      RuntimeException exception = (RuntimeException) OStreamableHelper.fromStream(in);
      this.resultPayload = new OTxException(exception);
    case OTxUniqueIndex.ID:
      //RID
      ORecordId rid2 = new ORecordId(in.readInt(), in.readLong());
      //index name
      int indexNameSize = in.readInt();
      byte[] indexNameBytes = new byte[indexNameSize];
      in.readFully(indexNameBytes);
      String indexName = new String(indexNameBytes);
      //index key
      int type2Id = in.readInt();
      OType type2 = OType.getById((byte) type2Id);
      int keySize = in.readInt();
      byte[] keyBytes = new byte[keySize];
      in.readFully(keyBytes);
      Object keyValue = ORecordSerializerNetworkV37.INSTANCE.deserializeValue(keyBytes, type2);
      this.resultPayload = new OTxUniqueIndex(rid2, indexName, keyValue);
    }
  }

  public OTransactionResultPayload getResultPayload() {
    return resultPayload;
  }
}
