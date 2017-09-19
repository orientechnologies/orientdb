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
import com.orientechnologies.orient.core.serialization.OStreamable;
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
    case OTxConcurrentModification.ID:
      OTxConcurrentModification pl = (OTxConcurrentModification) resultPayload;
      out.writeInt(pl.getRecordId().getClusterId());
      out.writeLong(pl.getRecordId().getClusterPosition());
      out.writeInt(pl.getVersion());
      break;
    case OTxException.ID:
      throw new UnsupportedOperationException(); //TODO!
    case OTxUniqueIndex.ID:
      throw new UnsupportedOperationException(); //TODO!
    }
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    int type = in.readInt();
    switch (resultPayload.getResponseType()) {
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
    case OTxException.ID:
      throw new UnsupportedOperationException(); //TODO!
    case OTxUniqueIndex.ID:
      throw new UnsupportedOperationException(); //TODO!
    }
  }

  public OTransactionResultPayload getResultPayload() {
    return resultPayload;
  }
}
