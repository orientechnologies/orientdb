/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public abstract class OAbstractRecordReplicatedTask extends OAbstractReplicatedTask {
  protected ORecordId rid;
  protected int       version;
  protected int       partitionKey = -1;
  protected boolean   inTx         = false;

  public OAbstractRecordReplicatedTask() {
  }

  public OAbstractRecordReplicatedTask(final ORecordId iRid, final int iVersion) {
    this.rid = iRid;
    this.version = iVersion;

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null) {
      final OClass clazz = db.getMetadata().getSchema().getClassByClusterId(rid.clusterId);
      final Set<OIndex<?>> indexes = clazz.getIndexes();
      if (indexes != null && !indexes.isEmpty()) {
        for (OIndex idx : indexes)
          if (idx.isUnique())
            // UNIQUE INDEX: RETURN THE HASH OF THE NAME TO USE THE SAME PARTITION ID AVOIDING CONCURRENCY ON INDEX UPDATES
            partitionKey = idx.getName().hashCode();
      }
    }
  }

  public abstract ORecord getRecord();

  @Override
  public int getPartitionKey() {
    return partitionKey > -1 ? partitionKey : rid.clusterId;
  }

  @Override
  public String toString() {
    return super.toString() + "(" + rid + " v." + version + ")";
  }

  public ORecordId getRid() {
    return rid;
  }

  public void setRid(ORecordId rid) {
    this.rid = rid;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  abstract void setLockRecord(boolean lockRecord);

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(rid.toString());
    out.writeInt(version);
    out.writeInt(partitionKey);
    out.writeBoolean(inTx);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rid = new ORecordId(in.readUTF());
    version = in.readInt();
    partitionKey = in.readInt();
    inTx = in.readBoolean();
  }

  public boolean isInTx() {
    return inTx;
  }

  public void setInTx(final boolean inTx) {
    this.inTx = inTx;
  }

  @Override
  public String getPayload() {
    return "rid=" + rid + " v=" + version;
  }
}
