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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public abstract class OAbstractRecordReplicatedTask extends OAbstractReplicatedTask {
  protected ORecordId      rid;
  protected ORecordVersion version;
  protected boolean        inTx = false;

  public OAbstractRecordReplicatedTask() {
  }

  public OAbstractRecordReplicatedTask(final ORecordId iRid, final ORecordVersion iVersion) {
    this.rid = iRid;
    this.version = iVersion;
  }

  public abstract ORecord getRecord();

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

  public ORecordVersion getVersion() {
    return version;
  }

  public void setVersion(ORecordVersion version) {
    this.version = version;
  }

  abstract void setLockRecord(boolean lockRecord);

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(rid.toString());
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().writeTo(out, version);
    out.writeBoolean(inTx);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rid = new ORecordId(in.readUTF());
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().readFrom(in, version);
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
