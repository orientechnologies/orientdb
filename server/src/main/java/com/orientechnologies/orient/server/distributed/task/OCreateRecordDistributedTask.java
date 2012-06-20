/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;

/**
 * Distributed create record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OCreateRecordDistributedTask extends OAbstractRecordDistributedTask<OPhysicalPosition> {
  private static final long serialVersionUID = 1L;

  protected byte[]          content;
  protected byte            recordType;

  public OCreateRecordDistributedTask() {
  }

  public OCreateRecordDistributedTask(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode,
      final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType) {
    super(nodeSource, iDbName, iMode, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
    OLogManager.instance().debug(this, "DISTRIBUTED -> route CREATE RECORD in %s mode to %s %s{%s} v.%d", iMode, nodeSource,
        iDbName, iRid, iVersion);
  }

  @Override
  protected OPhysicalPosition executeOnLocalNode(final OStorageSynchronizer dbSynchronizer) {
    final OPhysicalPosition result = getStorage().createRecord(0, rid, content, version, recordType, 0, null);

    if (rid.getClusterPosition() != result.clusterPosition)
      OLogManager.instance().warn(this, "DISTRIBUTED <- detected conflict against operation %s RID local %s != remote #%d:%d",
          getName(), rid, rid.getClusterId(), result.clusterPosition);

    if (isRedistribute())
      // LOG THE OPERATION BEFORE TO SEND TO OTHER NODES
      dbSynchronizer.updateOperationRid(rid, result.recordVersion, getNodeSource());

    return result;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(rid.toString());
    out.writeInt(content.length);
    out.write(content);
    out.writeInt(version);
    out.write(recordType);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    rid = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    version = in.readInt();
    recordType = in.readByte();
  }

  @Override
  public String getName() {
    return "record_create";
  }
}
