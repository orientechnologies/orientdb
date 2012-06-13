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
package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;

/**
 * Distributed task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedTask implements Callable<Object>, Externalizable {
  private static final long serialVersionUID = 1L;

  public enum OPERATION {
    RECORD_CREATE, RECORD_READ, RECORD_UPDATE, RECORD_DELETE, COMMAND
  }

  protected String         databaseName;
  protected OPERATION      operation;
  protected ORecordId      rid;
  protected byte[]         content;
  protected int            version;
  protected byte           recordType;
  protected EXECUTION_MODE mode;

  public ODistributedTask() {
  }

  public ODistributedTask(final String databaseName, final OPERATION iOperation, final ORecordId rid, final byte[] content,
      final int version, final byte recordType, final EXECUTION_MODE iMode) {
    this.databaseName = databaseName;
    this.operation = iOperation;
    this.rid = rid;
    this.content = content;
    this.version = version;
    this.recordType = recordType;
    this.mode = iMode;
  }

  @Override
  public Object call() throws Exception {
    OStorage stg = Orient.instance().getStorage(databaseName);
    if (stg == null) {
      final String url = OServerMain.server().getStorageURL(databaseName);

      if (url == null) {
        OLogManager.instance().error(this,
            "DISTRIBUTED <- database '%s' is not configured on this server. Copy the database here to enable the replication", rid,
            databaseName);
        return null;
      }

      stg = Orient.instance().loadStorage(url);
    }

    if (stg.isClosed())
      stg.open(null, null, null);

    Object result = null;
    switch (operation) {
    case RECORD_CREATE:
      OLogManager.instance().debug(this, "DISTRIBUTED <- creating record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      // RETURNS THE WRAPPED PHY-POS TO OPTIMIZE SERIALIZATION USING HAZELCAST'S ONE
      result = stg.createRecord(0, rid, content, version, recordType, 0, null);
      break;

    case RECORD_READ:
      OLogManager.instance().debug(this, "DISTRIBUTED <- reading record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      result = stg.readRecord(rid, null, false, null);
      break;

    case RECORD_UPDATE:
      OLogManager.instance().debug(this, "DISTRIBUTED <- updating record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      result = stg.updateRecord(rid, content, version, recordType, 0, null);
      break;

    case RECORD_DELETE:
      OLogManager.instance().debug(this, "DISTRIBUTED <- deleting record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      result = stg.deleteRecord(rid, version, 0, null);
      break;

    case COMMAND:
      OLogManager.instance().debug(this, "DISTRIBUTED <- deleting record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      result = stg.deleteRecord(rid, version, 0, null);
      break;
    }

    if (mode != EXECUTION_MODE.FIRE_AND_FORGET)
      return result;

    // FIRE AND FORGET MODE: AVOID THE PAYLOAD AS RESULT
    return null;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.writeByte(operation.ordinal());
    out.writeUTF(rid.toString());
    out.writeInt(content.length);
    out.write(content);
    out.writeInt(version);
    out.write(recordType);
    out.write(mode.ordinal());
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    databaseName = in.readUTF();
    operation = OPERATION.values()[in.readByte()];
    rid = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    version = in.readInt();
    recordType = in.readByte();
    mode = EXECUTION_MODE.values()[in.readByte()];
  }
}
