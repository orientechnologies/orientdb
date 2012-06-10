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
package com.orientechnologies.orient.server.cluster.hazelcast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.nio.DataSerializable;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;

/**
 * Distributed task used for replication
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OReplicationTask implements Callable<Object>, DataSerializable {
  private static final long serialVersionUID = 1L;

  public static final byte  UPDATE           = 1;
  public static final byte  DELETE           = 2;
  public static final byte  CREATE           = 3;

  private String            databaseName;
  private byte              operation;
  private ORecordId         rid;
  private byte[]            content;
  private int               version;
  private byte              recordType;

  public OReplicationTask() {
  }

  public OReplicationTask(String databaseName, byte iOperation, ORecordId rid, byte[] content, int version, byte recordType) {
    this.databaseName = databaseName;
    this.operation = iOperation;
    this.rid = rid;
    this.content = content;
    this.version = version;
    this.recordType = recordType;
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

    if (operation == CREATE) {
      OLogManager.instance().debug(this, "DISTRIBUTED <- creating record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      return stg.createRecord(0, rid, content, version, recordType, 0, null);

    } else if (operation == UPDATE) {
      OLogManager.instance().debug(this, "DISTRIBUTED <- updating record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      return stg.updateRecord(rid, content, version, recordType, 0, null);

    } else if (operation == DELETE) {
      OLogManager.instance().debug(this, "DISTRIBUTED <- deleting record %s v.%d size=%s", rid, version,
          OFileUtils.getSizeAsString(content.length));
      return stg.deleteRecord(rid, version, 0, null);
    } else
      OLogManager.instance().error(this, "DISTRIBUTED <- received invalid operation code %d", operation);

    return null;
  }

  @Override
  public void readData(final DataInput in) throws IOException {
    databaseName = in.readUTF();
    operation = in.readByte();
    rid = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    version = in.readInt();
    recordType = in.readByte();
  }

  @Override
  public void writeData(final DataOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.write(operation);
    out.writeUTF(rid.toString());
    out.writeInt(content.length);
    out.write(content);
    out.writeInt(version);
    out.write(recordType);
  }
}
