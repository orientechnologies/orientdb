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
package com.orientechnologies.orient.server.hazelcast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.replication.OReplicationTask;

/**
 * Hazelcast implementation of the distributed task used for replication. it uses the Hazelcast serialization to improve
 * performance.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastReplicationTask extends OReplicationTask implements DataSerializable {

  public OHazelcastReplicationTask() {
  }

  public OHazelcastReplicationTask(String databaseName, byte iOperation, ORecordId rid, byte[] content, int version,
      byte recordType, final EXECUTION_MODE iMode) {
    super(databaseName, iOperation, rid, content, version, recordType, iMode);
  }

  @Override
  public Object call() throws Exception {
    Object result = super.call();
    if (result instanceof OPhysicalPosition)
      // WRAP IT TO IMPROVE PERFORMANCE
      result = new OHazelcastPhysicalPosition((OPhysicalPosition) result);
    return result;
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
