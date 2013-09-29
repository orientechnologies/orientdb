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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Distributed task to fix records in conflict on synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OFixCreateRecordTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  protected ORecordId       ridAssigned;
  protected ORecordVersion  version;
  protected byte[]          content;
  protected byte            recordType;
  protected ORecordId       ridToAssign;

  public OFixCreateRecordTask() {
  }

  public OFixCreateRecordTask(final ORecordId iRid, final byte[] iContent, final ORecordVersion iVersion, final byte iRecordType,
      final ORecordId iRIDToAssign) {
    ridAssigned = iRid;
    content = iContent;
    version = iVersion;
    recordType = iRecordType;
    ridToAssign = iRIDToAssign;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "fixing create record %s/%s v.%s by applying a %s", database.getName(), ridAssigned.toString(), version.toString());

    ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
    record.fill(ridAssigned, version, content, true);
    database.save(record);

    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "+-> fixed create record %s/%s v.%s", database.getName(), record.getIdentity().toString(), record.getRecordVersion()
            .toString());

    return Boolean.TRUE;
  }

  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(ridAssigned.toString());
    out.writeInt(content.length);
    out.write(content);
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().writeTo(out, version);
    out.writeUTF(ridToAssign.toString());
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    ridAssigned = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().readFrom(in, version);
    ridToAssign = new ORecordId(in.readUTF());
  }

  @Override
  public String getName() {
    return "fix_record_create";
  }
}
