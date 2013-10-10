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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Distributed task to fix update record in conflict on synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OFixUpdateRecordTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  private ORecordId         rid;
  private byte[]            content;
  private ORecordVersion    version;

  public OFixUpdateRecordTask() {
  }

  public OFixUpdateRecordTask(final ORecordId iRid, final byte[] iContent, final ORecordVersion iVersion) {
    rid = iRid;
    content = iContent;
    version = iVersion;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "fixing update record %s/%s v.%s", database.getName(), rid.toString(), version.toString());

    final ORecordInternal<?> record = rid.getRecord();
    if (record == null)
      // DELETED, CANNOT FIX IT
      return Boolean.FALSE;

    final ORecordInternal<?> newRecord = Orient.instance().getRecordFactoryManager().newInstance(record.getRecordType());
    newRecord.fill(rid, version, content, true);

    ((ODocument) record).merge((ODocument) newRecord, false, false);

    database.save(record);

    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "+-> fixed update record %s/%s v.%s", database.getName(), record.getIdentity().toString(), record.getRecordVersion()
            .toString());

    return Boolean.TRUE;
  }

  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(rid.toString());
    out.writeInt(content.length);
    out.write(content);
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().writeTo(out, version);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rid = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().readFrom(in, version);
  }

  @Override
  public String getName() {
    return "fix_record_update";
  }
}
