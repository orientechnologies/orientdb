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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OCreateRecordTask extends OAbstractRecordReplicatedTask {
  public static final String SUFFIX_QUEUE_NAME = ".insert";
  private static final long  serialVersionUID  = 1L;
  protected byte[]           content;
  protected byte             recordType;
  protected int              clusterId         = -1;
  private transient ORecord  record;

  public OCreateRecordTask() {
  }

  public OCreateRecordTask(final ORecordId iRid, final byte[] iContent, final ORecordVersion iVersion, final byte iRecordType) {
    super(iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  public OCreateRecordTask(final ORecord record) {
    this((ORecordId) record.getIdentity(), record.toStream(), record.getRecordVersion(), ORecordInternal.getRecordType(record));

    if (rid.getClusterId() == ORID.CLUSTER_ID_INVALID) {
      final OClass clazz;
      if (record instanceof ODocument && (clazz = ODocumentInternal.getImmutableSchemaClass((ODocument) record)) != null) {
        // PRE-ASSIGN THE CLUSTER ID ON CALLER NODE
        clusterId = clazz.getClusterSelection().getCluster(clazz, (ODocument) record);
      } else {
        ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
        clusterId = db.getDefaultClusterId();
      }
    }
  }

  @Override
  public ORecord getRecord() {
    if (record == null) {
      record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
      ORecordInternal.fill(record, rid, version, content, true);
    }
    return record;
  }

  @Override
  void setLockRecord(boolean lockRecord) {
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "creating record %s/%s v.%s...",
        database.getName(), rid.toString(), version.toString());

    getRecord();

    if (ORecordId.isPersistent(rid.getClusterPosition()))
      // OVERWRITE RID TO BE TEMPORARY
      ORecordInternal.setIdentity(record, rid.getClusterId(), ORID.CLUSTER_POS_INVALID);

    if (clusterId > -1)
      record.save(database.getClusterNameById(clusterId), true);
    else if (rid.getClusterId() != -1)
      record.save(database.getClusterNameById(rid.getClusterId()), true);
    else
      record.save();

    rid = (ORecordId) record.getIdentity();

    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "+-> assigned new rid %s/%s v.%d", database.getName(), rid.toString(), record.getVersion());

    // TODO: IMPROVE TRANSPORT BY AVOIDING THE RECORD CONTENT, BUT JUST RID + VERSION
    return new OPlaceholder(record);
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public ODeleteRecordTask getFixTask(final ODistributedRequest iRequest, OAbstractRemoteTask iOriginalTask,
      final Object iBadResponse, final Object iGoodResponse) {
    if (iBadResponse instanceof Throwable)
      return null;

    final OPlaceholder badResult = (OPlaceholder) iBadResponse;
    return new ODeleteRecordTask(new ORecordId(badResult.getIdentity()), badResult.getRecordVersion()).setDelayed(false);
  }

  @Override
  public ODeleteRecordTask getUndoTask(final ODistributedRequest iRequest, final Object iBadResponse) {
    if (iBadResponse instanceof Throwable)
      return null;

    final OPlaceholder badResult = (OPlaceholder) iBadResponse;
    return new ODeleteRecordTask(new ORecordId(badResult.getIdentity()), badResult.getRecordVersion()).setDelayed(false);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    if (content == null)
      out.writeInt(0);
    else {
      out.writeInt(content.length);
      out.write(content);
    }
    out.write(recordType);
    out.writeInt(clusterId);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    final int contentSize = in.readInt();
    if (contentSize == 0)
      content = null;
    else {
      content = new byte[contentSize];
      in.readFully(content);
    }
    recordType = in.readByte();
    clusterId = in.readInt();
  }

  // @Override
  // public String getQueueName(final String iOriginalQueueName) {
  // return iOriginalQueueName + SUFFIX_QUEUE_NAME;
  // }

  @Override
  public String getName() {
    return "record_create";
  }
}
