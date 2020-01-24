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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OCreateRecordTask extends OAbstractRecordReplicatedTask {
  public static final int FACTORYID = 0;
  protected byte[] content;
  protected byte   recordType;
  protected int clusterId = -1;
  private transient ORecord record;

  public OCreateRecordTask() {
  }

  public OCreateRecordTask init(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType) {
    super.init(iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
    return this;
  }

  public OCreateRecordTask init(final ORecord record) {
    init((ORecordId) record.getIdentity(), record.toStream(), record.getVersion() - 1, ORecordInternal.getRecordType(record));

    if (rid.getClusterId() == ORID.CLUSTER_ID_INVALID) {
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      clusterId = db.assignAndCheckCluster(record, null);
      // RESETTING FOR AVOID DESERIALIZATION ISSUE.
      ((ORecordId) record.getIdentity()).setClusterId(ORID.CLUSTER_ID_INVALID);
    }
    return this;
  }

  @Override
  public ORecord prepareUndoOperation() {
    // NO UNDO IS NEEDED
    return null;
  }

  @Override
  public void checkRecordExists() {
    // NO CHECK ON PREVIOUS RECORD BECAUSE IS A CREATE
  }

  @Override
  public ORecord getRecord() {
    if (record == null) {
      record = Orient.instance().getRecordFactoryManager()
          .newInstance(recordType, rid.getClusterId(), ODatabaseRecordThreadLocal.instance().get());
      ORecordInternal.fill(record, rid, version, content, true);
    }
    return record;
  }

  @Override
  public Object executeRecordTask(final ODistributedRequestId requestId, final OServer iServer,
      final ODistributedServerManager iManager, final ODatabaseDocumentInternal database) throws Exception {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog
          .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "Creating record %s/%s v.%d reqId=%s...",
              database.getName(), rid.toString(), version, requestId);

    if (!rid.isPersistent())
      throw new ODistributedException("Record " + rid + " has not been saved on owner node first (temporary rid)");

    final OPaginatedCluster cluster = (OPaginatedCluster) ODatabaseRecordThreadLocal.instance().get().getStorage()
        .getClusterById(rid.getClusterId());
    final OPaginatedCluster.RECORD_STATUS recordStatus = cluster.getRecordStatus(rid.getClusterPosition());

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog
          .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "Found record %s/%s status=%s reqId=%s...",
              database.getName(), rid.toString(), recordStatus, requestId);

    switch (recordStatus) {
    case REMOVED: {
      // RECYCLE THE RID AND OVERWRITE IT WITH THE NEW CONTENT
      getRecord();
      ODatabaseRecordThreadLocal.instance().get().recycle(record);
    }

    case ALLOCATED:
      getRecord();
      // FORCE CREATION
      if (record.getVersion() < 0)
        // INCREMENT THE VERSION IN CASE OF ROLLBACK
        ORecordInternal.setVersion(record, record.getVersion() + 1);
      record.save();
      break;

    case PRESENT: {
      getRecord();
      record.save();
      break;
    }

    case NOT_EXISTENT: {
      // try {
      ORecordId newRid;
      do {
        getRecord();

        if (clusterId > -1)
          record.save(database.getClusterNameById(clusterId), true);
        else if (rid.getClusterId() != -1)
          record.save(database.getClusterNameById(rid.getClusterId()), true);
        else
          record.save();

        newRid = (ORecordId) record.getIdentity();
        if (newRid.getClusterPosition() >= rid.getClusterPosition())
          break;

        // CREATE AN HOLE
        record.delete();
        record = null;

      } while (newRid.getClusterPosition() < rid.getClusterPosition());

      if (!rid.equals(newRid)) {
        ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
            "Record %s has been saved with the RID %s instead of the expected %s reqId=%s", record, newRid, rid, requestId);

        // DELETE THE INVALID RECORD FIRST
        record.delete();

        throw new ODistributedException(
            "Record " + rid + " has been saved with the different RID " + newRid + " on server " + iManager.getLocalNodeName());
      }

      ODistributedServerLog
          .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "+-> assigning new rid %s/%s v.%d reqId=%s",
              database.getName(), rid.toString(), record.getVersion(), requestId);
    }
    }

    // IMPROVED TRANSPORT BY AVOIDING THE RECORD CONTENT, BUT JUST RID + VERSION
    return new OPlaceholder(record);
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public ORemoteTask getFixTask(final ODistributedRequest iRequest, ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, final String executorNode, final ODistributedServerManager dManager) {
    if (iBadResponse == null || (iBadResponse instanceof Throwable && !(iBadResponse instanceof ONeedRetryException)))
      return null;

    ORemoteTask result = null;

    final OPlaceholder goodResult = (OPlaceholder) iGoodResponse;

    if (iBadResponse instanceof ONeedRetryException)
      return ((OCreateRecordTask) dManager.getTaskFactoryManager().getFactoryByServerName(executorNode)
          .createTask(OCreateRecordTask.FACTORYID))
          .init((ORecordId) goodResult.getIdentity(), content, ORecordVersionHelper.setRollbackMode(version), recordType);

    final OPlaceholder badResult = (OPlaceholder) iBadResponse;

    if (!badResult.equals(goodResult)) {
      // CREATE RECORD FAILED TO HAVE THE SAME RIDS. FORCE REALIGNING OF DATA CLUSTERS
      if (badResult.getIdentity().getClusterId() == goodResult.getIdentity().getClusterId()
          && badResult.getIdentity().getClusterPosition() < goodResult.getIdentity().getClusterPosition()) {

        final long minPos = Math.max(badResult.getIdentity().getClusterPosition() - 1, 0);
        for (long pos = minPos; pos < goodResult.getIdentity().getClusterPosition(); ++pos) {
          // UPDATE INTERMEDIATE RECORDS
          final ORecordId toUpdateRid = new ORecordId(goodResult.getIdentity().getClusterId(), pos);

          final ORecord toUpdateRecord;
          if (dManager.getLocalNodeName().equals(executorNode)) {
            // SAME SERVER: LOAD THE RECORD FROM ANOTHER NODE
            final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(iRequest.getDatabaseName());
            final List<String> nodes = dCfg
                .getServers(ODatabaseRecordThreadLocal.instance().get().getClusterNameById(clusterId), dManager.getLocalNodeName());

            final OReadRecordTask task = (OReadRecordTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
                .createTask(OReadRecordTask.FACTORYID);
            task.init(toUpdateRid);

            final ODistributedResponse response = dManager
                .sendRequest(iRequest.getDatabaseName(), null, nodes, task, dManager.getNextMessageIdCounter(),
                    ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

            final ORawBuffer remoteReadRecord = (ORawBuffer) response.getPayload();

            if (remoteReadRecord != null) {
              toUpdateRecord = Orient.instance().getRecordFactoryManager()
                  .newInstance(recordType, record.getIdentity().getClusterId(), ODatabaseRecordThreadLocal.instance().get());
              ORecordInternal.fill(toUpdateRecord, toUpdateRid, remoteReadRecord.version, remoteReadRecord.buffer, false);
            } else
              toUpdateRecord = null;
          } else
            // LOAD IT LOCALLY
            toUpdateRecord = toUpdateRid.getRecord();

          if (toUpdateRecord != null)
            try {
              new OFixUpdateRecordTask().init(toUpdateRid, toUpdateRecord.toStream(), toUpdateRecord.getVersion(),
                  ORecordInternal.getRecordType(toUpdateRecord))
                  .execute(iRequest.getId(), dManager.getServerInstance(), dManager, ODatabaseRecordThreadLocal.instance().get());
            } catch (Exception e) {
              throw OException.wrapException(
                  new ODistributedOperationException("Cannot create record " + rid + " because assigned RID is different"), e);
            }
        }

        // CREATE LAST RECORD
        final OCreateRecordTask task = (OCreateRecordTask) dManager.getTaskFactoryManager().getFactoryByServerName(executorNode)
            .createTask(OCreateRecordTask.FACTORYID);
        task.init((ORecordId) goodResult.getIdentity(), content, version, recordType);
        result = task;

      } else if (badResult.getIdentity().getClusterId() == goodResult.getIdentity().getClusterId()
          && badResult.getIdentity().getClusterPosition() > goodResult.getIdentity().getClusterPosition()) {
        //Do Nothing is fine
      } else {
        // ANY OTHER CASE JUST DELETE IT
        final OFixCreateRecordTask task = (OFixCreateRecordTask) dManager.getTaskFactoryManager()
            .getFactoryByServerName(executorNode).createTask(OFixCreateRecordTask.FACTORYID);
        task.init(new ORecordId(badResult.getIdentity()), badResult.getVersion());
        result = task;
      }
    }

    return result;
  }

  @Override
  public ODeleteRecordTask getUndoTask(final ODistributedServerManager dManager, final ODistributedRequestId reqId,
      final List<String> servers) {
    final ODeleteRecordTask task = (ODeleteRecordTask) dManager.getTaskFactoryManager().getFactoryByServerNames(servers)
        .createTask(ODeleteRecordTask.FACTORYID);
    task.init(rid, -1);
    task.setLockRecords(false);
    return task;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    super.toStream(out);
    if (content == null)
      out.writeInt(0);
    else {
      out.writeInt(content.length);
      out.write(content);
    }
    out.writeByte(recordType);
    out.writeInt(clusterId);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
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

  @Override
  public String getName() {
    return "record_create";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  public byte[] getContent() {
    return content;
  }

  public byte getRecordType() {
    return recordType;
  }

  public int getClusterId() {
    return clusterId;
  }
}
