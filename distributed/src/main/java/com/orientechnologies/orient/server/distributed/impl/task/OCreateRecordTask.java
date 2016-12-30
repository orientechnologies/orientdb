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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OCreateRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;
  public static final  int  FACTORYID        = 0;
  protected byte[] content;
  protected byte   recordType;
  protected int clusterId = -1;
  private transient ORecord record;

  public OCreateRecordTask() {
  }

  public OCreateRecordTask(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType) {
    super(iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  public OCreateRecordTask(final ORecord record) {
    this((ORecordId) record.getIdentity(), record.toStream(), record.getVersion() - 1, ORecordInternal.getRecordType(record));

    if (rid.getClusterId() == ORID.CLUSTER_ID_INVALID) {
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
      clusterId = db.assignAndCheckCluster(record, null);
      // RESETTING FOR AVOID DESERIALIZATION ISSUE.
      ((ORecordId) record.getIdentity()).setClusterId(ORID.CLUSTER_ID_INVALID);
    }
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
      record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
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

    final OPaginatedCluster cluster = (OPaginatedCluster) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getClusterById(rid.getClusterId());
    final OPaginatedCluster.RECORD_STATUS recordStatus = cluster.getRecordStatus(rid.getClusterPosition());

    switch (recordStatus) {
    case REMOVED:
      // RECYCLE THE RID AND OVERWRITE IT WITH THE NEW CONTENT
      ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().recyclePosition(rid, new byte[] {}, version, recordType);

      // CREATE A RECORD TO CALL ALL THE HOOKS (LIKE INDEXES FOR UNIQUE CONSTRAINTS)
      final ORecord loadedRecordInstance = Orient.instance().getRecordFactoryManager().newInstance(recordType);
      ORecordInternal.fill(loadedRecordInstance, rid, version, content, true);
      loadedRecordInstance.save();
      return new OPlaceholder(rid, loadedRecordInstance.getVersion());

    case ALLOCATED:
    case PRESENT:
      final OStorageOperationResult<ORawBuffer> loadedRecord = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
          .readRecord(rid, null, true, false, null);

      if (loadedRecord.getResult() != null) {
        // ALREADY PRESENT
        record = forceUpdate(iManager, database, requestId, loadedRecord.getResult());
        return new OPlaceholder(record);
      }

      // GOES DOWN

    case NOT_EXISTENT:
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
          .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "+-> assigned new rid %s/%s v.%d reqId=%s",
              database.getName(), rid.toString(), record.getVersion(), requestId);

      // } catch (ORecordDuplicatedException e) {
      // // DUPLICATED INDEX ON THE TARGET: CREATE AN EMPTY RECORD JUST TO MAINTAIN THE RID AND LET TO THE FIX OPERATION TO SORT OUT
      // // WHAT HAPPENED
      // ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
      // "+-> duplicated record %s (existent=%s), assigned new rid %s/%s v.%d reqId=%s", record, e.getRid(), database.getName(),
      // rid.toString(), record.getVersion(), requestId);
      //
      // record.clear();
      // if (clusterId > -1)
      // record.save(database.getClusterNameById(clusterId), true);
      // else if (rid.getClusterId() != -1)
      // record.save(database.getClusterNameById(rid.getClusterId()), true);
      // else
      // record.save();
      //
      // throw e;
      // }
    }

    // IMPROVED TRANSPORT BY AVOIDING THE RECORD CONTENT, BUT JUST RID + VERSION
    return new OPlaceholder(record);
  }

  protected ORecord forceUpdate(final ODistributedServerManager manager, final ODatabaseDocumentInternal database,
      final ODistributedRequestId requestId, final ORawBuffer loadedRecord) {
    // LOAD IT AS RECORD
    final ORecord loadedRecordInstance = Orient.instance().getRecordFactoryManager().newInstance(loadedRecord.recordType);
    ORecordInternal.fill(loadedRecordInstance, rid, loadedRecord.version, loadedRecord.getBuffer(), false);

    // RECORD HAS BEEN ALREADY CREATED (PROBABLY DURING DATABASE SYNC) CHECKING COHERENCY
    if (Arrays.equals(loadedRecord.getBuffer(), content))
      // SAME CONTENT
      return loadedRecordInstance;

    ODistributedServerLog.info(this, manager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "Error on creating record in an existent position. toStore=%s stored=%s reqId=%s", getRecord(), loadedRecordInstance,
        requestId);

    throw new ODistributedOperationException("Cannot create the record " + rid + " in an already existent position");
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public ORemoteTask getFixTask(final ODistributedRequest iRequest, ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, final String executorNode, final ODistributedServerManager dManager) {
    if (iBadResponse == null || iBadResponse instanceof Throwable)
      return null;

    final OPlaceholder badResult = (OPlaceholder) iBadResponse;
    final OPlaceholder goodResult = (OPlaceholder) iGoodResponse;

    ORemoteTask result = null;

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
                .getServers(ODatabaseRecordThreadLocal.INSTANCE.get().getClusterNameById(clusterId), dManager.getLocalNodeName());

            final ODistributedResponse response = dManager
                .sendRequest(iRequest.getDatabaseName(), null, nodes, new OReadRecordTask(toUpdateRid),
                    dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

            final ORawBuffer remoteReadRecord = (ORawBuffer) response.getPayload();

            if (remoteReadRecord != null) {
              toUpdateRecord = Orient.instance().getRecordFactoryManager().newInstance(recordType);
              ORecordInternal.fill(toUpdateRecord, toUpdateRid, remoteReadRecord.version, remoteReadRecord.buffer, false);
            } else
              toUpdateRecord = null;
          } else
            // LOAD IT LOCALLY
            toUpdateRecord = toUpdateRid.getRecord();

          if (toUpdateRecord != null)
            result = new OFixUpdateRecordTask(toUpdateRid, toUpdateRecord.toStream(), toUpdateRecord.getVersion(),
                ORecordInternal.getRecordType(toUpdateRecord));
        }

        // CREATE LAST RECORD
        result = new OCreateRecordTask((ORecordId) goodResult.getIdentity(), content, version, recordType);

      } else if (badResult.getIdentity().getClusterId() == goodResult.getIdentity().getClusterId()
          && badResult.getIdentity().getClusterPosition() > goodResult.getIdentity().getClusterPosition()) {

      } else
        // ANY OTHER CASE JUST DELETE IT
        result = new OFixCreateRecordTask(new ORecordId(badResult.getIdentity()), badResult.getVersion());
    }

    return result;
  }

  @Override
  public ODeleteRecordTask getUndoTask(ODistributedRequestId reqId) {
    final ODeleteRecordTask task = new OFixCreateRecordTask(rid, -1);
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
}
