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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODatabaseIsOldException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Abstract task for synchronization of database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public abstract class OAbstractSyncDatabaseTask extends OAbstractReplicatedTask implements OCommandOutputListener {
  public final static int    CHUNK_MAX_SIZE = 8388608;    // 8MB
  public static final String DEPLOYDB       = "deploydb.";
  public static final int    FACTORYID      = 14;

  protected long             lastOperationTimestamp;
  protected long             random;

  public OAbstractSyncDatabaseTask() {
  }

  protected OAbstractSyncDatabaseTask(final long lastOperationTimestamp) {
    random = UUID.randomUUID().getLeastSignificantBits();
    this.lastOperationTimestamp = lastOperationTimestamp;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n"))
      iText = iText.substring(2);
    if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return false;
  }

  protected ODistributedDatabase checkIfCurrentDatabaseIsNotOlder(final ODistributedServerManager iManager,
      final String databaseName, final OLogSequenceNumber startLSN) {
    final ODistributedDatabase dDatabase = iManager.getMessageService().getDatabase(databaseName);

    if (startLSN != null) {
      final OLogSequenceNumber currentLSN = dDatabase.getSyncConfiguration().getLastLSN(iManager.getLocalNodeName());
      if (startLSN.equals(currentLSN))
        // REQUESTING LSN IS THE LAST ONE, FINE
        return dDatabase;
    }

    // CHECK THE DATE OF LAST OPERATION
    if (dDatabase.getSyncConfiguration().getLastOperationTimestamp() < lastOperationTimestamp) {
      final OLogSequenceNumber currentLSN = dDatabase.getSyncConfiguration().getLastLSN(getNodeSource());
      if (currentLSN == null)
        return dDatabase;

      if (lastLSN != null) {
        // USE THE LSN TO COMPARE DATABASES
        if (lastLSN.equals(currentLSN))
          return dDatabase;
      }
      databaseIsOld(iManager, databaseName, dDatabase);
    }

    return dDatabase;
  }

  private void databaseIsOld(ODistributedServerManager iManager, String databaseName, ODistributedDatabase dDatabase) {
    final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    final String msg = String.format(
        "Skip deploying delta database '%s' because the requesting server has a most recent database (requester LastOperationOn=%s current LastOperationOn=%s)",
        databaseName, df.format(new Date(lastOperationTimestamp)),
        df.format(new Date(dDatabase.getSyncConfiguration().getLastOperationTimestamp())));
    ODistributedServerLog.error(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE, msg);

    throw new ODatabaseIsOldException(msg);
  }

  protected void readOptionalLSN(DataInput in) throws IOException {
    final boolean lastLSNPresent = in.readBoolean();
    if (lastLSNPresent)
      lastLSN = new OLogSequenceNumber(in);
  }

  protected void writeOptionalLSN(DataOutput out) throws IOException {
    if (lastLSN != null) {
      out.writeBoolean(true);
      lastLSN.toStream(out);
    } else
      out.writeBoolean(false);
  }
}
