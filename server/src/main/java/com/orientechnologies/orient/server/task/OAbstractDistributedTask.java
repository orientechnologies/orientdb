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
package com.orientechnologies.orient.server.task;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;

/**
 * Distributed task base abstract class used for distributed actions and events.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractDistributedTask<T> implements Callable<T>, Externalizable {
  private static final long serialVersionUID = 1L;

  public enum STATUS {
    DISTRIBUTE, REMOTE_EXEC, ALIGN, LOCAL_EXEC
  }

  protected String                          nodeSource;
  protected String                          databaseName;
  protected long                            runId;
  protected long                            operationSerial;

  protected EXECUTION_MODE                  mode;
  protected STATUS                          status;

  protected static OServerUserConfiguration replicatorUser;
  static {
    replicatorUser = OServerMain.server().getUser(ODistributedAbstractPlugin.REPLICATOR_USER);
  }

  public abstract String getName();

  /**
   * Constructor used from unmarshalling.
   */
  public OAbstractDistributedTask() {
    status = STATUS.REMOTE_EXEC;
  }

  /**
   * Constructor used on creation from log.
   * 
   * @param iRunId
   * @param iOperationId
   */
  public OAbstractDistributedTask(final long iRunId, final long iOperationId) {
    this.runId = iRunId;
    this.operationSerial = iOperationId;
    this.status = STATUS.ALIGN;
  }

  public OAbstractDistributedTask(final String nodeSource, final String databaseName, final EXECUTION_MODE iMode) {
    this.nodeSource = nodeSource;
    this.databaseName = databaseName;
    this.mode = iMode;
    this.status = STATUS.DISTRIBUTE;

    this.runId = getDistributedServerManager().getRunId();
    this.operationSerial = getDistributedServerManager().incrementDistributedSerial(databaseName);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(nodeSource);
    out.writeUTF(databaseName);
    out.writeLong(runId);
    out.writeLong(operationSerial);
    out.writeByte(mode.ordinal());
    out.writeByte(status.ordinal());
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    nodeSource = in.readUTF();
    databaseName = in.readUTF();
    runId = in.readLong();
    operationSerial = in.readLong();
    mode = EXECUTION_MODE.values()[in.readByte()];
    status = STATUS.values()[in.readByte()];
  }

  public String getNodeSource() {
    return nodeSource;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public long getOperationSerial() {
    return operationSerial;
  }

  public long getRunId() {
    return runId;
  }

  public EXECUTION_MODE getMode() {
    return mode;
  }

  public void setMode(final EXECUTION_MODE iMode) {
    mode = iMode;
  }

  public STATUS getStatus() {
    return status;
  }

  public OAbstractDistributedTask<T> setStatus(final STATUS status) {
    this.status = status;
    return this;
  }

  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  protected OStorage getStorage() {
    OStorage stg = Orient.instance().getStorage(databaseName);
    if (stg == null) {
      final String url = OServerMain.server().getStorageURL(databaseName);

      if (url == null) {
        OLogManager.instance().error(this,
            "DISTRIBUTED <- database '%s' is not configured on this server. Copy the database here to enable the replication",
            databaseName);
        return null;
      }

      stg = Orient.instance().loadStorage(url);
    }

    if (stg.isClosed())
      stg.open(null, null, null);
    return stg;
  }

  protected OStorageSynchronizer getDatabaseSynchronizer() {
    return getDistributedServerManager().getDatabaseSynchronizer(databaseName);
  }

  protected ODistributedServerManager getDistributedServerManager() {
    return (ODistributedServerManager) OServerMain.server().getVariable("ODistributedAbstractPlugin");
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public String toString() {
    return getName();
  }

  protected void setAsCompleted(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().changeOperationStatus(operationLogOffset, null);
  }

  protected ODatabaseDocumentTx getDatabase() {
    return (ODatabaseDocumentTx) OServerMain.server().openDatabase("document", databaseName, replicatorUser.name,
        replicatorUser.password);
  }
}
