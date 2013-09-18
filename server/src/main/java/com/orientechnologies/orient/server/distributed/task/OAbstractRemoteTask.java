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

import java.io.Externalizable;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;

/**
 * Base class for Tasks to be executed remotely.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractRemoteTask implements Externalizable {
  private static final long   serialVersionUID = 1L;

  protected static final long DEFAULT_TIMEOUT  = 10000;

  protected long              runId;
  protected long              operationSerial;

  protected transient boolean inheritedDatabase;
  protected transient String  nodeSource;

  /**
   * Constructor used from unmarshalling.
   */
  public OAbstractRemoteTask() {
  }

  public abstract String getName();

  public abstract Object execute(final OServer iServer, ODistributedServerManager iManager, final String iDatabaseName)
      throws Exception;

  /**
   * Handles conflict between local and remote execution results.
   * 
   * @param iDatabaseName
   *          TODO
   * @param localResult
   *          The result on local node
   * @param remoteResult
   *          the result on remote node
   * @param iConfictStrategy
   *          TODO
   * @param remoteResult2
   */
  public void handleConflict(String iDatabaseName, final String iRemoteNode, Object localResult, Object remoteResult,
      OReplicationConflictResolver iConfictStrategy) {
  }

  @Override
  public String toString() {
    return getName();
  }

  protected ODatabaseDocumentTx openDatabase(final OServer serverInstance, final String databaseName) {
    inheritedDatabase = true;

    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && db.getName().equals(databaseName) && !db.isClosed()) {
      if (db instanceof ODatabaseDocumentTx)
        return (ODatabaseDocumentTx) db;
      else if (db.getDatabaseOwner() instanceof ODatabaseDocumentTx)
        return (ODatabaseDocumentTx) db.getDatabaseOwner();
    }

    inheritedDatabase = false;
    final OServerUserConfiguration replicatorUser = serverInstance.getUser(ODistributedAbstractPlugin.REPLICATOR_USER);
    return (ODatabaseDocumentTx) serverInstance
        .openDatabase("document", databaseName, replicatorUser.name, replicatorUser.password);
  }

  protected void closeDatabase(final ODatabaseDocumentTx iDatabase) {
    if (!inheritedDatabase)
      iDatabase.close();
  }

  public OAbstractRemoteTask copy(final OAbstractRemoteTask iCopy) {
    iCopy.inheritedDatabase = inheritedDatabase;
    return iCopy;
  }

  public long getTimeout() {
    return DEFAULT_TIMEOUT;
  }

  public String getClusterName() {
    return null;
  }

  public String getNodeSource() {
    return nodeSource;
  }

  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  public void undo() {
  }

  public void setId(final long iRunId, final long iOperationSerial) {
    this.runId = iRunId;
    this.operationSerial = iOperationSerial;
  }
}
