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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Interface for embedded storage.
 * 
 * @see com.orientechnologies.orient.core.storage.impl.local.OStorageLocal, OStorageMemory
 * @author Luca Garulli
 * 
 */
public abstract class OStorageEmbedded extends OStorageAbstract {
  protected final ORecordLockManager lockManager;
  protected final String             PROFILER_CREATE_RECORD;
  protected final String             PROFILER_READ_RECORD;
  protected final String             PROFILER_UPDATE_RECORD;
  protected final String             PROFILER_DELETE_RECORD;

  public OStorageEmbedded(final String iName, final String iFilePath, final String iMode) {
    super(iName, iFilePath, iMode);
    lockManager = new ORecordLockManager(OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger());

    PROFILER_CREATE_RECORD = "db." + name + ".createRecord";
    PROFILER_READ_RECORD = "db." + name + ".readRecord";
    PROFILER_UPDATE_RECORD = "db." + name + ".updateRecord";
    PROFILER_DELETE_RECORD = "db." + name + ".deleteRecord";
  }

  protected abstract ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, boolean iAtomicLock);

  public abstract OCluster getClusterByName(final String iClusterName);

  /**
   * Closes the storage freeing the lock manager first.
   */
  @Override
  public void close(final boolean iForce) {
    if (checkForClose(iForce))
      lockManager.clear();
    super.close(iForce);
  }

  /**
   * Executes the command request and return the result back.
   */
  public Object command(final OCommandRequestText iCommand) {
    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    return executeCommand(iCommand, executor);
  }

  public Object executeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor) {
    if (iCommand.isIdempotent() && !executor.isIdempotent())
      throw new OCommandExecutionException("Cannot execute non idempotent command");

    try {
      final Object result = executor.execute(iCommand.getParameters());
      iCommand.setContext(executor.getContext());
      return result;
    } catch (OException e) {
      // PASS THROUGHT
      throw e;
    } catch (Exception e) {
      throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);
    }
  }

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  protected void checkOpeness() {
    if (status != STATUS.OPEN)
      throw new OStorageException("Storage " + name + " is not opened.");
  }

  @Override
  public long[] getClusterPositionsForEntry(int currentClusterId, long entry) {
    if (currentClusterId == -1)
      throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'');

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(currentClusterId);
      final OPhysicalPosition[] physicalPositions = cluster.getPositionsByEntryPos(entry);
      final long[] positions = new long[physicalPositions.length];

      for (int i = 0; i < positions.length; i++)
        positions[i] = physicalPositions[i].clusterPosition;

      return positions;
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }
}
