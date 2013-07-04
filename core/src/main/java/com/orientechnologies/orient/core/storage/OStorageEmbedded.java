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

import com.orientechnologies.common.concur.lock.OLockManager.LOCK;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Interface for embedded storage.
 * 
 * @author Luca Garulli
 * @see com.orientechnologies.orient.core.storage.impl.local.OStorageLocal, OStorageMemory
 */
public abstract class OStorageEmbedded extends OStorageAbstract {
  protected final ORecordLockManager lockManager;
  protected final String             PROFILER_CREATE_RECORD;
  protected final String             PROFILER_READ_RECORD;
  protected final String             PROFILER_UPDATE_RECORD;
  protected final String             PROFILER_DELETE_RECORD;

  public OStorageEmbedded(final String iName, final String iFilePath, final String iMode) {
    super(iName, iFilePath, iMode, OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsInteger());
    lockManager = new ORecordLockManager(OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger());

    PROFILER_CREATE_RECORD = "db." + name + ".createRecord";
    PROFILER_READ_RECORD = "db." + name + ".readRecord";
    PROFILER_UPDATE_RECORD = "db." + name + ".updateRecord";
    PROFILER_DELETE_RECORD = "db." + name + ".deleteRecord";
  }

  public abstract OCluster getClusterByName(final String iClusterName);

  protected abstract ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, boolean iAtomicLock,
      boolean loadTombstones);

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

    // COPY THE CONTEXT FROM THE REQUEST
    executor.setContext(iCommand.getContext());

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    return executeCommand(iCommand, executor);
  }

  public Object executeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor) {
    if (iCommand.isIdempotent() && !executor.isIdempotent())
      throw new OCommandExecutionException("Cannot execute non idempotent command");

    long beginTime = Orient.instance().getProfiler().startChrono();

    try {

      final Object result = executor.execute(iCommand.getParameters());
      return result;

    } catch (OException e) {
      // PASS THROUGHT
      throw e;
    } catch (Exception e) {
      throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);

    } finally {
      if (Orient.instance().getProfiler().isRecording())
        Orient
            .instance()
            .getProfiler()
            .stopChrono("db." + ODatabaseRecordThreadLocal.INSTANCE.get().getName() + ".command." + iCommand.toString(),
                "Command executed against the database", beginTime, "db.*.command.*");
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(currentClusterId);
      return cluster.higherPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(clusterId);
      return cluster.ceilingPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(currentClusterId);

      return cluster.lowerPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(clusterId);

      return cluster.floorPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public void acquireWriteLock(final ORID iRid) {
    lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
  }

  public void releaseWriteLock(final ORID iRid) {
    lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
  }

  public void acquireReadLock(final ORID iRid) {
    lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.SHARED);
  }

  public void releaseReadLock(final ORID iRid) {
    lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.SHARED);
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    if (rid.isNew())
      throw new OStorageException("Passed record with id " + rid + " is new and can not be stored.");

    checkOpeness();

    final OCluster cluster = getClusterById(rid.getClusterId());
    lock.acquireSharedLock();
    try {
      lockManager.acquireLock(Thread.currentThread(), rid, LOCK.SHARED);
      try {
        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
        if (ppos == null || ppos.dataSegmentId < 0)
          return null;

        return new ORecordMetadata(rid, ppos.recordVersion);
      } finally {
        lockManager.releaseLock(Thread.currentThread(), rid, LOCK.SHARED);
      }
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
    } finally {
      lock.releaseSharedLock();
    }

    return null;
  }

  protected OPhysicalPosition moveRecord(ORID originalId, ORID newId) throws IOException {
    final OCluster originalCluster = getClusterById(originalId.getClusterId());
    final OCluster destinationCluster = getClusterById(newId.getClusterId());

    if (destinationCluster.getDataSegmentId() != originalCluster.getDataSegmentId())
      throw new OStorageException("Original and destination clusters use different data segment ids "
          + originalCluster.getDataSegmentId() + "<->" + destinationCluster.getDataSegmentId());

    if (!destinationCluster.isHashBased()) {
      if (originalId.getClusterId() == newId.getClusterId())
        throw new OStorageException("Record identity can not be moved inside of the same non LH based cluster.");

      if (newId.getClusterPosition().compareTo(destinationCluster.getLastPosition()) <= 0)
        throw new OStorageException("New position " + newId.getClusterPosition() + " of " + originalId + " record inside of "
            + destinationCluster.getName() + " cluster and can not be used as destination");

      if (OGlobalConfiguration.USE_LHPEPS_CLUSTER.getValueAsBoolean()
          || OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean()) {
        if (destinationCluster.getFirstPosition().longValue() != 0
            || destinationCluster.getEntries() != destinationCluster.getLastPosition().longValue() + 1)
          throw new OStorageException("Cluster " + destinationCluster.getName()
              + " contains holes and can not be used as destination for " + originalId + " record.");
      }
    }

    final OPhysicalPosition ppos = originalCluster.getPhysicalPosition(new OPhysicalPosition(originalId.getClusterPosition()));

    if (ppos == null)
      throw new OStorageException("Record with id " + originalId + " does not exist");

    ppos.clusterPosition = newId.getClusterPosition();
    if (destinationCluster.isHashBased()) {
      if (!destinationCluster.addPhysicalPosition(ppos))
        throw new OStorageException("Record with id " + newId + " has already exists in cluster " + destinationCluster.getName());
    } else {
      final int diff = (int) (newId.getClusterPosition().longValue() - destinationCluster.getLastPosition().longValue() - 1);

      final OClusterPosition startPos = OClusterPositionFactory.INSTANCE
          .valueOf(destinationCluster.getLastPosition().longValue() + 1);
      OClusterPosition pos = startPos;

      final OPhysicalPosition physicalPosition = new OPhysicalPosition(pos);
      for (int i = 0; i < diff; i++) {
        physicalPosition.clusterPosition = pos;

        destinationCluster.addPhysicalPosition(physicalPosition);
        pos = pos.inc();
      }

      destinationCluster.addPhysicalPosition(ppos);

      pos = startPos;
      for (int i = 0; i < diff; i++) {
        destinationCluster.removePhysicalPosition(pos);
        pos = pos.inc();
      }
    }

    originalCluster.removePhysicalPosition(originalId.getClusterPosition());

    ORecordInternal<?> recordInternal = getLevel2Cache().freeRecord(originalId);
    if (recordInternal != null) {
      recordInternal.setIdentity(newId.getClusterId(), newId.getClusterPosition());
      getLevel2Cache().updateRecord(recordInternal);
    }

    return ppos;
  }

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  protected void checkOpeness() {
    if (status != STATUS.OPEN)
      throw new OStorageException("Storage " + name + " is not opened.");
  }
}
