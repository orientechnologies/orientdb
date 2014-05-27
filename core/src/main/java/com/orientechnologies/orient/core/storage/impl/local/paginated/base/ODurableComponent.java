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

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * Base class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * 
 * This class contains methods which are used to support such concepts as:
 * <ol>
 * <li>"atomic operation" - set of operations which should be either applied together or not. It includes not only changes on
 * current data structure but on all durable data structures which are used by current one during implementation of specific
 * operation.</li>
 * <li>write ahead log - log of all changes which were done with page content after loading it from cache.</li>
 * </ol>
 * 
 * 
 * To support of "atomic operation" concept following should be done:
 * <ol>
 * <li>Call {@link #startAtomicOperation()} method.</li>
 * <li>If changes should be isolated till operation completes call also {@link #lockTillAtomicOperationCompletes()} which will apply
 * exclusive lock on existing data structure till atomic operation completes. This lock is not replacement of data structure locking
 * system it is used to serialize access to set of components which participate in single atomic operation. It is kind of rudiment
 * lock manager which is used to isolate access to units which participate in single transaction and which is going to be created to
 * provide efficient multi core scalability feature. It is recommended to always call it just after start of atomic operation but
 * always remember it is not replacement of thread safety mechanics for current data structure it is a mean to provide isolation
 * between atomic operations.</li>
 * <li>Log all page changes in WAL by calling of {@link #logPageChanges(ODurablePage, long, long, boolean)}</li>
 * <li>Call {@link #endAtomicOperation(boolean)} method when atomic operation completes, passed in parameter should be
 * <code>false</code> if atomic operation completes with success and <code>true</code> if there were some exceptions and it is
 * needed to rollback given operation.</li>
 * </ol>
 * 
 * 
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  private OWriteAheadLog           writeAheadLog;
  private OAtomicOperationsManager atomicOperationsManager;

  public ODurableComponent() {
  }

  public ODurableComponent(int iTimeout) {
    super(iTimeout);
  }

  public ODurableComponent(boolean iConcurrent) {
    super(iConcurrent);
  }

  public ODurableComponent(boolean iConcurrent, int iTimeout, boolean ignoreThreadInterruption) {
    super(iConcurrent, iTimeout, ignoreThreadInterruption);
  }

  protected void init(final OAtomicOperationsManager atomicOperationsManager, final OWriteAheadLog writeAheadLog) {
    this.atomicOperationsManager = atomicOperationsManager;
    this.writeAheadLog = writeAheadLog;
  }

  protected void endAtomicOperation(boolean rollback) throws IOException {
    atomicOperationsManager.endAtomicOperation(rollback);
  }

  protected void startAtomicOperation() throws IOException {
    atomicOperationsManager.startAtomicOperation();
  }

  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    if (writeAheadLog != null) {
      final OPageChanges pageChanges = localPage.getPageChanges();
      if (pageChanges.isEmpty())
        return;

      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      assert atomicOperation != null;

      final OOperationUnitId unitId = atomicOperation.getOperationUnitId();
      final OLogSequenceNumber prevLsn;
      if (isNewPage)
        prevLsn = atomicOperation.getStartLSN();
      else
        prevLsn = localPage.getLsn();

      final OLogSequenceNumber lsn = writeAheadLog.log(new OUpdatePageRecord(pageIndex, fileId, unitId, pageChanges, prevLsn));
      localPage.setLsn(lsn);
    }
  }

  protected void logFileCreation(String fileName, long fileId) throws IOException {
    if (writeAheadLog != null) {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      assert atomicOperation != null;

      final OOperationUnitId unitId = atomicOperation.getOperationUnitId();
      writeAheadLog.log(new OFileCreatedCreatedWALRecord(unitId, fileName, fileId));
    }
  }

  protected void lockTillAtomicOperationCompletes() {
    atomicOperationsManager.lockTillOperationComplete(this);
  }

  protected ODurablePage.TrackMode getTrackMode() {
    final ODurablePage.TrackMode trackMode;

    if (writeAheadLog == null)
      trackMode = ODurablePage.TrackMode.NONE;
    else
      trackMode = ODurablePage.TrackMode.FULL;
    return trackMode;
  }

}
