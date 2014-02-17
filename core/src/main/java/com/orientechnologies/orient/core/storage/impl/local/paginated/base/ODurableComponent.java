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
