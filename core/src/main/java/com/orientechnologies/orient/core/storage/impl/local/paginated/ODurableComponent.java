package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  private ThreadLocal<OOperationUnitId>   currentUnitId = new ThreadLocal<OOperationUnitId>();
  private ThreadLocal<OLogSequenceNumber> startLSN      = new ThreadLocal<OLogSequenceNumber>();

  private OWriteAheadLog                  writeAheadLog;

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

  protected void init(OWriteAheadLog writeAheadLog) {
    this.writeAheadLog = writeAheadLog;
  }

  protected void endDurableOperation(OStorageTransaction transaction, boolean rollback) throws IOException {
    if (transaction == null && writeAheadLog != null) {
      writeAheadLog.log(new OAtomicUnitEndRecord(currentUnitId.get(), rollback));
    }

    currentUnitId.set(null);
    startLSN.set(null);
  }

  protected void startDurableOperation(OStorageTransaction transaction) throws IOException {
    if (transaction == null) {
      if (writeAheadLog != null) {
        OOperationUnitId unitId = OOperationUnitId.generateId();

        OLogSequenceNumber lsn = writeAheadLog.log(new OAtomicUnitStartRecord(true, unitId));
        startLSN.set(lsn);
        currentUnitId.set(unitId);
      }
    } else {
      startLSN.set(transaction.getStartLSN());
      currentUnitId.set(transaction.getOperationUnitId());
    }
  }

  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    if (writeAheadLog != null) {
      OPageChanges pageChanges = localPage.getPageChanges();
      if (pageChanges.isEmpty())
        return;

      OOperationUnitId unitId = currentUnitId.get();
      assert unitId != null;

      OLogSequenceNumber prevLsn;
      if (isNewPage)
        prevLsn = startLSN.get();
      else
        prevLsn = localPage.getLsn();

      OLogSequenceNumber lsn = writeAheadLog.log(new OUpdatePageRecord(pageIndex, fileId, unitId, pageChanges, prevLsn));

      localPage.setLsn(lsn);
    }
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
