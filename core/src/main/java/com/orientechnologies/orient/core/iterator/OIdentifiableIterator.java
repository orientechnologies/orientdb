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
package com.orientechnologies.orient.core.iterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator cannot change
 * it.
 * 
 * @author Luca Garulli
 */
public abstract class OIdentifiableIterator<REC extends OIdentifiable> implements Iterator<REC>, Iterable<REC> {
  protected final ODatabaseRecord  database;
  private final ODatabaseRecord    lowLevelDatabase;
  private final OStorage           dbStorage;

  protected boolean                liveUpdated            = false;
  protected long                   limit                  = -1;
  protected long                   browsedRecords         = 0;

  private String                   fetchPlan;
  private ORecordInternal<?>       reusedRecord           = null;                                          // DEFAULT = NOT
                                                                                                            // REUSE IT
  private Boolean                  directionForward;

  protected final ORecordId        current                = new ORecordId();

  protected long                   totalAvailableRecords;
  protected List<ORecordOperation> txEntries;

  protected int                    currentTxEntryPosition = -1;

  protected OClusterPosition       firstClusterEntry      = OClusterPositionFactory.INSTANCE.valueOf(0);
  protected OClusterPosition       lastClusterEntry       = OClusterPositionFactory.INSTANCE.getMaxValue();

  private OClusterPosition         currentEntry           = OClusterPosition.INVALID_POSITION;

  private int                      currentEntryPosition   = -1;
  private OPhysicalPosition[]      positionsToProcess     = null;

  private final boolean            useCache;
  private final boolean            iterateThroughTombstones;

  public OIdentifiableIterator(final ODatabaseRecord iDatabase, final ODatabaseRecord iLowLevelDatabase, final boolean useCache,
      final boolean iterateThroughTombstones) {
    database = iDatabase;
    lowLevelDatabase = iLowLevelDatabase;
    this.iterateThroughTombstones = iterateThroughTombstones;
    this.useCache = useCache;

    dbStorage = lowLevelDatabase.getStorage();

    current.clusterPosition = OClusterPosition.INVALID_POSITION; // DEFAULT = START FROM THE BEGIN
  }

  public boolean isIterateThroughTombstones() {
    return iterateThroughTombstones;
  }

  public abstract boolean hasPrevious();

  public abstract OIdentifiable previous();

  public abstract OIdentifiableIterator<REC> begin();

  public abstract OIdentifiableIterator<REC> last();

  public ORecordInternal<?> current() {
    return readCurrentRecord(getRecord(), 0);
  }

  protected ORecordInternal<?> getTransactionEntry() {
    boolean noPhysicalRecordToBrowse;

    if (current.clusterPosition.isTemporary())
      noPhysicalRecordToBrowse = true;
    else if (directionForward)
      noPhysicalRecordToBrowse = lastClusterEntry.compareTo(currentEntry) <= 0;
    else
      noPhysicalRecordToBrowse = currentEntry.compareTo(firstClusterEntry) <= 0;

    if (!noPhysicalRecordToBrowse && positionsToProcess.length == 0)
      noPhysicalRecordToBrowse = true;

    if (noPhysicalRecordToBrowse && txEntries != null) {
      // IN TX
      currentTxEntryPosition++;
      if (currentTxEntryPosition >= txEntries.size())
        throw new NoSuchElementException();
      else
        return txEntries.get(currentTxEntryPosition).getRecord();
    }
    return null;
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public void setFetchPlan(String fetchPlan) {
    this.fetchPlan = fetchPlan;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  /**
   * Tells if the iterator is using the same record for browsing.
   * 
   * @see #setReuseSameRecord(boolean)
   */
  public boolean isReuseSameRecord() {
    return reusedRecord != null;
  }

  public OClusterPosition getCurrentEntry() {
    return currentEntry;
  }

  /**
   * Tell to the iterator to use the same record for browsing. The record will be reset before every use. This improve the
   * performance and reduce memory utilization since it does not create a new one for each operation, but pay attention to copy the
   * data of the record once read otherwise they will be reset to the next operation.
   * 
   * @param reuseSameRecord
   *          if true the same record will be used for iteration. If false new record will be created each time iterator retrieves
   *          record from db.
   * @return @see #isReuseSameRecord()
   */
  public OIdentifiableIterator<REC> setReuseSameRecord(final boolean reuseSameRecord) {
    reusedRecord = (ORecordInternal<?>) (reuseSameRecord ? database.newInstance() : null);
    return this;
  }

  /**
   * Return the record to use for the operation.
   * 
   * @return the record to use for the operation.
   */
  protected ORecordInternal<?> getRecord() {
    final ORecordInternal<?> record;
    if (reusedRecord != null) {
      // REUSE THE SAME RECORD AFTER HAVING RESETTED IT
      record = reusedRecord;
      record.reset();
    } else
      record = null;
    return record;
  }

  /**
   * Return the iterator to be used in Java5+ constructs<br/>
   * <br/>
   * <code>
   * for( ORecordDocument rec : database.browseCluster( "Animal" ) ){<br/>
   * ...<br/>
   * }<br/>
   * </code>
   */
  public Iterator<REC> iterator() {
    return this;
  }

  /**
   * Return the current limit on browsing record. -1 means no limits (default).
   * 
   * @return The limit if setted, otherwise -1
   * @see #setLimit(long)
   */
  public long getLimit() {
    return limit;
  }

  /**
   * Set the limit on browsing record. -1 means no limits. You can set the limit even while you're browsing.
   * 
   * @param limit
   *          The current limit on browsing record. -1 means no limits (default).
   * @see #getLimit()
   */
  public OIdentifiableIterator<REC> setLimit(long limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Return current configuration of live updates.
   * 
   * @return True to activate it, otherwise false (default)
   * @see #setLiveUpdated(boolean)
   */
  public boolean isLiveUpdated() {
    return liveUpdated;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when concurrent deletes or additions change
   * the size of the cluster while you're browsing it. Default is false.
   * 
   * @param liveUpdated
   *          True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  public OIdentifiableIterator<REC> setLiveUpdated(boolean liveUpdated) {
    this.liveUpdated = liveUpdated;
    return this;
  }

  protected void checkDirection(final boolean iForward) {
    if (directionForward == null)
      // SET THE DIRECTION
      directionForward = iForward;
    else if (directionForward != iForward)
      throw new OIterationException("Iterator cannot change direction while browsing");
  }

  /**
   * Read the current record and increment the counter if the record was found.
   * 
   * @param iRecord
   *          to read value from database inside it. If record is null link will be created and stored in it.
   * @return record which was read from db.
   */
  protected ORecordInternal<?> readCurrentRecord(ORecordInternal<?> iRecord, final int iMovement) {
    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return null;

    do {
      final boolean moveResult;
      switch (iMovement) {
      case 1:
        moveResult = nextPosition();
        break;
      case -1:
        moveResult = prevPosition();
        break;
      case 0:
        moveResult = checkCurrentPosition();
        break;
      default:
        throw new IllegalStateException("Invalid movement value : " + iMovement);
      }

      if (!moveResult)
        return null;

      try {
        if (iRecord != null) {
          iRecord.setIdentity(new ORecordId(current.clusterId, current.clusterPosition));
          iRecord = lowLevelDatabase.load(iRecord, fetchPlan, !useCache, iterateThroughTombstones);
        } else
          iRecord = lowLevelDatabase.load(current, fetchPlan, !useCache, iterateThroughTombstones);
      } catch (ODatabaseException e) {
        if (Thread.interrupted())
          // THREAD INTERRUPTED: RETURN
          throw e;

        OLogManager.instance().error(this, "Error on fetching record during browsing. The record has been skipped", e);
      }

      if (iRecord != null) {
        browsedRecords++;
        return iRecord;
      }
    } while (iMovement != 0);

    return null;
  }

  protected boolean nextPosition() {
    if (positionsToProcess == null) {
      positionsToProcess = dbStorage.ceilingPhysicalPositions(current.clusterId, new OPhysicalPosition(firstClusterEntry));
      if (positionsToProcess == null)
        return false;
    } else {
      if (currentEntry.compareTo(lastClusterEntry) >= 0)
        return false;
    }

    incrementEntreePosition();
    while (positionsToProcess.length > 0 && currentEntryPosition >= positionsToProcess.length) {
      positionsToProcess = dbStorage.higherPhysicalPositions(current.clusterId, positionsToProcess[positionsToProcess.length - 1]);

      currentEntryPosition = -1;
      incrementEntreePosition();
    }

    if (positionsToProcess.length == 0)
      return false;

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry.compareTo(lastClusterEntry) > 0 || currentEntry.equals(OClusterPosition.INVALID_POSITION))
      return false;

    current.clusterPosition = currentEntry;
    return true;
  }

  protected boolean checkCurrentPosition() {
    if (currentEntry == null || currentEntry.equals(OClusterPosition.INVALID_POSITION)
        || firstClusterEntry.compareTo(currentEntry) > 0 || lastClusterEntry.compareTo(currentEntry) < 0)
      return false;

    current.clusterPosition = currentEntry;
    return true;
  }

  protected boolean prevPosition() {
    if (positionsToProcess == null) {
      positionsToProcess = dbStorage.floorPhysicalPositions(current.clusterId, new OPhysicalPosition(lastClusterEntry));
      if (positionsToProcess == null)
        return false;

      if (positionsToProcess.length == 0)
        return false;

      currentEntryPosition = positionsToProcess.length;
    } else {
      if (currentEntry.compareTo(firstClusterEntry) < 0)
        return false;
    }

    decrementEntreePosition();

    while (positionsToProcess.length > 0 && currentEntryPosition < 0) {
      positionsToProcess = dbStorage.lowerPhysicalPositions(current.clusterId, positionsToProcess[0]);
      currentEntryPosition = positionsToProcess.length;

      decrementEntreePosition();
    }

    if (positionsToProcess.length == 0)
      return false;

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry.compareTo(firstClusterEntry) < 0)
      return false;

    current.clusterPosition = currentEntry;
    return true;
  }

  private void decrementEntreePosition() {
    if (positionsToProcess.length > 0)
      if (iterateThroughTombstones)
        currentEntryPosition--;
      else
        do {
          currentEntryPosition--;
        } while (currentEntryPosition >= 0 && positionsToProcess[currentEntryPosition].recordVersion.isTombstone());
  }

  private void incrementEntreePosition() {
    if (positionsToProcess.length > 0)
      if (iterateThroughTombstones)
        currentEntryPosition++;
      else
        do {
          currentEntryPosition++;
        } while (currentEntryPosition < positionsToProcess.length
            && positionsToProcess[currentEntryPosition].recordVersion.isTombstone());
  }

  protected void resetCurrentPosition() {
    currentEntry = OClusterPosition.INVALID_POSITION;
    positionsToProcess = null;
    currentEntryPosition = -1;
  }

  protected OClusterPosition currentPosition() {
    return currentEntry;
  }
}
