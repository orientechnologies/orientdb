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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a
 * direction, the iterator cannot change it.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIdentifiableIterator<REC extends OIdentifiable>
    implements Iterator<REC>, Iterable<REC> {
  protected final ODatabaseDocumentInternal database;
  protected final ORecordId current = new ORecordId();
  private final OStorage dbStorage;
  protected boolean liveUpdated = false;
  protected long limit = -1;
  protected long browsedRecords = 0;
  protected OStorage.LOCKING_STRATEGY lockingStrategy = OStorage.LOCKING_STRATEGY.NONE;
  protected long totalAvailableRecords;
  protected List<ORecordOperation> txEntries;
  protected int currentTxEntryPosition = -1;
  protected long firstClusterEntry = 0;
  protected long lastClusterEntry = Long.MAX_VALUE;
  private String fetchPlan;
  private ORecord reusedRecord = null; // DEFAULT = NOT
  // REUSE IT
  private Boolean directionForward;
  private long currentEntry = ORID.CLUSTER_POS_INVALID;
  private int currentEntryPosition = -1;
  private OPhysicalPosition[] positionsToProcess = null;

  /**
   * Set of RIDs of records which were indicated as broken during cluster iteration. Mainly used
   * during JSON export/import procedure to fix links on broken records.
   */
  protected final Set<ORID> brokenRIDs = new HashSet<>();

  public OIdentifiableIterator(final ODatabaseDocumentInternal iDatabase) {
    this(iDatabase, OStorage.LOCKING_STRATEGY.NONE);
  }

  /** @deprecated usage of this constructor may lead to deadlocks. */
  @Deprecated
  public OIdentifiableIterator(
      final ODatabaseDocumentInternal iDatabase, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    database = iDatabase;
    lockingStrategy = iLockingStrategy;

    dbStorage = database.getStorage();
    current.setClusterPosition(ORID.CLUSTER_POS_INVALID); // DEFAULT = START FROM THE BEGIN
  }

  public abstract boolean hasPrevious();

  public abstract OIdentifiable previous();

  public abstract OIdentifiableIterator<REC> begin();

  public abstract OIdentifiableIterator<REC> last();

  public ORecord current() {
    return readCurrentRecord(getRecord(), 0);
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public Set<ORID> getBrokenRIDs() {
    return brokenRIDs;
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

  /**
   * Tell to the iterator to use the same record for browsing. The record will be reset before every
   * use. This improve the performance and reduce memory utilization since it does not create a new
   * one for each operation, but pay attention to copy the data of the record once read otherwise
   * they will be reset to the next operation.
   *
   * @param reuseSameRecord if true the same record will be used for iteration. If false new record
   *     will be created each time iterator retrieves record from db.
   * @return @see #isReuseSameRecord()
   */
  public OIdentifiableIterator<REC> setReuseSameRecord(final boolean reuseSameRecord) {
    reusedRecord = (ORecord) (reuseSameRecord ? database.newInstance() : null);
    return this;
  }

  public long getCurrentEntry() {
    return currentEntry;
  }

  /**
   * Return the iterator to be used in Java5+ constructs<br>
   * <br>
   * <code>
   * for( ORecordDocument rec : database.browseCluster( "Animal" ) ){<br> ...<br> }<br>
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
   * Set the limit on browsing record. -1 means no limits. You can set the limit even while you're
   * browsing.
   *
   * @param limit The current limit on browsing record. -1 means no limits (default).
   * @see #getLimit()
   */
  public OIdentifiableIterator<REC> setLimit(final long limit) {
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
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when
   * concurrent deletes or additions change the size of the cluster while you're browsing it.
   * Default is false.
   *
   * @param liveUpdated True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  public OIdentifiableIterator<REC> setLiveUpdated(final boolean liveUpdated) {
    this.liveUpdated = liveUpdated;
    return this;
  }

  protected ORecord getTransactionEntry() {
    boolean noPhysicalRecordToBrowse;

    if (current.getClusterPosition() < ORID.CLUSTER_POS_INVALID) noPhysicalRecordToBrowse = true;
    else if (directionForward) noPhysicalRecordToBrowse = lastClusterEntry <= currentEntry;
    else noPhysicalRecordToBrowse = currentEntry <= firstClusterEntry;

    if (!noPhysicalRecordToBrowse && positionsToProcess.length == 0)
      noPhysicalRecordToBrowse = true;

    if (noPhysicalRecordToBrowse && txEntries != null) {
      // IN TX
      currentTxEntryPosition++;
      if (currentTxEntryPosition >= txEntries.size()) throw new NoSuchElementException();
      else return txEntries.get(currentTxEntryPosition).getRecord();
    }
    return null;
  }

  /**
   * Return the record to use for the operation.
   *
   * @return the record to use for the operation.
   */
  protected ORecord getRecord() {
    final ORecord record;
    if (reusedRecord != null) {
      // REUSE THE SAME RECORD AFTER HAVING RESETTED IT
      record = reusedRecord;
      record.reset();
    } else record = null;
    return record;
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
   * @param iRecord to read value from database inside it. If record is null link will be created
   *     and stored in it.
   * @return record which was read from db.
   */
  protected ORecord readCurrentRecord(ORecord iRecord, final int iMovement) {
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

      if (!moveResult) return null;

      try {
        if (iRecord != null) {
          ORecordInternal.setIdentity(
              iRecord, new ORecordId(current.getClusterId(), current.getClusterPosition()));
          iRecord = database.load(iRecord, fetchPlan, false);
        } else iRecord = database.load(current, fetchPlan, false);
      } catch (ODatabaseException e) {
        if (Thread.interrupted() || database.isClosed())
          // THREAD INTERRUPTED: RETURN
          throw e;

        if (e.getCause() instanceof OSecurityException) throw e;

        brokenRIDs.add(current.copy());

        OLogManager.instance()
            .error(
                this, "Error on fetching record during browsing. The record has been skipped", e);
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
      positionsToProcess =
          dbStorage.ceilingPhysicalPositions(
              current.getClusterId(), new OPhysicalPosition(firstClusterEntry));
      if (positionsToProcess == null) return false;
    } else {
      if (currentEntry >= lastClusterEntry) return false;
    }

    incrementEntreePosition();
    while (positionsToProcess.length > 0 && currentEntryPosition >= positionsToProcess.length) {
      positionsToProcess =
          dbStorage.higherPhysicalPositions(
              current.getClusterId(), positionsToProcess[positionsToProcess.length - 1]);

      currentEntryPosition = -1;
      incrementEntreePosition();
    }

    if (positionsToProcess.length == 0) return false;

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry > lastClusterEntry || currentEntry == ORID.CLUSTER_POS_INVALID) return false;

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected boolean checkCurrentPosition() {
    if (currentEntry == ORID.CLUSTER_POS_INVALID
        || firstClusterEntry > currentEntry
        || lastClusterEntry < currentEntry) return false;

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected boolean prevPosition() {
    if (positionsToProcess == null) {
      positionsToProcess =
          dbStorage.floorPhysicalPositions(
              current.getClusterId(), new OPhysicalPosition(lastClusterEntry));
      if (positionsToProcess == null) return false;

      if (positionsToProcess.length == 0) return false;

      currentEntryPosition = positionsToProcess.length;
    } else {
      if (currentEntry < firstClusterEntry) return false;
    }

    decrementEntreePosition();

    while (positionsToProcess.length > 0 && currentEntryPosition < 0) {
      positionsToProcess =
          dbStorage.lowerPhysicalPositions(current.getClusterId(), positionsToProcess[0]);
      currentEntryPosition = positionsToProcess.length;

      decrementEntreePosition();
    }

    if (positionsToProcess.length == 0) return false;

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry < firstClusterEntry) return false;

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected void resetCurrentPosition() {
    currentEntry = ORID.CLUSTER_POS_INVALID;
    positionsToProcess = null;
    currentEntryPosition = -1;
  }

  protected long currentPosition() {
    return currentEntry;
  }

  protected static void checkForSystemClusters(
      final ODatabaseDocumentInternal iDatabase, final int[] iClusterIds) {
    if (iDatabase.isRemote()) {
      return;
    }

    for (int clId : iClusterIds) {
      if (iDatabase.getStorage().isSystemCluster(clId)) {
        final OSecurityUser dbUser = iDatabase.getUser();
        if (dbUser == null
            || dbUser.allow(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_READ)
                != null)
          // AUTHORIZED
          break;
      }
    }
  }

  private void decrementEntreePosition() {
    if (positionsToProcess.length > 0) currentEntryPosition--;
  }

  private void incrementEntreePosition() {
    if (positionsToProcess.length > 0) currentEntryPosition++;
  }
}
