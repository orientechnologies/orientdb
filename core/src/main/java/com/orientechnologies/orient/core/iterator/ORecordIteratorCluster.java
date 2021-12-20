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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a
 * direction, the iterator cannot change it.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordIteratorCluster<REC extends ORecord> extends OIdentifiableIterator<REC> {
  private ORecord currentRecord;

  public ORecordIteratorCluster(final ODatabaseDocumentInternal iDatabase, final int iClusterId) {
    this(
        iDatabase,
        iClusterId,
        ORID.CLUSTER_POS_INVALID,
        ORID.CLUSTER_POS_INVALID,
        OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  public ORecordIteratorCluster(
      final ODatabaseDocumentInternal iDatabase,
      final int iClusterId,
      final long firstClusterEntry,
      final long lastClusterEntry) {
    this(
        iDatabase, iClusterId, firstClusterEntry, lastClusterEntry, OStorage.LOCKING_STRATEGY.NONE);
  }

  protected ORecordIteratorCluster(final ODatabaseDocumentInternal database) {
    super(database, OStorage.LOCKING_STRATEGY.NONE);
  }

  @Deprecated
  public ORecordIteratorCluster(
      final ODatabaseDocumentInternal iDatabase,
      final int iClusterId,
      final long firstClusterEntry,
      final long lastClusterEntry,
      final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    super(iDatabase, iLockingStrategy);

    if (iClusterId == ORID.CLUSTER_ID_INVALID)
      throw new IllegalArgumentException("The clusterId is invalid");

    checkForSystemClusters(iDatabase, new int[] {iClusterId});

    current.setClusterId(iClusterId);
    final long[] range = database.getClusterDataRange(current.getClusterId());

    if (firstClusterEntry == ORID.CLUSTER_POS_INVALID) this.firstClusterEntry = range[0];
    else this.firstClusterEntry = firstClusterEntry > range[0] ? firstClusterEntry : range[0];

    if (lastClusterEntry == ORID.CLUSTER_POS_INVALID) this.lastClusterEntry = range[1];
    else this.lastClusterEntry = lastClusterEntry < range[1] ? lastClusterEntry : range[1];

    totalAvailableRecords = database.countClusterElements(current.getClusterId());

    txEntries = iDatabase.getTransaction().getNewRecordEntriesByClusterIds(new int[] {iClusterId});

    if (txEntries != null)
      // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
      for (ORecordOperation entry : txEntries) {
        switch (entry.type) {
          case ORecordOperation.CREATED:
            totalAvailableRecords++;
            break;

          case ORecordOperation.DELETED:
            totalAvailableRecords--;
            break;
        }
      }

    begin();
  }

  @Override
  public boolean hasPrevious() {
    checkDirection(false);

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    boolean thereAreRecordsToBrowse = getCurrentEntry() > firstClusterEntry;

    if (thereAreRecordsToBrowse) {
      ORecord record = getRecord();
      currentRecord = readCurrentRecord(record, -1);
    }

    return currentRecord != null;
  }

  public boolean hasNext() {
    checkDirection(true);

    if (Thread.interrupted())
      // INTERRUPTED
      return false;

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords) return false;

    if (!(current.getClusterPosition() < ORID.CLUSTER_POS_INVALID)
        && getCurrentEntry() < lastClusterEntry) {
      ORecord record = getRecord();
      try {
        currentRecord = readCurrentRecord(record, +1);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during read of record", e);

        final ORID recordRid = record == null ? null : record.getIdentity();

        if (recordRid != null) brokenRIDs.add(recordRid.copy());

        currentRecord = null;
      }

      if (currentRecord != null) return true;
    }

    // CHECK IN TX IF ANY
    if (txEntries != null) return txEntries.size() - (currentTxEntryPosition + 1) > 0;

    return false;
  }

  /**
   * Return the element at the current position and move backward the stream to the previous
   * position available.
   *
   * @return the previous record found, otherwise the NoSuchElementException exception is thrown
   *     when no more records are found.
   */
  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    checkDirection(false);

    if (currentRecord != null) {
      try {
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }
    }
    // ITERATE UNTIL THE PREVIOUS GOOD RECORD
    while (hasPrevious()) {
      try {
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }
    }

    return null;
  }

  /**
   * Return the element at the current position and move forward the stream to the next position
   * available.
   *
   * @return the next record found, otherwise the NoSuchElementException exception is thrown when no
   *     more records are found.
   */
  @SuppressWarnings("unchecked")
  public REC next() {
    checkDirection(true);

    ORecord record;

    // ITERATE UNTIL THE NEXT GOOD RECORD
    while (hasNext()) {
      // FOUND
      if (currentRecord != null) {
        try {
          return (REC) currentRecord;
        } finally {
          currentRecord = null;
        }
      }

      record = getTransactionEntry();
      if (record != null) return (REC) record;
    }

    return null;
  }

  /**
   * Move the iterator to the begin of the range. If no range was specified move to the first record
   * of the cluster.
   *
   * @return The object itself
   */
  @Override
  public ORecordIteratorCluster<REC> begin() {
    browsedRecords = 0;

    updateRangesOnLiveUpdate();
    resetCurrentPosition();

    currentRecord = readCurrentRecord(getRecord(), +1);

    return this;
  }

  /**
   * Move the iterator to the end of the range. If no range was specified move to the last record of
   * the cluster.
   *
   * @return The object itself
   */
  @Override
  public ORecordIteratorCluster<REC> last() {
    browsedRecords = 0;

    updateRangesOnLiveUpdate();
    resetCurrentPosition();

    currentRecord = readCurrentRecord(getRecord(), -1);

    return this;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when
   * concurrent deletes or additions change the size of the cluster while you're browsing it.
   * Default is false.
   *
   * @param iLiveUpdated True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  @Override
  public ORecordIteratorCluster<REC> setLiveUpdated(boolean iLiveUpdated) {
    super.setLiveUpdated(iLiveUpdated);

    // SET THE RANGE LIMITS
    if (iLiveUpdated) {
      firstClusterEntry = 0L;
      lastClusterEntry = Long.MAX_VALUE;
    } else {
      final long[] range = database.getClusterDataRange(current.getClusterId());
      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }

    totalAvailableRecords = database.countClusterElements(current.getClusterId());

    return this;
  }

  private void updateRangesOnLiveUpdate() {
    if (liveUpdated) {
      final long[] range = database.getClusterDataRange(current.getClusterId());

      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }
  }
}
