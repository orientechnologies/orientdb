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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator cannot change
 * it.
 * 
 * @author Luca Garulli
 */
public class ORecordIteratorCluster<REC extends ORecordInternal<?>> extends OIdentifiableIterator<REC> {
  private ORecord<?> currentRecord;

  public ORecordIteratorCluster(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
      final int iClusterId) {
    super(iDatabase, iLowLevelDatabase);

    if (iClusterId == ORID.CLUSTER_ID_INVALID)
      throw new IllegalArgumentException("The clusterId is invalid");

    current.clusterId = iClusterId;
    final OClusterPosition[] range = database.getStorage().getClusterDataRange(current.clusterId);

    firstClusterEntry = range[0];
    lastClusterEntry = range[1];

    totalAvailableRecords = database.countClusterElements(current.clusterId);

    txEntries = iDatabase.getTransaction().getNewRecordEntriesByClusterIds(new int[] { iClusterId });

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

    boolean thereAreRecordsToBrowse = currentEntry.compareTo(firstClusterEntry) > 0;

    if (thereAreRecordsToBrowse) {
      ORecordInternal<?> record = getRecord();
      currentRecord = readCurrentRecord(record, -1);
    }

    return thereAreRecordsToBrowse;
  }

  private void updateRangesOnLiveUpdate() {
    if (liveUpdated) {
      OClusterPosition[] range = dbStorage.getClusterDataRange(current.clusterId);

      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }
  }

  public boolean hasNext() {
    checkDirection(true);

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords)
      return false;

    boolean thereAreRecordsToBrowse;

    if (current.clusterPosition.isTemporary())
      thereAreRecordsToBrowse = false;
    else if (currentEntry.compareTo(lastClusterEntry) < 0)
      thereAreRecordsToBrowse = true;
    else
      thereAreRecordsToBrowse = false;

    if (thereAreRecordsToBrowse) {
      ORecordInternal<?> record = getRecord();
      currentRecord = readCurrentRecord(record, +1);
      return true;
    }

    // CHECK IN TX IF ANY
    if (txEntries != null)
      thereAreRecordsToBrowse = txEntries.size() - (currentTxEntryPosition + 1) > 0;

    return thereAreRecordsToBrowse;
  }

  /**
   * Return the element at the current position and move backward the cursor to the previous position available.
   * 
   * @return the previous record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
   */
  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    checkDirection(false);

    ORecordInternal<?> record = getRecord();

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
   * Return the element at the current position and move forward the cursor to the next position available.
   * 
   * @return the next record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
   */
  @SuppressWarnings("unchecked")
  public REC next() {
    checkDirection(true);

    ORecordInternal<?> record;

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
      if (record != null)
        return (REC) record;
    }

    return null;
  }

  /**
   * Move the iterator to the begin of the range. If no range was specified move to the first record of the cluster.
   * 
   * @return The object itself
   */
  @Override
  public ORecordIteratorCluster<REC> begin() {
    updateRangesOnLiveUpdate();

    currentEntry = firstClusterEntry;
    currentRecord = readCurrentRecord(getRecord(), 0);

    return this;
  }

  /**
   * Move the iterator to the end of the range. If no range was specified move to the last record of the cluster.
   * 
   * @return The object itself
   */
  @Override
  public ORecordIteratorCluster<REC> last() {
    updateRangesOnLiveUpdate();

    currentEntry = lastClusterEntry;
    currentRecord = readCurrentRecord(getRecord(), 0);

    return this;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when concurrent deletes or additions change
   * the size of the cluster while you're browsing it. Default is false.
   * 
   * @param iLiveUpdated
   *          True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  @Override
  public ORecordIteratorCluster<REC> setLiveUpdated(boolean iLiveUpdated) {
    super.setLiveUpdated(iLiveUpdated);

    // SET THE RANGE LIMITS
    if (iLiveUpdated) {
      firstClusterEntry = OClusterPosition.INVALID_POSITION;
      lastClusterEntry = OClusterPosition.INVALID_POSITION;
    } else {
      OClusterPosition[] range = database.getStorage().getClusterDataRange(current.clusterId);
      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }

    totalAvailableRecords = database.countClusterElements(current.clusterId);

    return this;
  }
}
