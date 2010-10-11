/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionEntry;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator can't change
 * it. This iterator with "live updates" set is able to catch updates to the cluster sizes while browsing. This is the case when
 * concurrent clients/threads insert and remove item in any cluster the iterator is browsing. If the cluster are hot removed by from
 * the database the iterator could be invalid and throw exception of cluster not found.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record Type
 */
public class ORecordIteratorClass<REC extends ORecordInternal<?>> extends ORecordIterator<REC> {
	protected final int[]	clusterIds;
	protected int					currentClusterIdx;

	protected int					lastClusterId;
	protected long				firstClusterPosition;
	protected long				lastClusterPosition;
	protected long				totalAvailableRecords;

	public ORecordIteratorClass(final ODatabaseRecord<REC> iDatabase, final ODatabaseRecordAbstract<REC> iLowLevelDatabase,
			final String iClassName) {
		super(iDatabase, iLowLevelDatabase);

		clusterIds = database.getMetadata().getSchema().getClass(iClassName).getClusterIds();

		currentClusterIdx = 0; // START FROM THE FIRST CLUSTER

		lastClusterId = clusterIds[clusterIds.length - 1];

		long[] range = database.getStorage().getClusterDataRange(lastClusterId);
		firstClusterPosition = range[0];
		lastClusterPosition = range[1];

		currentClusterPosition = firstClusterPosition - 1;

		totalAvailableRecords = database.countClusterElements(clusterIds);

		txEntries = iDatabase.getTransaction().getEntriesByClass(iClassName);

		if (txEntries != null)
			// ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
			for (OTransactionEntry<?> entry : txEntries) {
				switch (entry.status) {
				case OTransactionEntry.CREATED:
					totalAvailableRecords++;
					break;

				case OTransactionEntry.DELETED:
					totalAvailableRecords--;
					break;
				}
			}
	}

	@Override
	public boolean hasPrevious() {
		checkDirection(false);

		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return false;

		if (browsedRecords >= totalAvailableRecords)
			return false;

		if (liveUpdated)
			return currentClusterPosition > database.getStorage().getClusterDataRange(lastClusterId)[0];

		return currentClusterPosition > firstClusterPosition;
	}

	public boolean hasNext() {
		checkDirection(true);

		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return false;

		if (browsedRecords >= totalAvailableRecords)
			return false;

		if (currentClusterIdx < clusterIds.length - 1)
			// PRESUME THAT IF IT'S NOT AT THE LAST CLUSTER THERE COULD BE OTHER ELEMENTS
			return true;

		// COMPUTE THE NUMBER OF RECORDS TO BROWSE
		if (liveUpdated)
			lastClusterPosition = database.getStorage().getClusterDataRange(lastClusterId)[1];

		final long recordsToBrowse = currentClusterPosition > -2 && lastClusterPosition > -1 ? lastClusterPosition
				- currentClusterPosition : 0;

		if (recordsToBrowse <= 0)
			return hasTxEntry();

		return true;
	}

	/**
	 * Return the element at the current position and move backward the cursor to the previous position available.
	 * 
	 * @return the previous record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
	 */
	@Override
	public REC previous() {
		checkDirection(false);

		final REC record = getRecord();

		// ITERATE UNTIL THE PREVIOUS GOOD RECORD
		while (currentClusterIdx > -1) {

			// MOVE BACKWARD IN THE CURRENT CLUSTER
			while (hasPrevious()) {
				if (readCurrentRecord(record, -1) != null)
					// FOUND
					return record;
			}

			// CLUSTER EXHAUSTED, TRY WITH THE PREVIOUS ONE
			currentClusterIdx--;
		}

		throw new NoSuchElementException();
	}

	/**
	 * Return the element at the current position and move forward the cursor to the next position available.
	 * 
	 * @return the next record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
	 */
	@SuppressWarnings("unchecked")
	public REC next() {
		checkDirection(true);

		final REC record = getRecord();

		if (currentTxEntryPosition > -1)
			// IN TX
			return (REC) txEntries.get(currentTxEntryPosition).record;

		// ITERATE UNTIL THE NEXT GOOD RECORD
		while (currentClusterIdx < clusterIds.length) {

			// MOVE FORWARD IN THE CURRENT CLUSTER
			while (hasNext()) {
				if (readCurrentRecord(record, +1) != null)
					// FOUND
					return record;
			}

			// CLUSTER EXHAUSTED, TRY WITH THE NEXT ONE
			currentClusterIdx++;
		}

		throw new NoSuchElementException();
	}

	public REC current() {
		return readCurrentRecord(getRecord(), 0);
	}

	/**
	 * Move the iterator to the begin of the range. If no range was specified move to the first record of the cluster.
	 * 
	 * @return The object itself
	 */
	@Override
	public ORecordIterator<REC> begin() {
		currentClusterIdx = 0;
		currentClusterPosition = -1;
		return this;
	}

	/**
	 * Move the iterator to the end of the range. If no range was specified move to the last record of the cluster.
	 * 
	 * @return The object itself
	 */
	@Override
	public ORecordIterator<REC> last() {
		currentClusterIdx = clusterIds.length - 1;
		currentClusterPosition = liveUpdated ? database.countClusterElements(clusterIds[currentClusterIdx]) : lastClusterPosition + 1;
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
	public ORecordIterator<REC> setLiveUpdated(boolean iLiveUpdated) {
		super.setLiveUpdated(iLiveUpdated);

		// SET THE UPPER LIMIT TO -1 IF IT'S ENABLED
		lastClusterPosition = iLiveUpdated ? -1 : database.countClusterElements(lastClusterId);

		if (iLiveUpdated) {
			firstClusterPosition = -1;
			lastClusterPosition = -1;
		} else {
			long[] range = database.getStorage().getClusterDataRange(lastClusterId);
			firstClusterPosition = range[0];
			lastClusterPosition = range[1];
		}

		return this;
	}

	/**
	 * Read the current record and increment the counter if the record was found.
	 * 
	 * @param iRecord
	 * @return
	 */
	private REC readCurrentRecord(REC iRecord, final int iMovement) {
		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return null;

		currentClusterPosition += iMovement;

		iRecord = loadRecord(iRecord);

		if (iRecord != null) {
			browsedRecords++;
			return iRecord;
		}

		return null;
	}

	protected REC loadRecord(final REC iRecord) {
		return lowLevelDatabase.executeReadRecord(clusterIds[currentClusterIdx], currentClusterPosition, iRecord, fetchPlan);
	}
}
