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
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;

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
	protected boolean			polymorphic;

	public ORecordIteratorClass(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
			final String iClassName, final boolean iPolymorphic) {
		super(iDatabase, iLowLevelDatabase);

		polymorphic = iPolymorphic;
		clusterIds = polymorphic ? database.getMetadata().getSchema().getClass(iClassName).getPolymorphicClusterIds() : database
				.getMetadata().getSchema().getClass(iClassName).getClusterIds();

		currentClusterIdx = 0; // START FROM THE FIRST CLUSTER

		updateClusterRange();
		current.clusterPosition = firstClusterPosition - 1;

		totalAvailableRecords = database.countClusterElements(clusterIds);

		txEntries = iDatabase.getTransaction().getRecordEntriesByClass(iClassName);

		if (txEntries != null)
			// ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
			for (OTransactionRecordEntry entry : txEntries) {
				if (entry.getRecord().getIdentity().isTemporary())
					totalAvailableRecords++;
				else if (entry.status == OTransactionRecordEntry.DELETED)
					totalAvailableRecords--;
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
			return current.clusterPosition > database.getStorage().getClusterDataRange(current.clusterId)[0];

		// ITERATE UNTIL THE PREVIOUS GOOD RECORD
		while (currentClusterIdx > -1) {
			if (current.clusterPosition > firstClusterPosition)
				return true;

			// CLUSTER EXHAUSTED, TRY WITH THE PREVIOUS ONE
			currentClusterIdx--;
			updateClusterRange();
			current.clusterPosition = lastClusterPosition + 1;
		}

		// CHECK IN TX IF ANY
		return txEntries != null && txEntries.size() - (currentTxEntryPosition + 1) > 0;
	}

	public boolean hasNext() {
		checkDirection(true);

		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return false;

		if (browsedRecords >= totalAvailableRecords)
			return false;

		// COMPUTE THE NUMBER OF RECORDS TO BROWSE
		if (liveUpdated)
			lastClusterPosition = database.getStorage().getClusterDataRange(current.clusterId)[1];

		// ITERATE UNTIL THE NEXT GOOD RECORD
		while (currentClusterIdx < clusterIds.length) {
			final long recordsToBrowse = current.clusterPosition > -2 && lastClusterPosition > -1 ? lastClusterPosition
					- current.clusterPosition : 0;

			if (recordsToBrowse > 0)
				return true;

			// CLUSTER EXHAUSTED, TRY WITH THE NEXT ONE
			currentClusterIdx++;
			if (currentClusterIdx >= clusterIds.length)
				break;
			updateClusterRange();
			current.clusterPosition = firstClusterPosition - 1;
		}

		// CHECK IN TX IF ANY
		return txEntries != null && txEntries.size() - (currentTxEntryPosition + 1) > 0;
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

		// ITERATE UNTIL THE PREVIOUS GOOD RECORD
		while (currentClusterIdx > -1) {

			// MOVE BACKWARD IN THE CURRENT CLUSTER
			while (hasPrevious()) {
				if ((record = readCurrentRecord(record, -1)) != null)
					// FOUND
					return (REC) record;
			}

			// CLUSTER EXHAUSTED, TRY WITH THE PREVIOUS ONE
			currentClusterIdx--;
			updateClusterRange();
			current.clusterPosition = lastClusterPosition + 1;
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

		ORecordInternal<?> record = getRecord();

		// ITERATE UNTIL THE NEXT GOOD RECORD
		while (currentClusterIdx < clusterIds.length) {

			// MOVE FORWARD IN THE CURRENT CLUSTER
			while (hasNext()) {
				record = getTransactionEntry();
				if (record != null)
					return (REC) record;

				if ((record = readCurrentRecord(null, +1)) != null)
					// FOUND
					return (REC) record;
			}

			// CLUSTER EXHAUSTED, TRY WITH THE NEXT ONE
			currentClusterIdx++;
			if (currentClusterIdx >= clusterIds.length)
				break;
			updateClusterRange();
			current.clusterPosition = firstClusterPosition - 1;
		}

		record = getTransactionEntry();
		if (record != null)
			return (REC) record;
		
		throw new NoSuchElementException();
	}

	public boolean isPolymorphic() {
		return polymorphic;
	}

	public ORecordInternal<?> current() {
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
		current.clusterPosition = -1;
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
		current.clusterPosition = liveUpdated ? database.countClusterElements(clusterIds[currentClusterIdx]) : lastClusterPosition + 1;
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
		lastClusterPosition = iLiveUpdated ? -1 : database.countClusterElements(current.clusterId);

		if (iLiveUpdated) {
			firstClusterPosition = -1;
			lastClusterPosition = -1;
		} else {
			updateClusterRange();
		}

		return this;
	}

	/**
	 * Read the current record and increment the counter if the record was found.
	 * 
	 * @param iRecord
	 * @return
	 */
	private ORecordInternal<?> readCurrentRecord(ORecordInternal<?> iRecord, final int iMovement) {
		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return null;

		current.clusterPosition += iMovement;

		if (iRecord != null) {
			iRecord.setIdentity(current);
			iRecord = lowLevelDatabase.load(iRecord, fetchPlan);
		} else
			iRecord = lowLevelDatabase.load(current, fetchPlan);

		if (iRecord != null) {
			browsedRecords++;
			return iRecord;
		}

		return null;
	}

	protected void updateClusterRange() {
		current.clusterId = clusterIds[currentClusterIdx];
		long[] range = database.getStorage().getClusterDataRange(current.clusterId);
		firstClusterPosition = range[0];
		lastClusterPosition = range[1];
	}
}
