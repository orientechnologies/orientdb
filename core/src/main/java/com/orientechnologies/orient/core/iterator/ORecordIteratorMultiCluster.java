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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Iterator class to browse forward and backward the records of a cluster. This iterator with "live updates" setted is able to catch
 * updates to the cluster sizes while browsing. This is the case when concurrent clients/threads insert and remove item in any
 * cluster the iterator is browsing. If the cluster are hot removed by from the database the iterator could be invalid and throw
 * exception of cluster not found.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record Type
 */
public class ORecordIteratorMultiCluster<REC extends ORecordInternal<?>> extends ORecordIterator<REC> {
	private final int[]	clusterIds;
	private int					currentClusterIdx;

	private int					lastClusterId;
	private long				lastClusterPosition;
	private long				totalAvailableRecords;

	public ORecordIteratorMultiCluster(final ODatabaseRecord<REC> iDatabase, final ODatabaseRecordAbstract<REC> iLowLevelDatabase,
			final int[] iClusterIds) {
		super(iDatabase, iLowLevelDatabase);

		clusterIds = iClusterIds;

		currentClusterIdx = 0; // START FROM THE FIRST CLUSTER
		currentClusterPosition = -1; // DEFAULT = START FROM THE BEGIN

		lastClusterId = clusterIds[clusterIds.length - 1];
		lastClusterPosition = database.getStorage().getClusterLastEntryPosition(lastClusterId);

		totalAvailableRecords = database.countClusterElements(iClusterIds);
	}

	@Override
	public boolean hasPrevious() {
		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return false;

		return currentClusterPosition > 0;
	}

	public boolean hasNext() {
		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return false;
		
		if( browsedRecords >= totalAvailableRecords )
			return false;

		if (currentClusterIdx < clusterIds.length - 1)
			// PRESUME THAT IF IT'S NOT AT THE LAST CLUSTER THERE COULD BE OTHER ELEMENTS
			return true;

		if (liveUpdated)
			return currentClusterPosition < database.getStorage().getClusterLastEntryPosition(lastClusterId) - 1;

		return currentClusterPosition < lastClusterPosition - 1;
	}

	/**
	 * Return the element at the current position and move backward the cursor to the previous position available.
	 * 
	 * @return the previous record found, otherwise NULL when no more records are found.
	 */
	@Override
	public REC previous() {
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

		return null;
	}

	/**
	 * Return the element at the current position and move forward the cursor to the next position available.
	 * 
	 * @return the next record found, otherwise NULL when no more records are found.
	 */
	public REC next() {
		final REC record = getRecord();

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

		return null;
	}

	public REC current() {
		final REC record = getRecord();
		return readCurrentRecord(record, 0);
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
		currentClusterPosition = liveUpdated ? database.countClusterElements(clusterIds[currentClusterIdx]) : lastClusterPosition;
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

		return this;
	}

	/**
	 * Read the current record and increment the counter if the record was found.
	 * 
	 * @param iRecord
	 * @return
	 */
	private REC readCurrentRecord(final REC iRecord, final int iMovement) {
		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return null;

		currentClusterPosition += iMovement;

		if (lowLevelDatabase.executeReadRecord(clusterIds[currentClusterIdx], currentClusterPosition, iRecord, fetchPlan) != null) {
			browsedRecords++;
			return iRecord;
		}

		return null;
	}
}
