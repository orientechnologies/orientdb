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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator can't change
 * it.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record Type
 */
public class ORecordIteratorCluster<REC extends ORecordInternal<?>> extends ORecordIterator<REC> {
	protected long	rangeFrom;
	protected long	rangeTo;

	public ORecordIteratorCluster(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
			final int iClusterId) {
		this(iDatabase, iLowLevelDatabase, iClusterId, -1, -1);
	}

	public ORecordIteratorCluster(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
			final int iClusterId, final long iRangeFrom, final long iRangeTo) {
		super(iDatabase, iLowLevelDatabase);
		if (iClusterId == ORID.CLUSTER_ID_INVALID)
			throw new IllegalArgumentException("The clusterId is invalid");

		current.clusterId = iClusterId;
		rangeFrom = iRangeFrom > -1 ? iRangeFrom - 1 : iRangeFrom;
		rangeTo = iRangeTo;

		long[] range = database.getStorage().getClusterDataRange(current.clusterId);
		firstClusterPosition = range[0];
		lastClusterPosition = range[1];

		totalAvailableRecords = database.countClusterElements(current.clusterId);

		txEntries = iDatabase.getTransaction().getRecordEntriesByClusterIds(new int[] { iClusterId });

		if (txEntries != null)
			// ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
			for (OTransactionRecordEntry entry : txEntries) {
				switch (entry.status) {
				case OTransactionRecordEntry.CREATED:
					totalAvailableRecords++;
					break;

				case OTransactionRecordEntry.DELETED:
					totalAvailableRecords--;
					break;
				}
			}

		begin();
	}

	@Override
	public boolean hasPrevious() {
		checkDirection(false);

		if (limit > -1 && browsedRecords >= limit)
			// LIMIT REACHED
			return false;

		return current.clusterPosition > getRangeFrom() + 1;
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
			lastClusterPosition = getRangeTo();

		long recordsToBrowse = current.clusterPosition > -2 && lastClusterPosition > -1 ? lastClusterPosition - current.clusterPosition
				: 0;

		if (recordsToBrowse > 0)
			return true;

		// CHECK IN TX IF ANY
		if (txEntries != null)
			recordsToBrowse += txEntries.size() - (currentTxEntryPosition + 1);

		return recordsToBrowse > 0;
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
		while (hasPrevious()) {
			if ((record = readCurrentRecord(record, -1)) != null)
				// FOUND
				return (REC) record;
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
		while (hasNext()) {
			record = getTransactionEntry();
			if (record != null)
				return (REC) record;

			if ((record = readCurrentRecord(null, +1)) != null)
				// FOUND
				return (REC) record;
		}

		throw new NoSuchElementException();
	}

	public ORecordInternal<?> current() {
		final ORecordInternal<?> record = getRecord();
		return readCurrentRecord(record, 0);
	}

	/**
	 * Move the iterator to the begin of the range. If no range was specified move to the first record of the cluster.
	 * 
	 * @return The object itself
	 */
	@Override
	public ORecordIterator<REC> begin() {
		current.clusterPosition = getRangeFrom();
		return this;
	}

	/**
	 * Move the iterator to the end of the range. If no range was specified move to the last record of the cluster.
	 * 
	 * @return The object itself
	 */
	@Override
	public ORecordIterator<REC> last() {
		current.clusterPosition = getRangeTo();
		return this;
	}

	/**
	 * Define the range where move the iterator forward and backward.
	 * 
	 * @param iFrom
	 *          Lower bound limit of the range
	 * @param iEnd
	 *          Upper bound limit of the range
	 * @return
	 */
	public ORecordIteratorCluster<REC> setRange(final long iFrom, final long iEnd) {
		firstClusterPosition = iFrom;
		rangeTo = iEnd;
		current.clusterPosition = firstClusterPosition;
		return this;
	}

	/**
	 * Return the lower bound limit of the range if any, otherwise 0.
	 * 
	 * @return
	 */
	public long getRangeFrom() {
		final long limit = (liveUpdated ? database.getStorage().getClusterDataRange(current.clusterId)[1] : firstClusterPosition) - 1;
		if (rangeFrom > -1)
			return Math.max(rangeFrom, limit);
		return limit;
	}

	/**
	 * Return the upper bound limit of the range if any, otherwise the last record.
	 * 
	 * @return
	 */
	public long getRangeTo() {
		final long limit = (liveUpdated ? database.getStorage().getClusterDataRange(current.clusterId)[1] : lastClusterPosition) + 1;
		if (rangeTo > -1)
			return Math.min(rangeTo, limit);
		return limit;
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

		// SET THE RANGE LIMITS
		if (iLiveUpdated) {
			firstClusterPosition = -1;
			lastClusterPosition = -1;
		} else {
			long[] range = database.getStorage().getClusterDataRange(current.clusterId);
			firstClusterPosition = range[0];
			lastClusterPosition = range[1];
		}

		totalAvailableRecords = database.countClusterElements(current.clusterId);

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

		if (iRecord != null)
			browsedRecords++;

		return iRecord;
	}
}
