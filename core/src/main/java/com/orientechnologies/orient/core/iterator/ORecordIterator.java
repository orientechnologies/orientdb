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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator cannot change
 * it.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record Type
 */
public abstract class ORecordIterator<REC extends ORecordInternal<?>> implements Iterator<REC>, Iterable<REC> {
	protected final ODatabaseRecord					database;
	protected final ODatabaseRecordAbstract	lowLevelDatabase;

	protected boolean												liveUpdated							= false;
	protected long													limit										= -1;
	protected long													browsedRecords					= 0;
	protected String												fetchPlan;
	private ORecordInternal<?>							reusedRecord						= null;						// DEFAULT = NOT REUSE IT
	protected Boolean												directionForward;
	protected final ORecordId								current									= new ORecordId();
	protected long													firstClusterPosition;
	protected long													lastClusterPosition;
	protected long													totalAvailableRecords;
	protected List<OTransactionRecordEntry>	txEntries;
	protected int														currentTxEntryPosition	= -1;

	public ORecordIterator(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase) {
		database = iDatabase;
		lowLevelDatabase = iLowLevelDatabase;

		current.clusterPosition = -1; // DEFAULT = START FROM THE BEGIN
	}

	public abstract boolean hasPrevious();

	public abstract REC previous();

	public abstract ORecordIterator<REC> begin();

	public abstract ORecordIterator<REC> last();

	@SuppressWarnings("unchecked")
	protected REC getTransactionEntry() {
		final long physicalRecordTobrowse = current.clusterPosition > -2 && lastClusterPosition > -1 ? lastClusterPosition
				- current.clusterPosition : 0;

		if (physicalRecordTobrowse == 0 && txEntries != null) {
			// IN TX
			currentTxEntryPosition++;
			if (currentTxEntryPosition >= txEntries.size())
				throw new NoSuchElementException();
			else
				return (REC) txEntries.get(currentTxEntryPosition).getRecord();
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

	/**
	 * Tell to the iterator to use the same record for browsing. The record will be reset before every use. This improve the
	 * performance and reduce memory utilization since it does not create a new one for each operation, but pay attention to copy the
	 * data of the record once read otherwise they will be reset to the next operation.
	 * 
	 * @param reuseSameRecord
	 * @return @see #isReuseSameRecord()
	 */
	public ORecordIterator<REC> setReuseSameRecord(final boolean reuseSameRecord) {
		reusedRecord = (ORecordInternal<?>) (reuseSameRecord ? database.newInstance() : null);
		return this;
	}

	/**
	 * Return the record to use for the operation.
	 * 
	 * @return
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
	public ORecordIterator<REC> setLimit(long limit) {
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
	public ORecordIterator<REC> setLiveUpdated(boolean liveUpdated) {
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
}
