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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionEntry;

/**
 * Memory implementation of storage. This storage works only in memory and has the following features:
 * <ul>
 * <li>The name is "Memory"</li>
 * <li>Has a unique Data Segment</li>
 * </ul>
 * 
 * @author Luca Garulli
 * 
 */
public class OStorageMemory extends OStorageAbstract {
	private final ODataSegmentMemory		data							= new ODataSegmentMemory();
	private final List<OClusterMemory>	clusters					= new ArrayList<OClusterMemory>();
	private int													defaultClusterId	= 0;

	public OStorageMemory(final String iURL) {
		super(iURL, iURL, "rw");
	}

	public void create() {
		open(-1, "", "");
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		addUser();

		final boolean locked = acquireExclusiveLock();
		try {
			configuration = new OStorageConfiguration(this);

			addDataSegment(OStorage.CLUSTER_DEFAULT_NAME);

			// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
			addCluster(OStorage.CLUSTER_INTERNAL_NAME, null);

			// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
			addCluster("index", null);

			// ADD THE DEFAULT CLUSTER
			defaultClusterId = addCluster(OStorage.CLUSTER_DEFAULT_NAME, null);

			configuration = new OStorageConfiguration(this);
			configuration.create();

			open = true;
		} catch (IOException e) {
		} finally {

			releaseExclusiveLock(locked);
		}
	}

	public void close() {
		final boolean locked = acquireExclusiveLock();
		try {

			// CLOSE ALL THE CLUSTERS
			for (OClusterMemory c : clusters)
				c.close();
			clusters.clear();

			// CLOSE THE DATA SEGMENT
			data.close();

			open = false;
		} finally {

			releaseExclusiveLock(locked);
		}
	}

	public int addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iParameters) {
		final boolean locked = acquireExclusiveLock();
		try {

			clusters.add(new OClusterMemory(clusters.size(), iClusterName));
			return clusters.size() - 1;
		} finally {

			releaseExclusiveLock(locked);
		}
	}

	public boolean removeCluster(final int iClusterId) {
		final boolean locked = acquireExclusiveLock();
		try {

			OCluster c = clusters.get(iClusterId);
			c.delete();
			clusters.set(iClusterId, null);

		} catch (IOException e) {
		} finally {

			releaseExclusiveLock(locked);
		}

		return false;
	}

	public int addDataSegment(final String iDataSegmentName) {
		// UNIQUE DATASEGMENT
		return 0;
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		return addDataSegment(iSegmentName);
	}

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireSharedLock();
		try {

			final long offset = data.createRecord(iContent);
			final OCluster cluster = getClusterById(iClusterId);

			return cluster.addPhysicalPosition(0, offset, iRecordType);
		} catch (IOException e) {
			throw new OStorageException("Error on create record in cluster: " + iClusterId, e);

		} finally {
			releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.createRecord", timer);
		}
	}

	public ORawBuffer readRecord(final ODatabaseRecord<?> iDatabase, final int iRequesterId, final int iClusterId,
			final long iClusterPosition, String iFetchPlan) {
		final OCluster cluster = getClusterById(iClusterId);

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireSharedLock();
		try {
			final OPhysicalPosition ppos = cluster.getPhysicalPosition(iClusterPosition, new OPhysicalPosition());

			if (ppos == null)
				return null;

			return new ORawBuffer(data.readRecord(ppos.dataPosition), ppos.version, ppos.type);
		} catch (IOException e) {
			throw new OStorageException("Error on read record in cluster: " + iClusterId, e);

		} finally {
			releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.readRecord", timer);
		}
	}

	public int updateRecord(final int iRequesterId, final int iClusterId, final long iClusterPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		final long timer = OProfiler.getInstance().startChrono();

		final OCluster cluster = getClusterById(iClusterId);

		final boolean locked = acquireSharedLock();
		try {
			OPhysicalPosition ppos = cluster.getPhysicalPosition(iClusterPosition, new OPhysicalPosition());
			if (ppos == null)
				return -1;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ ORecordId.generateString(iClusterId, iClusterPosition)
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			data.updateRecord(ppos.dataPosition, iContent);

			return ++(ppos.version);

		} catch (IOException e) {
			throw new OStorageException("Error on update record in cluster: " + iClusterId, e);

		} finally {
			releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.updateRecord", timer);
		}
	}

	public boolean deleteRecord(final int iRequesterId, final int iClusterId, final long iClusterPosition, final int iVersion) {
		final long timer = OProfiler.getInstance().startChrono();

		final OCluster cluster = getClusterById(iClusterId);

		final boolean locked = acquireSharedLock();
		try {
			OPhysicalPosition ppos = cluster.getPhysicalPosition(iClusterPosition, new OPhysicalPosition());

			if (ppos == null)
				return false;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ ORecordId.generateString(iClusterId, iClusterPosition)
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			cluster.removePhysicalPosition(iClusterPosition, null);
			data.deleteRecord(ppos.dataPosition);

			return true;

		} catch (IOException e) {
			throw new OStorageException("Error on delete record in cluster: " + iClusterId, e);

		} finally {
			releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.deleteRecord", timer);
		}
	}

	public long count(final int iClusterId) {
		final OCluster cluster = getClusterById(iClusterId);

		final boolean locked = acquireSharedLock();
		try {
			return cluster.getEntries();

		} catch (IOException e) {
			throw new OStorageException("Error on count record in cluster: " + iClusterId, e);

		} finally {
			releaseSharedLock(locked);
		}
	}

	public long[] getClusterDataRange(final int iClusterId) {
		final OCluster cluster = getClusterById(iClusterId);

		final boolean locked = acquireSharedLock();
		try {

			return new long[] { cluster.getFirstEntryPosition(), cluster.getLastEntryPosition() };
		} catch (IOException e) {

			throw new OStorageException("Error on getting last entry position in cluster: " + iClusterId, e);
		} finally {

			releaseSharedLock(locked);
		}
	}

	public long count(final int[] iClusterIds) {
		final boolean locked = acquireSharedLock();
		try {

			long tot = 0;
			for (int i = 0; i < iClusterIds.length; ++i)
				tot += clusters.get(iClusterIds[i]).getEntries();
			return tot;

		} finally {

			releaseSharedLock(locked);
		}
	}

	public OCluster getClusterByName(final String iClusterName) {
		final boolean locked = acquireSharedLock();
		try {
			for (int i = 0; i < clusters.size(); ++i)
				if (getClusterById(i).getName().equals(iClusterName))
					return getClusterById(i);
			return null;

		} finally {

			releaseSharedLock(locked);
		}
	}

	public int getClusterIdByName(final String iClusterName) {
		final boolean locked = acquireSharedLock();
		try {
			for (int i = 0; i < clusters.size(); ++i)
				if (getClusterById(i).getName().equals(iClusterName))
					return getClusterById(i).getId();
			return -1;

		} finally {

			releaseSharedLock(locked);
		}
	}

	public String getClusterTypeByName(final String iClusterName) {
		return OClusterMemory.TYPE;
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		for (int i = 0; i < clusters.size(); ++i)
			if (getClusterById(i).getId() == iClusterId)
				return getClusterById(i).getName();
		return null;
	}

	public Set<String> getClusterNames() {
		final boolean locked = acquireSharedLock();
		try {

			Set<String> result = new HashSet<String>();
			for (int i = 0; i < clusters.size(); ++i)
				result.add(getClusterById(i).getName());
			return result;

		} finally {

			releaseSharedLock(locked);
		}
	}

	public long count(final String iClassName) {
		throw new UnsupportedOperationException("count");
	}

	public List<ORecordSchemaAware<?>> query(final int iRequesterId, final OQuery<?> iQuery, final int iLimit) {
		throw new UnsupportedOperationException("count");
	}

	public ORecordSchemaAware<?> queryFirst(final int iRequesterId, final OQuery<?> iQuery) {
		throw new UnsupportedOperationException("count");
	}

	public void commit(final int iRequesterId, final OTransaction<?> iTx) {
		final boolean locked = acquireSharedLock();

		try {
			final List<OTransactionEntry<? extends ORecord<?>>> allEntries = new ArrayList<OTransactionEntry<? extends ORecord<?>>>();
			final List<OTransactionEntry<? extends ORecord<?>>> tmpEntries = new ArrayList<OTransactionEntry<? extends ORecord<?>>>();

			while (iTx.getEntries().iterator().hasNext()) {
				for (OTransactionEntry<? extends ORecord<?>> txEntry : iTx.getEntries())
					tmpEntries.add(txEntry);

				iTx.clearEntries();

				for (OTransactionEntry<? extends ORecord<?>> txEntry : tmpEntries)
					// COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
					commitEntry(iRequesterId, iTx.getId(), txEntry);

				allEntries.addAll(tmpEntries);
				tmpEntries.clear();
			}

			// UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
			OTransactionAbstract.updateCacheFromEntries(this, iTx, allEntries);

			allEntries.clear();
		} catch (IOException e) {
			rollback(iRequesterId, iTx);

		} finally {
			releaseSharedLock(locked);
		}
	}

	public void rollback(int iRequesterId, OTransaction<?> iTx) {
	}

	public void synch() {
	}

	public ODictionary<?> createDictionary(final ODatabaseRecord<?> iDatabase) throws Exception {
		return new ODictionaryMemory<Object>(iDatabase);
	}

	public void browse(int iRequesterId, int[] iClusterId, ORecordBrowsingListener iListener, ORecord<?> iRecord) {
	}

	public boolean exists() {
		return clusters.size() > 0;
	}

	public OCluster getClusterById(final int iClusterId) {
		return clusters.get(iClusterId);
	}

	public Collection<? extends OCluster> getClusters() {
		return Collections.unmodifiableCollection(clusters);
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public Object command(final OCommandRequestText iCommand) {
		return null;
	}

	@Override
	public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
		if (ppos.dataSegment > 0)
			return false;

		if (ppos.dataPosition >= data.size())
			return false;

		return true;
	}

	private void commitEntry(final int iRequesterId, final int iTxId, final OTransactionEntry<? extends ORecord<?>> txEntry)
			throws IOException {

		final ORecordId rid = (ORecordId) txEntry.record.getIdentity();

		final OCluster cluster = txEntry.clusterName != null ? getClusterByName(txEntry.clusterName) : getClusterById(rid.clusterId);

		switch (txEntry.status) {
		case OTransactionEntry.LOADED:
			break;

		case OTransactionEntry.CREATED:
			if (rid.isNew()) {
				rid.clusterPosition = createRecord(cluster.getId(), txEntry.record.toStream(), txEntry.record.getRecordType());
				rid.clusterId = cluster.getId();
			}
			break;

		case OTransactionEntry.UPDATED:
			txEntry.record.setVersion(updateRecord(iRequesterId, rid, txEntry.record.toStream(), txEntry.record.getVersion(),
					txEntry.record.getRecordType()));
			break;

		case OTransactionEntry.DELETED:
			deleteRecord(iRequesterId, rid, txEntry.record.getVersion());
			break;
		}
	}
}
