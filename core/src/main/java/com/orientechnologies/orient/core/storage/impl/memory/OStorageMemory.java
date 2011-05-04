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
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;

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
public class OStorageMemory extends OStorageEmbedded {
	private final ODataSegmentMemory		data							= new ODataSegmentMemory();
	private final List<OClusterMemory>	clusters					= new ArrayList<OClusterMemory>();
	private int													defaultClusterId	= 0;
	private ODictionary<?>							dictionary;

	public OStorageMemory(final String iURL) {
		super(iURL, iURL, "rw");
		configuration = new OStorageConfiguration(this);
	}

	public void create(final Map<String, Object> iOptions) {
		addUser();
		level2cache.startup();

		final boolean locked = lock.acquireExclusiveLock();
		try {
			addDataSegment(OStorage.DATA_DEFAULT_NAME);

			// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
			addCluster(OStorage.CLUSTER_INTERNAL_NAME, null);

			// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
			addCluster(OStorage.CLUSTER_INDEX_NAME, null);

			// ADD THE DEFAULT CLUSTER
			defaultClusterId = addCluster(OStorage.CLUSTER_DEFAULT_NAME, null);

			open = true;
		} finally {

			lock.releaseExclusiveLock(locked);
		}
	}

	public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
		addUser();
		level2cache.startup();

		if (open)
			// ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
			// REUSED
			return;

		if (!exists())
			throw new OStorageException("Can't open the storage '" + name + "' because it not exists in path: " + url);

		open = true;
	}

	public void close(final boolean iForce) {
		final boolean locked = lock.acquireExclusiveLock();
		try {
			if (!checkForClose(iForce))
				return;

			// CLOSE ALL THE CLUSTERS
			for (OClusterMemory c : clusters)
				c.close();
			clusters.clear();

			// CLOSE THE DATA SEGMENT
			data.close();

			level2cache.shutdown();
			sharedResources.clear();

			open = false;
		} finally {

			lock.releaseExclusiveLock(locked);
		}
	}

	public void delete() {
		close();
	}

	public int addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iParameters) {
		final boolean locked = lock.acquireExclusiveLock();
		try {

			clusters.add(new OClusterMemory(clusters.size(), iClusterName.toLowerCase()));
			return clusters.size() - 1;
		} finally {

			lock.releaseExclusiveLock(locked);
		}
	}

	public boolean removeCluster(final int iClusterId) {
		final boolean locked = lock.acquireExclusiveLock();
		try {

			OCluster c = clusters.get(iClusterId);
			c.delete();
			clusters.set(iClusterId, null);

		} catch (IOException e) {
		} finally {

			lock.releaseExclusiveLock(locked);
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

	public long createRecord(final ORecordId iRid, final byte[] iContent, final byte iRecordType) {
		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();
		try {

			final long offset = data.createRecord(iContent);
			final OCluster cluster = getClusterById(iRid.clusterId);

			iRid.clusterPosition = cluster.addPhysicalPosition(0, offset, iRecordType);
			return iRid.clusterPosition;
		} catch (IOException e) {
			throw new OStorageException("Error on create record in cluster: " + iRid.clusterId, e);

		} finally {
			lock.releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.createRecord", timer);
		}
	}

	public ORawBuffer readRecord(final ODatabaseRecord iDatabase, final ORecordId iRid, String iFetchPlan) {
		return readRecord(getClusterById(iRid.clusterId), iRid, true);
	}

	@Override
	protected ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, final boolean iAtomicLock) {
		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();
		try {
			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());

			if (ppos == null)
				return null;

			return new ORawBuffer(data.readRecord(ppos.dataPosition), ppos.version, ppos.type);
		} catch (IOException e) {
			throw new OStorageException("Error on read record in cluster: " + iClusterSegment.getId(), e);

		} finally {
			lock.releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.readRecord", timer);
		}
	}

	public int updateRecord(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType) {
		final long timer = OProfiler.getInstance().startChrono();

		final OCluster cluster = getClusterById(iRid.clusterId);

		final boolean locked = lock.acquireSharedLock();
		try {
			OPhysicalPosition ppos = cluster.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());
			if (ppos == null)
				return -1;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ iRid
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			data.updateRecord(ppos.dataPosition, iContent);

			return ++(ppos.version);

		} catch (IOException e) {
			throw new OStorageException("Error on update record #" + iRid, e);

		} finally {
			lock.releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.updateRecord", timer);
		}
	}

	public boolean deleteRecord(final ORecordId iRid, final int iVersion) {
		final long timer = OProfiler.getInstance().startChrono();

		final OCluster cluster = getClusterById(iRid.clusterId);

		final boolean locked = lock.acquireSharedLock();
		try {
			final OPhysicalPosition ppos = cluster.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());

			if (ppos == null)
				return false;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ iRid
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			cluster.removePhysicalPosition(iRid.clusterPosition, null);
			data.deleteRecord(ppos.dataPosition);

			return true;

		} catch (IOException e) {
			throw new OStorageException("Error on delete record #" + iRid, e);

		} finally {
			lock.releaseSharedLock(locked);
			OProfiler.getInstance().stopChrono("OStorageMemory.deleteRecord", timer);
		}
	}

	public long count(final int iClusterId) {
		final OCluster cluster = getClusterById(iClusterId);

		final boolean locked = lock.acquireSharedLock();
		try {
			return cluster.getEntries();

		} finally {
			lock.releaseSharedLock(locked);
		}
	}

	public long[] getClusterDataRange(final int iClusterId) {
		final OCluster cluster = getClusterById(iClusterId);

		final boolean locked = lock.acquireSharedLock();
		try {

			return new long[] { cluster.getFirstEntryPosition(), cluster.getLastEntryPosition() };
		} catch (IOException e) {

			throw new OStorageException("Error on getting last entry position in cluster: " + iClusterId, e);
		} finally {

			lock.releaseSharedLock(locked);
		}
	}

	public long count(final int[] iClusterIds) {
		final boolean locked = lock.acquireSharedLock();
		try {

			long tot = 0;
			for (int i = 0; i < iClusterIds.length; ++i)
				tot += clusters.get(iClusterIds[i]).getEntries();
			return tot;

		} finally {

			lock.releaseSharedLock(locked);
		}
	}

	public OCluster getClusterByName(final String iClusterName) {
		final boolean locked = lock.acquireSharedLock();
		try {
			for (int i = 0; i < clusters.size(); ++i)
				if (getClusterById(i).getName().equals(iClusterName))
					return getClusterById(i);
			return null;

		} finally {

			lock.releaseSharedLock(locked);
		}
	}

	public int getClusterIdByName(String iClusterName) {
		final boolean locked = lock.acquireSharedLock();

		iClusterName = iClusterName.toLowerCase();

		try {
			for (int i = 0; i < clusters.size(); ++i)
				if (getClusterById(i).getName().equals(iClusterName))
					return getClusterById(i).getId();
			return -1;

		} finally {

			lock.releaseSharedLock(locked);
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
		final boolean locked = lock.acquireSharedLock();
		try {

			Set<String> result = new HashSet<String>();
			for (int i = 0; i < clusters.size(); ++i)
				result.add(getClusterById(i).getName());
			return result;

		} finally {

			lock.releaseSharedLock(locked);
		}
	}

	public long count(final String iClassName) {
		throw new UnsupportedOperationException("count");
	}

	public List<ORecordSchemaAware<?>> query(final OQuery<?> iQuery, final int iLimit) {
		throw new UnsupportedOperationException("count");
	}

	public ORecordSchemaAware<?> queryFirst(final OQuery<?> iQuery) {
		throw new UnsupportedOperationException("count");
	}

	public void commit(final OTransaction iTx) {
		final boolean locked = lock.acquireSharedLock();

		try {
			final List<OTransactionEntry> allEntries = new ArrayList<OTransactionEntry>();
			final List<OTransactionEntry> tmpEntries = new ArrayList<OTransactionEntry>();

			while (iTx.getEntries().iterator().hasNext()) {
				for (OTransactionEntry txEntry : iTx.getEntries())
					tmpEntries.add(txEntry);

				iTx.clearEntries();

				for (OTransactionEntry txEntry : tmpEntries)
					// COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
					commitEntry(((OTransactionRealAbstract) iTx).getId(), txEntry);

				allEntries.addAll(tmpEntries);
				tmpEntries.clear();
			}

			// UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
			OTransactionAbstract.updateCacheFromEntries(this, iTx, allEntries);

			allEntries.clear();
		} catch (IOException e) {
			rollback(iTx);

		} finally {
			lock.releaseSharedLock(locked);
		}
	}

	public void rollback(final OTransaction iTx) {
	}

	public void synch() {
	}

	public void browse(final int[] iClusterId, final ORecordBrowsingListener iListener, final ORecord<?> iRecord) {
	}

	public boolean exists() {
		return clusters.size() > 0;
	}

	public OCluster getClusterById(int iClusterId) {
		if (iClusterId == ORID.CLUSTER_ID_INVALID)
			// GET THE DEFAULT CLUSTER
			iClusterId = defaultClusterId;

		return clusters.get(iClusterId);
	}

	public Collection<? extends OCluster> getClusters() {
		return Collections.unmodifiableCollection(clusters);
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public long getSize() {
		long size = 0;

		size += data.getSize();

		for (OClusterMemory c : clusters)
			size += c.getSize();

		return size;
	}

	@Override
	public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
		if (ppos.dataSegment > 0)
			return false;

		if (ppos.dataPosition >= data.count())
			return false;

		return true;
	}

	private void commitEntry(final int iTxId, final OTransactionEntry txEntry) throws IOException {

		final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

		final OCluster cluster = txEntry.clusterName != null ? getClusterByName(txEntry.clusterName) : getClusterById(rid.clusterId);
		rid.clusterId = cluster.getId();

		switch (txEntry.status) {
		case OTransactionEntry.LOADED:
			break;

		case OTransactionEntry.CREATED:
			if (rid.isNew()) {
				// CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
				final byte[] stream = txEntry.getRecord().toStream();

				if (rid.isNew()) {
					createRecord(rid, stream, txEntry.getRecord().getRecordType());
				} else {
					txEntry.getRecord().setVersion(
							updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType()));
				}
			}
			break;

		case OTransactionEntry.UPDATED:
			txEntry.getRecord().setVersion(
					updateRecord(rid, txEntry.getRecord().toStream(), txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType()));
			break;

		case OTransactionEntry.DELETED:
			deleteRecord(rid, txEntry.getRecord().getVersion());
			break;
		}
	}

	public OStorageConfigurationSegment getConfigurationSegment() {
		// TODO Auto-generated method stub
		return null;
	}
}
