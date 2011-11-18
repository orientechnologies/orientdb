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

import com.orientechnologies.common.concur.lock.OLockManager.LOCK;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.core.tx.OTxListener;

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

	public OStorageMemory(final String iURL) {
		super(iURL, OEngineMemory.NAME + ":" + iURL, "rw");
		configuration = new OStorageConfiguration(this);
	}

	public void create(final Map<String, Object> iOptions) {
		addUser();

		lock.acquireExclusiveLock();
		try {

			addDataSegment(OStorage.DATA_DEFAULT_NAME);

			// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
			addCluster(OStorage.CLUSTER_INTERNAL_NAME, null);

			// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
			addCluster(OStorage.CLUSTER_INDEX_NAME, null);

			// ADD THE DEFAULT CLUSTER
			defaultClusterId = addCluster(OStorage.CLUSTER_DEFAULT_NAME, null);

			configuration.create();

			status = STATUS.OPEN;

		} catch (OStorageException e) {
			close();
			throw e;

		} catch (IOException e) {
			close();
			throw new OStorageException("Error on creation of storage: " + name, e);

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
		addUser();

		if (status == STATUS.OPEN)
			// ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
			// REUSED
			return;

		lock.acquireExclusiveLock();
		try {

			if (!exists())
				throw new OStorageException("Can't open the storage '" + name + "' because it not exists in path: " + url);

			status = STATUS.OPEN;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void close(final boolean iForce) {
		lock.acquireExclusiveLock();
		try {

			if (!checkForClose(iForce))
				return;

			status = STATUS.CLOSING;

			// CLOSE ALL THE CLUSTERS
			for (OClusterMemory c : clusters)
				if (c != null)
					c.close();
			clusters.clear();

			// CLOSE THE DATA SEGMENT
			data.close();
			level2Cache.shutdown();

			super.close(iForce);

			Orient.instance().unregisterStorage(this);
			status = STATUS.CLOSED;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void delete() {
		close(true);
	}

	public void reload() {
	}

	public int addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iParameters) {
		lock.acquireExclusiveLock();
		try {
			int clusterId = clusters.size();
			for (int i = 0; i < clusters.size(); ++i) {
				if (clusters.get(i) == null) {
					clusterId = i;
					break;
				}
			}

			final OClusterMemory cluster = new OClusterMemory(clusterId, iClusterName.toLowerCase());

			if (clusterId == clusters.size())
				// APPEND IT
				clusters.add(cluster);
			else
				// RECYCLE THE FREE POSITION
				clusters.set(clusterId, cluster);

			return clusterId;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public boolean dropCluster(final int iClusterId) {
		lock.acquireExclusiveLock();
		try {

			final OCluster c = clusters.get(iClusterId);
			if (c != null) {
				c.delete();
				clusters.set(iClusterId, null);
				getLevel2Cache().freeCluster(iClusterId);
			}

		} catch (IOException e) {
		} finally {

			lock.releaseExclusiveLock();
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

	public long createRecord(final ORecordId iRid, final byte[] iContent, final byte iRecordType, ORecordCallback<Long> iCallback) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireSharedLock();
		try {

			final long offset = data.createRecord(iContent);
			final OCluster cluster = getClusterById(iRid.clusterId);

			iRid.clusterPosition = cluster.addPhysicalPosition(0, offset, iRecordType);
			return iRid.clusterPosition;
		} catch (IOException e) {
			throw new OStorageException("Error on create record in cluster: " + iRid.clusterId, e);

		} finally {
			lock.releaseSharedLock();
			OProfiler.getInstance().stopChrono("OStorageMemory.createRecord", timer);
		}
	}

	public ORawBuffer readRecord(final ODatabaseRecord iDatabase, final ORecordId iRid, String iFetchPlan,
			ORecordCallback<ORawBuffer> iCallback) {
		return readRecord(getClusterById(iRid.clusterId), iRid, true);
	}

	@Override
	protected ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, final boolean iAtomicLock) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireSharedLock();
		try {
			lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.SHARED);

			final long lastPos = iClusterSegment.getLastEntryPosition();

			if (iRid.clusterPosition > lastPos)
				throw new ORecordNotFoundException("Record " + iRid + " is out cluster size. Valid range for cluster '"
						+ iClusterSegment.getName() + "' is 0-" + lastPos);

			try {
				final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());

				if (ppos == null)
					return null;

				return new ORawBuffer(data.readRecord(ppos.dataPosition), ppos.version, ppos.type);

			} finally {
				lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.SHARED);
			}
		} catch (IOException e) {
			throw new OStorageException("Error on read record in cluster: " + iClusterSegment.getId(), e);

		} finally {
			lock.releaseSharedLock();
			OProfiler.getInstance().stopChrono("OStorageMemory.readRecord", timer);
		}
	}

	public int updateRecord(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType,
			ORecordCallback<Integer> iCallback) {
		final long timer = OProfiler.getInstance().startChrono();

		final OCluster cluster = getClusterById(iRid.clusterId);

		lock.acquireSharedLock();
		try {

			lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			try {

				final OPhysicalPosition ppos = cluster.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());
				if (ppos == null)
					return -1;

				if (iVersion != -1) {
					if (iVersion > -1) {
						// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
						if (iVersion != ppos.version)
							throw new OConcurrentModificationException(
									"Can't update record "
											+ iRid
											+ " because the version is not the latest one. Probably you are updating an old record or it has been modified by another user (db=v"
											+ ppos.version + " your=v" + iVersion + ")");

						++ppos.version;
					} else
						--ppos.version;
				}

				data.updateRecord(ppos.dataPosition, iContent);

				return ppos.version;

			} finally {
				lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			}
		} catch (IOException e) {
			throw new OStorageException("Error on update record " + iRid, e);

		} finally {
			lock.releaseSharedLock();
			OProfiler.getInstance().stopChrono("OStorageMemory.updateRecord", timer);
		}
	}

	public boolean deleteRecord(final ORecordId iRid, final int iVersion, ORecordCallback<Boolean> iCallback) {
		final long timer = OProfiler.getInstance().startChrono();

		final OCluster cluster = getClusterById(iRid.clusterId);

		lock.acquireSharedLock();
		try {

			lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			try {

				final OPhysicalPosition ppos = cluster.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());

				if (ppos == null)
					return false;

				// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
				if (iVersion > -1 && ppos.version != iVersion)
					throw new OConcurrentModificationException(
							"Can't delete record "
									+ iRid
									+ " because the version is not the latest one. Probably you are deleting an old record or it has been modified by another user (db=v"
									+ ppos.version + " your=v" + iVersion + ")");

				cluster.removePhysicalPosition(iRid.clusterPosition, null);
				data.deleteRecord(ppos.dataPosition);

				return true;

			} finally {
				lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			}

		} catch (IOException e) {
			throw new OStorageException("Error on delete record " + iRid, e);

		} finally {
			lock.releaseSharedLock();
			OProfiler.getInstance().stopChrono("OStorageMemory.deleteRecord", timer);
		}
	}

	public long count(final int iClusterId) {
		final OCluster cluster = getClusterById(iClusterId);

		lock.acquireSharedLock();
		try {

			return cluster.getEntries();

		} finally {
			lock.releaseSharedLock();
		}
	}

	public long[] getClusterDataRange(final int iClusterId) {
		final OCluster cluster = getClusterById(iClusterId);
		lock.acquireSharedLock();
		try {

			return new long[] { cluster.getFirstEntryPosition(), cluster.getLastEntryPosition() };

		} catch (IOException e) {
			throw new OStorageException("Error on getting last entry position in cluster: " + iClusterId, e);
		} finally {
			lock.releaseSharedLock();
		}
	}

	public long count(final int[] iClusterIds) {
		lock.acquireSharedLock();
		try {

			long tot = 0;
			for (int i = 0; i < iClusterIds.length; ++i) {
				final OCluster cluster = clusters.get(iClusterIds[i]);

				if (cluster != null)
					tot += cluster.getEntries();
			}
			return tot;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public OCluster getClusterByName(final String iClusterName) {
		lock.acquireSharedLock();
		try {

			for (int i = 0; i < clusters.size(); ++i) {
				final OCluster cluster = clusters.get(i);

				if (cluster != null && cluster.getName().equalsIgnoreCase(iClusterName))
					return cluster;
			}
			return null;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public int getClusterIdByName(String iClusterName) {
		iClusterName = iClusterName.toLowerCase();

		lock.acquireSharedLock();
		try {

			for (int i = 0; i < clusters.size(); ++i) {
				final OCluster cluster = clusters.get(i);

				if (cluster != null && cluster.getName().equalsIgnoreCase(iClusterName))
					return cluster.getId();
			}
			return -1;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public String getClusterTypeByName(final String iClusterName) {
		return OClusterMemory.TYPE;
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		lock.acquireSharedLock();
		try {

			for (int i = 0; i < clusters.size(); ++i) {
				final OCluster cluster = clusters.get(i);

				if (cluster != null && cluster.getId() == iClusterId)
					return cluster.getName();
			}
			return null;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public Set<String> getClusterNames() {
		lock.acquireSharedLock();
		try {

			Set<String> result = new HashSet<String>();
			for (int i = 0; i < clusters.size(); ++i) {
				final OCluster cluster = clusters.get(i);

				if (cluster != null)
					result.add(cluster.getName());
			}
			return result;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public void commit(final OTransaction iTx) {
		lock.acquireExclusiveLock();
		try {

			final List<OTransactionRecordEntry> tmpEntries = new ArrayList<OTransactionRecordEntry>();

			while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
				for (OTransactionRecordEntry txEntry : iTx.getCurrentRecordEntries())
					tmpEntries.add(txEntry);

				iTx.clearRecordEntries();

				for (OTransactionRecordEntry txEntry : tmpEntries)
					// COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
					commitEntry(iTx, txEntry);

				tmpEntries.clear();
			}

			// UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
			OTransactionAbstract.updateCacheFromEntries(this, iTx, iTx.getAllRecordEntries(), true);
		} catch (IOException e) {
			rollback(iTx);

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void rollback(final OTransaction iTx) {
	}

	public void synch() {
	}

	public void browse(final int[] iClusterId, final ORecordBrowsingListener iListener, final ORecord<?> iRecord) {
	}

	public boolean exists() {
		lock.acquireSharedLock();
		try {

			return !clusters.isEmpty();

		} finally {
			lock.releaseSharedLock();
		}
	}

	public OCluster getClusterById(int iClusterId) {
		lock.acquireSharedLock();
		try {

			if (iClusterId == ORID.CLUSTER_ID_INVALID)
				// GET THE DEFAULT CLUSTER
				iClusterId = defaultClusterId;

			return clusters.get(iClusterId);

		} finally {
			lock.releaseSharedLock();
		}
	}

	public int getClusters() {
		lock.acquireSharedLock();
		try {

			return clusters.size();

		} finally {
			lock.releaseSharedLock();
		}
	}

	public Collection<? extends OCluster> getClusterInstances() {
		lock.acquireSharedLock();
		try {

			return Collections.unmodifiableCollection(clusters);

		} finally {
			lock.releaseSharedLock();
		}
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public long getSize() {
		long size = 0;

		lock.acquireSharedLock();
		try {
			size += data.getSize();

			for (OClusterMemory c : clusters)
				if (c != null)
					size += c.getSize();

		} finally {
			lock.releaseSharedLock();
		}
		return size;
	}

	@Override
	public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
		if (ppos.dataSegment > 0)
			return false;

		lock.acquireSharedLock();
		try {

			if (ppos.dataPosition >= data.count())
				return false;

		} finally {
			lock.releaseSharedLock();
		}
		return true;
	}

	private void commitEntry(final OTransaction iTx, final OTransactionRecordEntry txEntry) throws IOException {

		final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

		final OCluster cluster = txEntry.clusterName != null ? getClusterByName(txEntry.clusterName) : getClusterById(rid.clusterId);
		rid.clusterId = cluster.getId();

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

		switch (txEntry.status) {
		case OTransactionRecordEntry.LOADED:
			break;

		case OTransactionRecordEntry.CREATED:
			if (rid.isNew()) {
				// CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
				byte[] stream = txEntry.getRecord().toStream();

				if (rid.isNew()) {
					if (iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_CREATE, txEntry.getRecord()))
						// RECORD CHANGED: RE-STREAM IT
						stream = txEntry.getRecord().toStream();

					createRecord(rid, stream, txEntry.getRecord().getRecordType(), null);

					iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_CREATE, txEntry.getRecord());

				} else {
					txEntry.getRecord().setVersion(
							updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), null));
				}
			}
			break;

		case OTransactionRecordEntry.UPDATED:
			byte[] stream = txEntry.getRecord().toStream();

			if (iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_UPDATE, txEntry.getRecord()))
				// RECORD CHANGED: RE-STREAM IT
				stream = txEntry.getRecord().toStream();

			txEntry.getRecord().setVersion(
					updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), null));
			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, txEntry.getRecord());
			break;

		case OTransactionRecordEntry.DELETED:
			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, txEntry.getRecord());
			deleteRecord(rid, txEntry.getRecord().getVersion(), null);
			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_DELETE, txEntry.getRecord());
			break;
		}

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
	}

	public OStorageConfigurationSegment getConfigurationSegment() {
		return null;
	}

	public void renameCluster(final String iOldName, final String iNewName) {
		final OClusterMemory cluster = (OClusterMemory) getClusterByName(iOldName);
		if (cluster != null)
			try {
				cluster.set(com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES.NAME, iNewName);
			} catch (IOException e) {
			}
	}
}
