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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageLogicalClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.OQueryExecutor;
import com.orientechnologies.orient.core.query.sql.OSQLAsynchQuery;
import com.orientechnologies.orient.core.query.sql.OSQLAsynchQueryLocalExecutor;
import com.orientechnologies.orient.core.query.sql.OSQLSynchQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.logical.OClusterLogical;
import com.orientechnologies.orient.core.tx.OTransaction;

public class OStorageLocal extends OStorageAbstract {
	// private final OLockManager<String, String> lockManager = new OLockManager<String, String>();
	protected final Map<String, OCluster>	clusterSegmentMap	= new LinkedHashMap<String, OCluster>();
	protected OCluster[]									physicalClusters;
	protected List<OClusterLogical>				logicalClusters		= new ArrayList<OClusterLogical>();
	protected ODataLocal[]								dataSegments;

	private OStorageLocalTxExecuter				txManager;
	private String												storagePath;
	private OStorageVariableParser				variableParser;
	private int														defaultCluster		= -1;

	public OStorageLocal(final String iName, final String iFilePath, final String iMode) throws IOException {
		super(iName, iFilePath, iMode);

		storagePath = OFileUtils.getPath(new File(fileURL).getParent());

		configuration = new OStorageConfiguration(this);
		variableParser = new OStorageVariableParser(storagePath);
		txManager = new OStorageLocalTxExecuter(this, configuration.txSegment);
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		final long timer = OProfiler.getInstance().startChrono();

		addUser();
		cache.addUser();

		final boolean locked = acquireExclusiveLock();

		try {
			if (open)
				// ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS REUSED
				return;

			open = true;

			// OPEN BASIC SEGMENTS
			int pos;
			pos = registerDataSegment(new OStorageDataConfiguration(configuration, OStorage.DEFAULT_SEGMENT));
			dataSegments[pos].open();

			pos = registerClusterSegment(new OStoragePhysicalClusterConfiguration(configuration, OMetadata.CLUSTER_METADATA_NAME));
			physicalClusters[pos].open();

			pos = registerClusterSegment(new OStoragePhysicalClusterConfiguration(configuration, "index"));
			physicalClusters[pos].open();

			defaultCluster = registerClusterSegment(new OStoragePhysicalClusterConfiguration(configuration, OStorage.DEFAULT_SEGMENT));
			physicalClusters[defaultCluster].open();

			configuration.load();

			if (configuration.isEmpty())
				throw new OStorageException("Can't open storage because it not exists. Storage path: " + fileURL);

			// REGISTER DATA SEGMENT
			for (OStorageDataConfiguration data : configuration.dataSegments) {
				pos = registerDataSegment(data);
				if (pos > -1)
					dataSegments[pos].open();
			}

			// REGISTER CLUSTER SEGMENT
			for (OStoragePhysicalClusterConfiguration cluster : configuration.physicalClusters) {
				pos = registerClusterSegment(cluster);

				if (pos > -1) {
					if (cluster.name.equals(OStorage.DEFAULT_SEGMENT))
						defaultCluster = pos;

					physicalClusters[pos].open();
				}
			}

			txManager.open();

		} catch (IOException e) {
			open = false;
			dataSegments = null;
			physicalClusters = null;
			clusterSegmentMap.clear();
			logicalClusters.clear();
			throw new OStorageException("Can't open local storage: " + fileURL + ", with mode=" + mode, e);
		} finally {
			releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.open", timer);
		}
	}

	public void create(final String iStorageMode) {
		final long timer = OProfiler.getInstance().startChrono();

		addUser();
		cache.addUser();

		final boolean locked = acquireExclusiveLock();

		try {
			if (!configuration.isEmpty())
				throw new OStorageException("Can't create new storage " + name + " because it already exists");

			open = true;

			File storageFolder = new File(storagePath);
			if (!storageFolder.exists())
				storageFolder.mkdir();

			addDataSegment(OStorage.DEFAULT_SEGMENT);

			// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
			addCluster(OMetadata.CLUSTER_METADATA_NAME);

			// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
			addCluster("index");

			// ADD THE DEFAULT CLUSTER
			defaultCluster = addCluster(OStorage.DEFAULT_SEGMENT);

			configuration.create();

			txManager.create();
		} catch (IOException e) {
			open = false;
			throw new OStorageException("Error on creation of storage: " + name, e);

		} finally {
			releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.create", timer);
		}
	}

	public boolean exists() {
		return !configuration.isEmpty();
	}

	public void close() {
		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireExclusiveLock();

		if (!open)
			return;

		try {
			if (physicalClusters != null)
				for (OCluster cluster : physicalClusters)
					cluster.close();
			physicalClusters = null;
			logicalClusters.clear();
			clusterSegmentMap.clear();

			if (dataSegments != null)
				for (ODataLocal data : dataSegments)
					data.close();
			dataSegments = null;

			txManager.close();

			cache.removeUser();
			cache.clear();
			configuration = new OStorageConfiguration(this);

			open = false;
		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on closing of the storage '" + name, e, OStorageException.class);

		} finally {
			releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.close", timer);
		}
	}

	public ODataLocal getDataSegment(final int iDataSegmentId) {
		checkOpeness();
		if (iDataSegmentId >= dataSegments.length)
			throw new IllegalArgumentException("Data segment #" + iDataSegmentId + " doesn't exist in current storage");

		final boolean locked = acquireSharedLock();

		try {
			return dataSegments[iDataSegmentId];

		} finally {
			releaseSharedLock(locked);
		}
	}

	/**
	 * Add a new data segment in the default segment directory and with filename equals to the cluster name.
	 */
	public int addDataSegment(final String iDataSegmentName) {
		String segmentFileName = storagePath + "/" + iDataSegmentName;
		return addDataSegment(iDataSegmentName, segmentFileName);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		checkOpeness();

		final boolean locked = acquireExclusiveLock();

		try {
			OStorageDataConfiguration conf = new OStorageDataConfiguration(configuration, iSegmentName);
			configuration.dataSegments.add(conf);

			final int pos = registerDataSegment(conf);

			if (pos == -1)
				throw new OConfigurationException("Can't add segment " + conf.name + " because it's already part of current storage");

			dataSegments[pos].create(-1);
			configuration.update();

			return pos;
		} catch (Throwable e) {
			OLogManager.instance().error(this, "Error on creation of new data segment '" + iSegmentName + "' in: " + iSegmentFileName, e,
					OStorageException.class);
			return -1;

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	public int addLogicalCluster(final OClusterLogical iCluster) {
		int id = getLogicalClusterIndex(logicalClusters.size());

		iCluster.setId(id);

		try {
			registerLogicalCluster(iCluster);

			configuration.logicalClusters.add(new OStorageLogicalClusterConfiguration(iCluster.getName(), id, iCluster.getRID()));
			configuration.update();
			return id;
		} catch (IOException e) {
			throw new ODatabaseException("Error on adding the new logical cluster: " + iCluster.getName(), e);
		}
	}

	/**
	 * Add a new cluster into the storage.
	 */
	public int addClusterSegment(final String iClusterName, String iClusterFileName, final int iStartSize) {
		checkOpeness();

		final boolean locked = acquireExclusiveLock();

		try {
			if (iClusterFileName == null)
				iClusterFileName = storagePath + "/" + iClusterName;

			OStoragePhysicalClusterConfiguration conf = new OStoragePhysicalClusterConfiguration(configuration, iClusterName);
			configuration.physicalClusters.add(conf);

			final int pos = registerClusterSegment(conf);

			if (pos == -1)
				throw new OConfigurationException("Can't add segment " + conf.name + " because already is part of the current storage");

			physicalClusters[pos].create(iStartSize);
			configuration.update();

			return pos;
		} catch (Throwable e) {
			OLogManager.instance().error(this, "Error on creation of new cluster '" + iClusterName + "' in: " + iClusterFileName, e,
					OStorageException.class);
			return -1;

		} finally {

			releaseExclusiveLock(locked);
		}
	}

	public long count(final int[] iClusterIds) {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireSharedLock();

		try {
			long tot = 0;

			// COUNT LOGICAL CLUSTERS IF ANY
			for (int i = 0; i < iClusterIds.length; ++i)
				if (iClusterIds[i] < -1)
					tot += logicalClusters.get(getLogicalClusterIndex(iClusterIds[i])).getElements();

			// COUNT PHYSICAL CLUSTER IF ANY
			for (int i = 0; i < iClusterIds.length; ++i)
				if (iClusterIds[i] > -1)
					tot += physicalClusters[iClusterIds[i]].getElements();
			return tot;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on getting element counts", e);
			return -1;

		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.getClusterElementCounts", timer);
		}
	}

	public long count(final int iClusterId) {
		if (iClusterId < -1)
			// LOGICAL CLUSTER
			return logicalClusters.get(getLogicalClusterIndex(iClusterId)).getElements();

		// COUNT PHYSICAL CLUSTER IF ANY
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireSharedLock();

		try {
			return physicalClusters[iClusterId].getElements();

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on getting element counts", e);
			return -1;

		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.getClusterElementCounts", timer);
		}
	}

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		checkOpeness();
		return createRecord(getCluster(iClusterId), iContent, iRecordType);
	}

	public ORawBuffer readRecord(final int iRequesterId, final int iClusterId, final long iPosition) {
		checkOpeness();
		return readRecord(iRequesterId, getCluster(iClusterId), iPosition, true);
	}

	public int updateRecord(final int iRequesterId, final int iClusterId, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		checkOpeness();
		return updateRecord(iRequesterId, getCluster(iClusterId), iPosition, iContent, iVersion, iRecordType);
	}

	public void deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		checkOpeness();
		deleteRecord(iRequesterId, getCluster(iClusterId), iPosition, iVersion);
	}

	/**
	 * Browse N clusters. The entire operation use a shared lock on the storage and lock the cluster from the external avoiding atomic
	 * lock at every record read.
	 * 
	 * @param iRequesterId
	 *          The requester of the operation. Needed to know who locks
	 * @param iClusterId
	 *          Array of cluster ids
	 * @param iListener
	 *          The listener to call for each record found
	 * @param iRecord
	 *          Record passed to minimize object creation. The record will be re-used ar every read
	 */
	public void browse(final int iRequesterId, final int[] iClusterId, final ORecordBrowsingListener iListener,
			final ORecordInternal<?> iRecord) {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireSharedLock();

		try {
			OCluster cluster;

			for (int clusterId : iClusterId) {
				cluster = getCluster(clusterId);

				browseCluster(iRequesterId, iListener, iRecord, cluster, clusterId);
			}
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on browsing elements of cluster: " + iClusterId, e);

		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.foreach", timer);
		}
	}

	private void browseCluster(final int iRequesterId, final ORecordBrowsingListener iListener, final ORecordInternal<?> iRecord,
			OCluster cluster, int iClusterId) throws IOException {
		ORawBuffer recordBuffer;
		long positionInPhyCluster;

		try {
			cluster.lock();

			OClusterPositionIterator iterator = cluster.absoluteIterator();

			// BROWSE ALL THE RECORDS
			while (iterator.hasNext()) {
				positionInPhyCluster = iterator.next();

				// READ THE RAW RECORD WITHOUT LOCKING THE CLUSTER SINCE IT HAS BEEN MADE HERE
				recordBuffer = readRecord(iRequesterId, cluster, positionInPhyCluster, false);
				if (recordBuffer == null)
					continue;

				iRecord.setIdentity(iClusterId, positionInPhyCluster);
				iRecord.fromStream(recordBuffer.buffer);
				if (!iListener.foreach(iRecord))
					// LISTENER HAS INTERRUPTED THE EXECUTION
					break;
			}
		} finally {

			cluster.unlock();
		}
	}

	public Set<String> getClusterNames() {
		checkOpeness();

		final boolean locked = acquireSharedLock();

		try {

			return clusterSegmentMap.keySet();

		} finally {
			releaseSharedLock(locked);
		}
	}

	public int getClusterIdByName(final String iClusterName) {
		checkOpeness();

		if (iClusterName == null)
			throw new IllegalArgumentException("Cluster name is null");

		if (Character.isDigit(iClusterName.charAt(0)))
			return Integer.parseInt(iClusterName);

		// SEARCH IT BETWEEN PHYSICAL CLUSTERS
		OCluster segment;

		final boolean locked = acquireSharedLock();

		try {
			segment = clusterSegmentMap.get(iClusterName.toLowerCase());

		} finally {
			releaseSharedLock(locked);
		}

		if (segment != null)
			return segment.getId();

		return -1;
	}

	public void commit(final int iRequesterId, final OTransaction<?> iTx) {
		final boolean locked = acquireSharedLock();

		try {
			txManager.commitAllPendingRecords(iRequesterId, iTx);

		} catch (IOException e) {
			rollback(iRequesterId, iTx);

		} finally {
			releaseSharedLock(locked);
		}

	}

	public void rollback(final int iRequesterId, final OTransaction<?> iTx) {
	}

	public void synch() {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireExclusiveLock();

		try {
			if (physicalClusters != null)
				for (OCluster cluster : physicalClusters)
					cluster.synch();

			if (dataSegments != null)
				for (ODataLocal data : dataSegments)
					data.synch();

		} finally {
			releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.synch", timer);
		}
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		checkOpeness();

		if (iClusterId < -1) {
			// LOGICAL CLUSTER
			int index = getLogicalClusterIndex(iClusterId);
			return logicalClusters.get(index).getName();
		}

		// PHYSICAL CLUSTER
		for (OCluster cluster : physicalClusters)
			if (cluster.getId() == iClusterId)
				return cluster.getName();

		return null;
	}

	@Override
	public OStorageConfiguration getConfiguration() {
		return configuration;
	}

	public OCluster getCluster(int iClusterId) {
		if (iClusterId == ORID.CLUSTER_ID_INVALID)
			// GET THE DEFAULT CLUSTER
			iClusterId = defaultCluster;

		checkClusterSegmentIndexRange(iClusterId);

		return iClusterId > -1 ? physicalClusters[iClusterId] : logicalClusters.get(getLogicalClusterIndex(iClusterId));
	}

	public OCluster getClusterByName(final String iClusterName) {
		final boolean locked = acquireSharedLock();

		try {
			final OCluster cluster = clusterSegmentMap.get(iClusterName.toLowerCase());

			if (cluster == null)
				throw new IllegalArgumentException("Cluster " + iClusterName + " not exists");
			return cluster;

		} finally {

			releaseSharedLock(locked);
		}
	}

	@SuppressWarnings("unchecked")
	public ODictionary<?> createDictionary(ODatabaseRecord<?> iDatabase) throws Exception {
		return new ODictionaryLocal(iDatabase);
	}

	public OQueryExecutor getQueryExecutor(OQuery<?> iQuery) {
		if (iQuery instanceof OSQLAsynchQuery<?>)
			return OSQLAsynchQueryLocalExecutor.INSTANCE;

		else if (iQuery instanceof OSQLSynchQuery<?>)
			return OSQLAsynchQueryLocalExecutor.INSTANCE;

		throw new OConfigurationException("Query executor not configured for query type: " + iQuery.getClass());
	}

	public String getStoragePath() {
		return storagePath;
	}

	public String getMode() {
		return mode;
	}

	public OStorageVariableParser getVariableParser() {
		return variableParser;
	}

	protected int registerDataSegment(OStorageDataConfiguration iConfig) throws IOException {
		checkOpeness();

		int pos = 0;

		// CHECK FOR DUPLICATION OF NAMES
		if (dataSegments != null) {
			for (ODataLocal data : dataSegments)
				if (data.getName().equals(iConfig.name))
					return -1;
			pos = dataSegments.length;
		}

		// CREATE AND ADD THE NEW REF SEGMENT
		ODataLocal segment = new ODataLocal(this, iConfig, pos);

		if (dataSegments != null) {
			dataSegments = Arrays.copyOf(dataSegments, dataSegments.length + 1);
		} else
			dataSegments = new ODataLocal[1];

		dataSegments[pos] = segment;

		return pos;
	}

	public int registerLogicalCluster(OClusterLogical iClusterLogical) {
		final String clusterName = iClusterLogical.getName().toLowerCase();
		if (clusterSegmentMap.containsKey(clusterName))
			return -1;

		clusterSegmentMap.put(clusterName, iClusterLogical);

		logicalClusters.add(iClusterLogical);
		return getLogicalClusterIndex(logicalClusters.size());
	}

	protected int registerClusterSegment(OStoragePhysicalClusterConfiguration iConfig) throws IOException {
		checkOpeness();

		int pos = 0;

		// CHECK FOR DUPLICATION OF NAMES
		if (physicalClusters != null) {
			if (clusterSegmentMap.containsKey(iConfig.name.toLowerCase()))
				return -1;

			pos = physicalClusters.length;
		}

		// CREATE AND ADD THE NEW REF SEGMENT
		final OCluster segment = new OClusterLocal(this, iConfig, pos, iConfig.name);
		clusterSegmentMap.put(iConfig.name.toLowerCase(), segment);

		if (physicalClusters != null) {
			physicalClusters = Arrays.copyOf(physicalClusters, physicalClusters.length + 1);
		} else
			physicalClusters = new OCluster[1];

		physicalClusters[pos] = segment;

		return pos;
	}

	private void checkClusterSegmentIndexRange(final int iClusterId) {
		if (iClusterId < -1 && getLogicalClusterIndex(iClusterId) > logicalClusters.size() - 1)
			throw new IllegalArgumentException("Cluster segment #" + iClusterId + " not exists");

		if (iClusterId > -1 && iClusterId > physicalClusters.length - 1)
			throw new IllegalArgumentException("Cluster segment #" + iClusterId + " not exists");
	}

	protected int getDataSegmentForRecord(final OCluster iCluster, final byte[] iContent) {
		// TODO: CREATE POLICY & STRATEGY TO ASSIGN THE BEST-MULTIPLE DATA SEGMENT
		return 0;
	}

	protected long createRecord(final OCluster iClusterSegment, final byte[] iContent, final byte iRecordType) {
		checkOpeness();

		if (iContent == null)
			throw new IllegalArgumentException("Record " + iContent + " is null");

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = acquireSharedLock();

		try {
			final int dataSegment = getDataSegmentForRecord(iClusterSegment, iContent);
			ODataLocal data = getDataSegment(dataSegment);

			final long clusterPosition = iClusterSegment.addPhysicalPosition(-1, -1, iRecordType);

			final long dataOffset = data.addRecord(iClusterSegment.getId(), clusterPosition, iContent);

			// UPDATE THE POSITION IN CLUSTER WITH THE POSITION OF RECORD IN DATA
			iClusterSegment.setPhysicalPosition(clusterPosition, dataSegment, dataOffset, iRecordType);

			return clusterPosition;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on creating record in cluster: " + iClusterSegment, e);
			return -1;
		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.createRecord", timer);
		}
	}

	protected ORawBuffer readRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition, boolean iAtomicLock) {
		if (iPosition < 0)
			throw new IllegalArgumentException("Can't read the record because the position #" + iPosition + " is invalid");

		// NOT FOUND: SEARCH IT IN THE STORAGE
		final long timer = OProfiler.getInstance().startChrono();

		// GET LOCK ONLY IF IT'S IN ATOMIC-MODE (SEE THE PARAMETER iAtomicLock) USUALLY BROWSING OPERATIONS (QUERY) AVOID ATOMIC LOCKING
		// TO IMPROVE PERFORMANCES BY LOCKING THE ENTIRE CLUSTER FROM THE OUTSIDE.
		final boolean locked = iAtomicLock ? acquireSharedLock() : false;

		try {
			// lockManager.acquireLock(iRequesterId, recId, LOCK.SHARED, timeout);

			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());
			if (ppos == null || !checkForRecordValidity(ppos))
				// DELETED
				return null;

			final ODataLocal data = getDataSegment(ppos.dataSegment);
			return new ORawBuffer(data.getRecord(ppos.dataPosition), ppos.version, ppos.type);

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on reading record #" + iPosition + " in cluster: " + iClusterSegment, e);
			return null;

		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.readRecord", timer);
		}
	}

	protected int updateRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		final long timer = OProfiler.getInstance().startChrono();

		final String recId = ORecordId.generateString(iClusterSegment.getId(), iPosition);

		final boolean locked = acquireSharedLock();

		try {
			// lockManager.acquireLock(iRequesterId, recId, LOCK.EXCLUSIVE, timeout);

			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());
			if (!checkForRecordValidity(ppos))
				// DELETED
				return -1;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ recId
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			iClusterSegment.updateVersion(iPosition, ++ppos.version);

			final long newDataSegmentOffset = getDataSegment(ppos.dataSegment).setRecord(ppos.dataPosition, iClusterSegment.getId(),
					iPosition, iContent);

			if (newDataSegmentOffset != ppos.dataPosition)
				// UPDATE DATA SEGMENT OFFSET WITH THE NEW PHYSICAL POSITION
				iClusterSegment.setPhysicalPosition(iPosition, ppos.dataSegment, newDataSegmentOffset, iRecordType);

			return ppos.version;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on updating record #" + iPosition + " in cluster: " + iClusterSegment, e);

		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.updateRecord", timer);
		}

		return -1;
	}

	protected void deleteRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition, final int iVersion) {
		final long timer = OProfiler.getInstance().startChrono();

		final String recId = ORecordId.generateString(iClusterSegment.getId(), iPosition);

		final boolean locked = acquireSharedLock();

		try {
			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());

			if (!checkForRecordValidity(ppos))
				// ALREADY DELETED
				return;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't delete the record #"
								+ recId
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			iClusterSegment.removePhysicalPosition(iPosition, ppos);

			getDataSegment(ppos.dataSegment).deleteRecord(ppos.dataPosition);

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on deleting record #" + iPosition + " in cluster: " + iClusterSegment, e);

		} finally {
			releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.deleteRecord", timer);
		}
	}

	private void checkOpeness() {
		if (!open)
			throw new OStorageException("Storage " + name + " is not opened.");
	}

	public Set<OCluster> getClusters() {
		Set<OCluster> result = new HashSet<OCluster>();

		// ADD PHYSICAL CLUSTERS
		for (OCluster c : physicalClusters)
			result.add(c);

		// ADD LOGICAL CLUSTERS
		result.addAll(logicalClusters);

		return result;
	}
}
