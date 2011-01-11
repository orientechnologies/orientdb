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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageLogicalClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageMemoryClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.fs.OMMapManager;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;
import com.orientechnologies.orient.core.tx.OTransaction;

public class OStorageLocal extends OStorageAbstract {
	private static final int						DELETE_MAX_RETRIES				= 20;
	private static final int						DELETE_WAIT_TIME					= 200;
	public static final String[]				TYPES											= { OClusterLocal.TYPE, OClusterLogical.TYPE };

	// private final OLockManager<String, String> lockManager = new
	// OLockManager<String, String>();
	private final Map<String, OCluster>	clusterMap								= new LinkedHashMap<String, OCluster>();
	private OCluster[]									clusters									= new OCluster[0];
	private ODataLocal[]								dataSegments							= new ODataLocal[0];

	private OStorageLocalTxExecuter			txManager;
	private String											storagePath;
	private OStorageVariableParser			variableParser;
	private int													defaultClusterId					= -1;

	public static final int							DEFAULT_FIXED_CONFIG_SIZE	= 200000;
	private int													fixedSize									= DEFAULT_FIXED_CONFIG_SIZE;

	private static String[]							ALL_FILE_EXTENSIONS				= { ".och", ".ocl", ".oda", ".odh", ".otx" };

	public OStorageLocal(final String iName, final String iFilePath, final String iMode) throws IOException {
		super(iName, iFilePath, iMode);

		File f = new File(url);

		if (f.exists() || !exists(f.getParent())) {
			// ALREADY EXISTS OR NOT LEGACY
			storagePath = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getPath()));
		} else {
			// LEGACY DB
			storagePath = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getParent()));
		}

		configuration = new OStorageConfiguration(this);
		variableParser = new OStorageVariableParser(storagePath);
		txManager = new OStorageLocalTxExecuter(this, configuration.txSegment);
	}

	public synchronized void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		final long timer = OProfiler.getInstance().startChrono();

		addUser();
		cache.addUser();

		final boolean locked = lock.acquireExclusiveLock();

		try {
			if (open)
				// ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
				// REUSED
				return;

			if (!exists())
				throw new OStorageException("Can't open the storage '" + name + "' because it not exists in path: " + url);

			open = true;

			// OPEN BASIC SEGMENTS
			int pos;
			pos = registerDataSegment(new OStorageDataConfiguration(configuration, OStorage.DATA_DEFAULT_NAME));
			dataSegments[pos].open();

			pos = createClusterFromConfig(new OStoragePhysicalClusterConfiguration(configuration, OStorage.CLUSTER_INTERNAL_NAME,
					clusters.length));
			clusters[pos].open();

			pos = createClusterFromConfig(new OStoragePhysicalClusterConfiguration(configuration, OStorage.CLUSTER_INDEX_NAME,
					clusters.length));
			clusters[pos].open();

			defaultClusterId = createClusterFromConfig(new OStoragePhysicalClusterConfiguration(configuration,
					OStorage.CLUSTER_DEFAULT_NAME, clusters.length));
			clusters[defaultClusterId].open();

			configuration.load();

			if (configuration.isEmpty())
				throw new OStorageException("Can't open storage because it not exists. Storage path: " + url);

			// REGISTER DATA SEGMENT
			OStorageDataConfiguration dataConfig;
			for (int i = 0; i < configuration.dataSegments.size(); ++i) {
				dataConfig = configuration.dataSegments.get(i);

				pos = registerDataSegment(dataConfig);
				if (pos == -1) {
					// CLOSE AND REOPEN TO BE SURE ALL THE FILE SEGMENTS ARE
					// OPENED
					dataSegments[i].close();
					dataSegments[i] = new ODataLocal(this, dataConfig, pos);
					dataSegments[i].open();
				} else
					dataSegments[pos].open();
			}

			// REGISTER CLUSTER
			OStorageClusterConfiguration clusterConfig;
			for (int i = 0; i < configuration.clusters.size(); ++i) {
				clusterConfig = configuration.clusters.get(i);

				if (clusterConfig != null) {
					pos = createClusterFromConfig(clusterConfig);

					if (pos == -1) {
						// CLOSE AND REOPEN TO BE SURE ALL THE FILE SEGMENTS ARE
						// OPENED
						clusters[i].close();
						clusters[i] = new OClusterLocal(this, (OStoragePhysicalClusterConfiguration) clusterConfig);
						clusterMap.put(clusters[i].getName(), clusters[i]);
						clusters[i].open();
					} else {
						if (clusterConfig.getName().equals(OStorage.CLUSTER_DEFAULT_NAME))
							defaultClusterId = pos;

						clusters[pos].open();
					}
				}
			}

			loadVersion();

			txManager.open();

		} catch (IOException e) {
			open = false;
			dataSegments = new ODataLocal[0];
			clusters = new OCluster[0];
			clusterMap.clear();
			throw new OStorageException("Can't open local storage: " + url + ", with mode=" + mode, e);
		} finally {
			lock.releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.open", timer);
		}
	}

	public void create() {
		final long timer = OProfiler.getInstance().startChrono();

		addUser();
		cache.addUser();

		final boolean locked = lock.acquireExclusiveLock();

		try {
			if (open)
				throw new OStorageException("Can't create new storage " + name + " because it isn't closed");

			File storageFolder = new File(storagePath);
			if (!storageFolder.exists())
				storageFolder.mkdir();

			if (exists())
				throw new OStorageException("Can't create new storage " + name + " because it already exists");

			open = true;

			addDataSegment(OStorage.DATA_DEFAULT_NAME);

			// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
			addCluster(OStorage.CLUSTER_INTERNAL_NAME, OStorage.CLUSTER_TYPE.PHYSICAL);

			// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
			// INDEXING
			addCluster(OStorage.CLUSTER_INDEX_NAME, OStorage.CLUSTER_TYPE.PHYSICAL);

			// ADD THE DEFAULT CLUSTER
			defaultClusterId = addCluster(OStorage.CLUSTER_DEFAULT_NAME, OStorage.CLUSTER_TYPE.PHYSICAL);

			configuration.create(fixedSize);

			txManager.create();
		} catch (OStorageException e) {
			close();
			throw e;
		} catch (IOException e) {
			close();
			throw new OStorageException("Error on creation of storage: " + name, e);

		} finally {
			lock.releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.create", timer);
		}
	}

	public boolean exists() {
		return exists(storagePath);
	}

	private boolean exists(String path) {
		return new File(path + "/" + OStorage.DATA_DEFAULT_NAME + ".0" + ODataLocal.DEF_EXTENSION).exists();
	}

	public void close() {
		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireExclusiveLock();

		if (!open)
			return;

		try {
			saveVersion();

			for (OCluster cluster : clusters)
				if (cluster != null)
					cluster.close();
			clusters = new OCluster[0];
			clusterMap.clear();

			for (ODataLocal data : dataSegments)
				data.close();
			dataSegments = new ODataLocal[0];

			txManager.close();

			cache.removeUser();
			cache.clear();
			configuration = new OStorageConfiguration(this);

			OMMapManager.flush();

			Orient.instance().unregisterStorage(this);

			open = false;
		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on closing of the storage '" + name, e, OStorageException.class);

		} finally {
			lock.releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.close", timer);
		}
	}

	/**
	 * Deletes physically all the database files (that ends for ".och", ".ocl", ".oda", ".odh", ".otx"). Tries also to delete the
	 * container folder if the directory is empty. If files are locked, retry up to 10 times before to raise an exception.
	 */
	public void delete() {
		if (!isClosed())
			// CLOSE THE DATABASE BY REMOVING THE CURRENT USER
			if (removeUser() > 0)
				throw new OStorageException("Can't delete a storage open");

		final long timer = OProfiler.getInstance().startChrono();

		// GET REAL DIRECTORY
		File dbDir = new File(OSystemVariableResolver.resolveSystemVariables(url));
		if (!dbDir.exists() || !dbDir.isDirectory())
			dbDir = dbDir.getParentFile();

		final boolean locked = lock.acquireExclusiveLock();

		try {
			// RETRIES
			for (int i = 0; i < DELETE_MAX_RETRIES; ++i) {
				if (dbDir.exists() && dbDir.isDirectory()) {
					int notDeletedFiles = 0;

					// TRY TO DELETE ALL THE FILES
					for (File f : dbDir.listFiles()) {
						// DELETE ONLY THE SUPPORTED FILES
						for (String ext : ALL_FILE_EXTENSIONS)
							if (f.getPath().endsWith(ext)) {
								if (!f.delete())
									notDeletedFiles++;
								break;
							}
					}

					if (notDeletedFiles == 0) {
						// TRY TO DELETE ALSO THE DIRECTORY IF IT'S EMPTY
						dbDir.delete();
						return;
					}
				} else
					return;

				try {
					OLogManager.instance().debug(this,
							"Can't delete database files because are locked by the OrientDB process yet: waiting a %d ms and retry %d/%d...",
							DELETE_WAIT_TIME, i, DELETE_MAX_RETRIES);

					// FORCE FINALIZATION TO COLLECT ALL THE PENDING BUFFERS
					System.gc();

					Thread.sleep(DELETE_WAIT_TIME);
				} catch (InterruptedException e) {
				}
			}

			throw new OStorageException("Can't delete database '" + name + "' located in: " + dbDir + ". Database files seems locked");

		} finally {
			lock.releaseExclusiveLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.delete", timer);
		}
	}

	public ODataLocal getDataSegment(final int iDataSegmentId) {
		checkOpeness();
		if (iDataSegmentId >= dataSegments.length)
			throw new IllegalArgumentException("Data segment #" + iDataSegmentId + " doesn't exist in current storage");

		final boolean locked = lock.acquireSharedLock();

		try {
			return dataSegments[iDataSegmentId];

		} finally {
			lock.releaseSharedLock(locked);
		}
	}

	/**
	 * Add a new data segment in the default segment directory and with filename equals to the cluster name.
	 */
	public int addDataSegment(final String iDataSegmentName) {
		String segmentFileName = storagePath + "/" + iDataSegmentName;
		return addDataSegment(iDataSegmentName, segmentFileName);
	}

	public int addDataSegment(String iSegmentName, final String iSegmentFileName) {
		checkOpeness();

		iSegmentName = iSegmentName.toLowerCase();

		final boolean locked = lock.acquireExclusiveLock();

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
			lock.releaseExclusiveLock(locked);
		}
	}

	/**
	 * Add a new cluster into the storage. Type can be: "physical" or "logical".
	 */
	public int addCluster(String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iParameters) {
		checkOpeness();

		final boolean locked = lock.acquireExclusiveLock();

		try {
			iClusterName = iClusterName.toLowerCase();

			switch (iClusterType) {
			case PHYSICAL: {
				// GET PARAMETERS
				final String clusterFileName = (String) (iParameters.length < 1 ? storagePath + "/" + iClusterName : iParameters[0]);
				final int startSize = (iParameters.length < 2 ? -1 : (Integer) iParameters[1]);

				return addPhysicalCluster(iClusterName, clusterFileName, startSize);
			}
			case LOGICAL: {
				// GET PARAMETERS
				final int physicalClusterId = (iParameters.length < 1 ? getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME)
						: (Integer) iParameters[0]);

				return addLogicalCluster(iClusterName, physicalClusterId);
			}

			case MEMORY:
				return addMemoryCluster(iClusterName);

			default:
				OLogManager.instance().exception(
						"Cluster type '" + iClusterType + "' is not supported. Supported types are: " + Arrays.toString(TYPES), null,
						OStorageException.class);
			}

		} catch (Exception e) {
			OLogManager.instance().exception("Error in creation of new cluster '" + iClusterName + "' of type: " + iClusterType, e,
					OStorageException.class);

		} finally {

			lock.releaseExclusiveLock(locked);
		}
		return -1;
	}

	public ODataLocal[] getDataSegments() {
		return dataSegments;
	}

	public OStorageLocalTxExecuter getTxManager() {
		return txManager;
	}

	public boolean removeCluster(final int iClusterId) {
		final boolean locked = lock.acquireExclusiveLock();

		try {
			if (iClusterId < 0 || iClusterId >= clusters.length)
				throw new IllegalArgumentException("Cluster id '" + iClusterId + "' is out of range of configured clusters (0-"
						+ (clusters.length - 1) + ")");

			final OCluster cluster = clusters[iClusterId];
			if (cluster == null)
				return false;

			cluster.delete();

			clusterMap.remove(cluster.getName());
			clusters[iClusterId] = null;

			// UPDATE CONFIGURATION
			configuration.clusters.set(iClusterId, null);
			configuration.update();

			return true;
		} catch (Exception e) {
			OLogManager.instance().exception("Error while removing cluster '" + iClusterId + "'", e, OStorageException.class);

		} finally {
			lock.releaseExclusiveLock(locked);
		}

		return false;
	}

	public long count(final int[] iClusterIds) {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			long tot = 0;

			OCluster c;
			for (int i = 0; i < iClusterIds.length; ++i) {
				if (iClusterIds[i] >= clusters.length)
					throw new OConfigurationException("Cluster id " + iClusterIds[i] + "was not found");

				c = clusters[iClusterIds[i]];
				if (c != null)
					tot += c.getEntries();
			}

			return tot;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on getting element counts", e);
			return -1;

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.getClusterElementCounts", timer);
		}
	}

	public long[] getClusterDataRange(final int iClusterId) {
		if (iClusterId == -1)
			throw new OStorageException("Cluster Id is invalid: " + iClusterId);

		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			return new long[] { clusters[iClusterId].getFirstEntryPosition(), clusters[iClusterId].getLastEntryPosition() };

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on getting last entry position", e);
			return null;

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.getClusterLastEntryPosition", timer);
		}
	}

	public long count(final int iClusterId) {
		if (iClusterId == -1)
			throw new OStorageException("Cluster Id is invalid: " + iClusterId);

		// COUNT PHYSICAL CLUSTER IF ANY
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			return clusters[iClusterId].getEntries();

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on getting element counts", e);
			return -1;

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.getClusterElementCounts", timer);
		}
	}

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		checkOpeness();

		final long clusterPosition = createRecord(getClusterById(iClusterId), iContent, iRecordType);
		return clusterPosition;
	}

	public ORawBuffer readRecord(final ODatabaseRecord iDatabase, final int iRequesterId, final int iClusterId, final long iPosition,
			final String iFetchPlan) {
		checkOpeness();
		return readRecord(iRequesterId, getClusterById(iClusterId), iPosition, true);
	}

	public int updateRecord(final int iRequesterId, final int iClusterId, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		checkOpeness();

		final int recordVersion = updateRecord(iRequesterId, getClusterById(iClusterId), iPosition, iContent, iVersion, iRecordType);
		return recordVersion;
	}

	public boolean deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		checkOpeness();
		final boolean succeed = deleteRecord(iRequesterId, getClusterById(iClusterId), iPosition, iVersion);
		return succeed;
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
	 * @param ioRecord
	 */
	public void browse(final int iRequesterId, final int[] iClusterId, final ORecordId iBeginRange, final ORecordId iEndRange,
			final ORecordBrowsingListener iListener, ORecordInternal<?> ioRecord, final boolean iLockEntireCluster) {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			OCluster cluster;

			long beginClusterPosition;
			long endClusterPosition;

			for (int clusterId : iClusterId) {
				if (iBeginRange != null)
					if (clusterId < iBeginRange.getClusterId())
						// JUMP THIS
						continue;

				if (iEndRange != null)
					if (clusterId > iEndRange.getClusterId())
						// STOP
						break;

				cluster = getClusterById(clusterId);

				beginClusterPosition = iBeginRange != null && iBeginRange.getClusterId() == clusterId ? iBeginRange.getClusterPosition()
						: 0;
				endClusterPosition = iEndRange != null && iEndRange.getClusterId() == clusterId ? iEndRange.getClusterPosition() : -1;

				ioRecord = browseCluster(iRequesterId, iListener, ioRecord, cluster, beginClusterPosition, endClusterPosition,
						iLockEntireCluster);
			}
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on browsing elements of cluster: " + iClusterId, e);

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.foreach", timer);
		}
	}

	private ORecordInternal<?> browseCluster(final int iRequesterId, final ORecordBrowsingListener iListener,
			ORecordInternal<?> ioRecord, final OCluster cluster, final long iBeginRange, final long iEndRange,
			final boolean iLockEntireCluster) throws IOException {
		ORawBuffer recordBuffer;
		long positionInPhyCluster;

		try {
			if (iLockEntireCluster)
				// LOCK THE ENTIRE CLUSTER AVOIDING TO LOCK EVERY SINGLE RECORD
				cluster.lock();

			OClusterPositionIterator iterator = cluster.absoluteIterator(iBeginRange, iEndRange);

			// BROWSE ALL THE RECORDS
			while (iterator.hasNext()) {
				positionInPhyCluster = iterator.next();

				if (positionInPhyCluster == -1)
					// NOT VALID POSITION (IT HAS BEEN DELETED)
					continue;

				// TRY IN THE CACHE
				recordBuffer = cache.getRecord(ORecordId.generateString(cluster.getId(), positionInPhyCluster));
				if (recordBuffer == null) {
					// READ THE RAW RECORD. IF iLockEntireCluster THEN THE READ WILL
					// BE NOT-LOCKING, OTHERWISE YES
					recordBuffer = readRecord(iRequesterId, cluster, positionInPhyCluster, !iLockEntireCluster);
					if (recordBuffer == null)
						continue;
				}

				if (recordBuffer.recordType != ODocument.RECORD_TYPE && recordBuffer.recordType != ORecordColumn.RECORD_TYPE)
					// WRONG RECORD TYPE: JUMP IT
					continue;

				if (ioRecord == null)
					// RECORD NULL OR DIFFERENT IN TYPE: CREATE A NEW ONE
					ioRecord = ORecordFactory.newInstance(recordBuffer.recordType);
				else if (ioRecord.getRecordType() != recordBuffer.recordType) {
					// RECORD NULL OR DIFFERENT IN TYPE: CREATE A NEW ONE
					final ORecordInternal<?> newRecord = ORecordFactory.newInstance(recordBuffer.recordType);
					newRecord.setDatabase(ioRecord.getDatabase());
					ioRecord = newRecord;
				} else
					// RESET CURRENT RECORD
					ioRecord.reset();

				ioRecord.setVersion(recordBuffer.version);
				ioRecord.setIdentity(cluster.getId(), positionInPhyCluster);
				ioRecord.fromStream(recordBuffer.buffer);
				if (!iListener.foreach(ioRecord))
					// LISTENER HAS INTERRUPTED THE EXECUTION
					break;
			}
		} finally {

			if (iLockEntireCluster)
				// UNLOCK THE ENTIRE CLUSTER
				cluster.unlock();
		}

		return ioRecord;
	}

	public Set<String> getClusterNames() {
		checkOpeness();

		final boolean locked = lock.acquireSharedLock();

		try {

			return clusterMap.keySet();

		} finally {
			lock.releaseSharedLock(locked);
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

		final boolean locked = lock.acquireSharedLock();

		try {
			segment = clusterMap.get(iClusterName.toLowerCase());

		} finally {
			lock.releaseSharedLock(locked);
		}

		if (segment != null)
			return segment.getId();

		return -1;
	}

	public String getClusterTypeByName(final String iClusterName) {
		checkOpeness();

		if (iClusterName == null)
			throw new IllegalArgumentException("Cluster name is null");

		// SEARCH IT BETWEEN PHYSICAL CLUSTERS
		OCluster segment;

		final boolean locked = lock.acquireSharedLock();

		try {
			segment = clusterMap.get(iClusterName.toLowerCase());

		} finally {
			lock.releaseSharedLock(locked);
		}

		if (segment != null)
			return segment.getType();

		return null;
	}

	public void commit(final int iRequesterId, final OTransaction iTx) {
		final boolean locked = lock.acquireSharedLock();

		try {
			txManager.commitAllPendingRecords(iRequesterId, iTx);

			incrementVersion();
			synch();

		} catch (IOException e) {
			rollback(iRequesterId, iTx);

		} finally {
			lock.releaseSharedLock(locked);
		}
	}

	public void rollback(final int iRequesterId, final OTransaction iTx) {
	}

	public void synch() {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			saveVersion();

			for (OCluster cluster : clusters)
				cluster.synch();

			for (ODataLocal data : dataSegments)
				data.synch();

		} catch (IOException e) {
			throw new OStorageException("Error on synch", e);
		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.synch", timer);
		}
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		checkOpeness();

		for (OCluster cluster : clusters)
			if (cluster != null && cluster.getId() == iClusterId)
				return cluster.getName();

		return null;
	}

	@Override
	public OStorageConfiguration getConfiguration() {
		return configuration;
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public OCluster getClusterById(int iClusterId) {
		if (iClusterId == ORID.CLUSTER_ID_INVALID)
			// GET THE DEFAULT CLUSTER
			iClusterId = defaultClusterId;

		checkClusterSegmentIndexRange(iClusterId);

		return clusters[iClusterId];
	}

	public OCluster getClusterByName(final String iClusterName) {
		final boolean locked = lock.acquireSharedLock();

		try {
			final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());

			if (cluster == null)
				throw new IllegalArgumentException("Cluster " + iClusterName + " not exists");
			return cluster;

		} finally {

			lock.releaseSharedLock(locked);
		}
	}

	public ODictionary<?> createDictionary(ODatabaseRecord iDatabase) throws Exception {
		return new ODictionaryLocal<Object>(iDatabase);
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

	public Set<OCluster> getClusters() {
		Set<OCluster> result = new HashSet<OCluster>();

		// ADD ALL THE CLUSTERS
		for (OCluster c : clusters)
			result.add(c);

		return result;
	}

	/**
	 * Execute the command request and return the result back.
	 */
	public Object command(final OCommandRequestText iCommand) {
		final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
		executor.setProgressListener(iCommand.getProgressListener());
		executor.parse(iCommand);
		try {
			return executor.execute(iCommand.getParameters());
		} catch (OCommandExecutionException e) {
			// PASS THROUGHT
			throw e;
		} catch (Exception e) {
			throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);
		}
	}

	protected int registerDataSegment(final OStorageDataConfiguration iConfig) throws IOException {
		checkOpeness();

		int pos = 0;

		// CHECK FOR DUPLICATION OF NAMES
		for (ODataLocal data : dataSegments)
			if (data.getName().equals(iConfig.name)) {
				// OVERWRITE CONFIG
				data.config = iConfig;
				return -1;
			}
		pos = dataSegments.length;

		// CREATE AND ADD THE NEW REF SEGMENT
		ODataLocal segment = new ODataLocal(this, iConfig, pos);

		dataSegments = OArrays.copyOf(dataSegments, dataSegments.length + 1);
		dataSegments[pos] = segment;

		return pos;
	}

	/**
	 * Create the cluster by reading the configuration received as argument and register it assigning it the higher serial id.
	 * 
	 * @param iConfig
	 *          A OStorageClusterConfiguration implementation, namely physical or logical
	 * @return The id (physical position into the array) of the new cluster just created. First is 0.
	 * @throws IOException
	 * @throws IOException
	 */
	private int createClusterFromConfig(final OStorageClusterConfiguration iConfig) throws IOException {
		OCluster cluster = clusterMap.get(iConfig.getName());

		if (cluster != null) {
			if (cluster instanceof OClusterLocal)
				// ALREADY CONFIGURED, JUST OVERWRITE CONFIG
				((OClusterLocal) cluster).config = (OStorageSegmentConfiguration) iConfig;
			return -1;
		}

		if (iConfig instanceof OStoragePhysicalClusterConfiguration)
			cluster = new OClusterLocal(this, (OStoragePhysicalClusterConfiguration) iConfig);
		else
			cluster = new OClusterLogical(this, (OStorageLogicalClusterConfiguration) iConfig);

		return registerCluster(cluster);
	}

	/**
	 * Register the cluster internally.
	 * 
	 * @param iCluster
	 *          OCluster implementation
	 * @return The id (physical position into the array) of the new cluster just created. First is 0.
	 * @throws IOException
	 */
	private int registerCluster(final OCluster iCluster) throws IOException {
		// CHECK FOR DUPLICATION OF NAMES
		if (clusterMap.containsKey(iCluster.getName()))
			throw new OConfigurationException("Can't add segment '" + iCluster.getName() + "' because it was already registered");

		// CREATE AND ADD THE NEW REF SEGMENT
		clusterMap.put(iCluster.getName(), iCluster);

		final int id = iCluster.getId();

		clusters = OArrays.copyOf(clusters, id + 1);
		clusters[id] = iCluster;

		return id;
	}

	private void checkClusterSegmentIndexRange(final int iClusterId) {
		if (iClusterId > clusters.length - 1)
			throw new IllegalArgumentException("Cluster segment #" + iClusterId + " not exists");
	}

	protected int getDataSegmentForRecord(final OCluster iCluster, final byte[] iContent) {
		// TODO: CREATE POLICY & STRATEGY TO ASSIGN THE BEST-MULTIPLE DATA
		// SEGMENT
		return 0;
	}

	protected long createRecord(final OCluster iClusterSegment, final byte[] iContent, final byte iRecordType) {
		checkOpeness();

		if (iContent == null)
			throw new IllegalArgumentException("Record " + iContent + " is null");

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			final int dataSegment = getDataSegmentForRecord(iClusterSegment, iContent);
			ODataLocal data = getDataSegment(dataSegment);

			final long clusterPosition = iClusterSegment.addPhysicalPosition(-1, -1, iRecordType);

			final long dataOffset = data.addRecord(iClusterSegment.getId(), clusterPosition, iContent);

			// UPDATE THE POSITION IN CLUSTER WITH THE POSITION OF RECORD IN
			// DATA
			iClusterSegment.setPhysicalPosition(clusterPosition, dataSegment, dataOffset, iRecordType);

			incrementVersion();

			return clusterPosition;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on creating record in cluster: " + iClusterSegment, e);
			return -1;
		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.createRecord", timer);
		}
	}

	protected ORawBuffer readRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition, boolean iAtomicLock) {
		if (iPosition < 0)
			throw new IllegalArgumentException("Can't read the record because the position #" + iPosition + " is invalid");

		// NOT FOUND: SEARCH IT IN THE STORAGE
		final long timer = OProfiler.getInstance().startChrono();

		// GET LOCK ONLY IF IT'S IN ATOMIC-MODE (SEE THE PARAMETER iAtomicLock)
		// USUALLY BROWSING OPERATIONS (QUERY) AVOID ATOMIC LOCKING
		// TO IMPROVE PERFORMANCES BY LOCKING THE ENTIRE CLUSTER FROM THE
		// OUTSIDE.
		final boolean locked = iAtomicLock ? lock.acquireSharedLock() : false;

		try {
			// lockManager.acquireLock(iRequesterId, recId, LOCK.SHARED,
			// timeout);

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
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.readRecord", timer);
		}
	}

	protected int updateRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		final long timer = OProfiler.getInstance().startChrono();

		final String recId = ORecordId.generateString(iClusterSegment.getId(), iPosition);

		final boolean locked = lock.acquireSharedLock();

		try {
			// lockManager.acquireLock(iRequesterId, recId, LOCK.EXCLUSIVE,
			// timeout);

			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());
			if (!checkForRecordValidity(ppos))
				// DELETED
				return -1;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && iVersion < ppos.version)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ recId
								+ " because it has been modified by another user (v"
								+ ppos.version
								+ " != v"
								+ iVersion
								+ ") in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			if (ppos.type != iRecordType)
				iClusterSegment.updateRecordType(iPosition, iRecordType);

			iClusterSegment.updateVersion(iPosition, ++ppos.version);

			final long newDataSegmentOffset = getDataSegment(ppos.dataSegment).setRecord(ppos.dataPosition, iClusterSegment.getId(),
					iPosition, iContent);

			if (newDataSegmentOffset != ppos.dataPosition)
				// UPDATE DATA SEGMENT OFFSET WITH THE NEW PHYSICAL POSITION
				iClusterSegment.setPhysicalPosition(iPosition, ppos.dataSegment, newDataSegmentOffset, iRecordType);

			incrementVersion();

			return ppos.version;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on updating record #" + iPosition + " in cluster: " + iClusterSegment, e);

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.updateRecord", timer);
		}

		return -1;
	}

	protected boolean deleteRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition, final int iVersion) {
		final long timer = OProfiler.getInstance().startChrono();

		final String recId = ORecordId.generateString(iClusterSegment.getId(), iPosition);

		final boolean locked = lock.acquireSharedLock();

		try {
			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());

			if (!checkForRecordValidity(ppos))
				// ALREADY DELETED
				return false;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't delete the record #"
								+ recId
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			iClusterSegment.removePhysicalPosition(iPosition, ppos);

			getDataSegment(ppos.dataSegment).deleteRecord(ppos.dataPosition);

			incrementVersion();

			return true;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on deleting record #" + iPosition + " in cluster: " + iClusterSegment, e);

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.deleteRecord", timer);
		}

		return false;
	}

	/**
	 * Check if the storage is open. If it's closed an exception is raised.
	 */
	private void checkOpeness() {
		if (!open)
			throw new OStorageException("Storage " + name + " is not opened.");
	}

	/***
	 * Save the version number to disk
	 * 
	 * @throws IOException
	 */
	private void saveVersion() throws IOException {
		dataSegments[0].files[0].writeHeaderLong(OConstants.SIZE_LONG, version);
	}

	/**
	 * Read the storage version from disk;
	 * 
	 * @return Long as serial version number
	 * @throws IOException
	 */
	private long loadVersion() throws IOException {
		return version = dataSegments[0].files[0].readHeaderLong(OConstants.SIZE_LONG);
	}

	/**
	 * Add a new physical cluster into the storage.
	 * 
	 * @throws IOException
	 */
	private int addPhysicalCluster(final String iClusterName, String iClusterFileName, final int iStartSize) throws IOException {
		final OStoragePhysicalClusterConfiguration config = new OStoragePhysicalClusterConfiguration(configuration, iClusterName,
				clusters.length);
		configuration.clusters.add(config);

		final OClusterLocal cluster = new OClusterLocal(this, config);
		final int id = registerCluster(cluster);

		clusters[id].create(iStartSize);
		configuration.update();
		return id;
	}

	private int addLogicalCluster(final String iClusterName, final int iPhysicalCluster) throws IOException {
		final OStorageLogicalClusterConfiguration config = new OStorageLogicalClusterConfiguration(iClusterName, clusters.length,
				iPhysicalCluster, null);

		configuration.clusters.add(config);

		final OClusterLogical cluster = new OClusterLogical(this, clusters.length, iClusterName, iPhysicalCluster);
		config.map = cluster.getRID();
		final int id = registerCluster(cluster);

		configuration.update();
		return id;
	}

	private int addMemoryCluster(final String iClusterName) throws IOException {
		final OStorageMemoryClusterConfiguration config = new OStorageMemoryClusterConfiguration(iClusterName, clusters.length);

		configuration.clusters.add(config);

		final OClusterMemory cluster = new OClusterMemory(clusters.length, iClusterName);
		final int id = registerCluster(cluster);
		configuration.update();

		return id;
	}

	public int getFixedSize() {
		return fixedSize;
	}

	public void setFixedSize(int fixedSize) {
		if (!isClosed() || exists())
			throw new IllegalStateException("Can't change configuration size to an existent storage");
		this.fixedSize = fixedSize;
	}
}
