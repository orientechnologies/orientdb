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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.concur.lock.OLockManager.LOCK;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageLogicalClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageMemoryClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.fs.OMMapManager;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;
import com.orientechnologies.orient.core.tx.OTransaction;

public class OStorageLocal extends OStorageEmbedded {
	private final int											DELETE_MAX_RETRIES;
	private final int											DELETE_WAIT_TIME;
	public static final String[]					TYPES								= { OClusterLocal.TYPE, OClusterLogical.TYPE };

	private final Map<String, OCluster>		clusterMap					= new LinkedHashMap<String, OCluster>();
	private OCluster[]										clusters						= new OCluster[0];
	private ODataLocal[]									dataSegments				= new ODataLocal[0];

	private final OStorageLocalTxExecuter	txManager;
	private String												storagePath;
	private final OStorageVariableParser	variableParser;
	private int														defaultClusterId		= -1;

	private OStorageConfigurationSegment	configurationSegment;

	private static String[]								ALL_FILE_EXTENSIONS	= { "ocf", ".och", ".ocl", ".oda", ".odh", ".otx" };
	private final String									PROFILER_CREATE_RECORD;
	private final String									PROFILER_READ_RECORD;
	private final String									PROFILER_UPDATE_RECORD;
	private final String									PROFILER_DELETE_RECORD;

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

		variableParser = new OStorageVariableParser(storagePath);
		configuration = new OStorageConfigurationSegment(this, storagePath);
		txManager = new OStorageLocalTxExecuter(this, configuration.txSegment);

		PROFILER_CREATE_RECORD = "storage." + name + ".createRecord";
		PROFILER_READ_RECORD = "storage." + name + ".readRecord";
		PROFILER_UPDATE_RECORD = "storage." + name + ".updateRecord";
		PROFILER_DELETE_RECORD = "storage." + name + ".deleteRecord";

		DELETE_MAX_RETRIES = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();
		DELETE_WAIT_TIME = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();

		installProfilerHooks();
	}

	public synchronized void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();
		try {

			addUser();

			if (status != STATUS.CLOSED)
				// ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
				// REUSED
				return;

			if (!exists())
				throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);

			status = STATUS.OPEN;

			// OPEN BASIC SEGMENTS
			int pos;
			pos = registerDataSegment(new OStorageDataConfiguration(configuration, OStorage.DATA_DEFAULT_NAME));
			dataSegments[pos].open();

			pos = createClusterFromConfig(new OStoragePhysicalClusterConfiguration(configuration, OStorage.CLUSTER_INTERNAL_NAME,
					clusters.length));
			clusters[pos].open();

			configuration.load();

			pos = createClusterFromConfig(new OStoragePhysicalClusterConfiguration(configuration, OStorage.CLUSTER_INDEX_NAME,
					clusters.length));
			clusters[pos].open();

			defaultClusterId = createClusterFromConfig(new OStoragePhysicalClusterConfiguration(configuration,
					OStorage.CLUSTER_DEFAULT_NAME, clusters.length));
			clusters[defaultClusterId].open();

			// REGISTER DATA SEGMENT
			for (int i = 0; i < configuration.dataSegments.size(); ++i) {
				final OStorageDataConfiguration dataConfig = configuration.dataSegments.get(i);

				pos = registerDataSegment(dataConfig);
				if (pos == -1) {
					// CLOSE AND REOPEN TO BE SURE ALL THE FILE SEGMENTS ARE
					// OPENED
					dataSegments[i].close();
					dataSegments[i] = new ODataLocal(this, dataConfig, i);
					dataSegments[i].open();
				} else
					dataSegments[pos].open();
			}

			// REGISTER CLUSTER
			for (int i = 0; i < configuration.clusters.size(); ++i) {
				final OStorageClusterConfiguration clusterConfig = configuration.clusters.get(i);

				if (clusterConfig != null) {
					pos = createClusterFromConfig(clusterConfig);

					try {
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
					} catch (FileNotFoundException e) {
						OLogManager.instance().warn(
								this,
								"Error on loading cluster '" + clusters[i].getName() + "' (" + i + "). It will be excluded from current database '"
										+ getName() + "'.");

						clusterMap.remove(clusters[i].getName());
						clusters[i] = null;
					}
				} else {
					clusters = Arrays.copyOf(clusters, clusters.length + 1);
					clusters[i] = null;
				}
			}

			loadVersion();

			txManager.open();

		} catch (Exception e) {
			close(true);
			throw new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode, e);
		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("storage." + name + ".open", timer);
		}
	}

	public void create(final Map<String, Object> iProperties) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();
		try {

			if (status != STATUS.CLOSED)
				throw new OStorageException("Cannot create new storage '" + name + "' because it is not closed");

			addUser();

			final File storageFolder = new File(storagePath);
			if (!storageFolder.exists())
				storageFolder.mkdir();

			if (exists())
				throw new OStorageException("Cannot create new storage '" + name + "' because it already exists");

			status = STATUS.OPEN;

			addDataSegment(OStorage.DATA_DEFAULT_NAME);

			// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
			addCluster(OStorage.CLUSTER_INTERNAL_NAME, OStorage.CLUSTER_TYPE.PHYSICAL);

			// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
			// INDEXING
			addCluster(OStorage.CLUSTER_INDEX_NAME, OStorage.CLUSTER_TYPE.PHYSICAL);

			// ADD THE DEFAULT CLUSTER
			defaultClusterId = addCluster(OStorage.CLUSTER_DEFAULT_NAME, OStorage.CLUSTER_TYPE.PHYSICAL);

			configuration.create();

			txManager.create();
		} catch (OStorageException e) {
			close();
			throw e;
		} catch (IOException e) {
			close();
			throw new OStorageException("Error on creation of storage '" + name + "'", e);

		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("storage." + name + ".create", timer);
		}
	}

	public void reload() {
	}

	public boolean exists() {
		return exists(storagePath);
	}

	private boolean exists(String path) {
		return new File(path + "/" + OStorage.DATA_DEFAULT_NAME + ".0" + ODataLocal.DEF_EXTENSION).exists();
	}

	@Override
	public void close(final boolean iForce) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();
		try {

			if (!checkForClose(iForce))
				return;

			status = STATUS.CLOSING;

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

			configuration.close();

			level2Cache.shutdown();

			OMMapManager.flush();

			super.close(iForce);

			Orient.instance().unregisterStorage(this);
			status = STATUS.CLOSED;
		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on closing of storage '" + name, e, OStorageException.class);

		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("storage." + name + ".close", timer);
		}
	}

	/**
	 * Deletes physically all the database files (that ends for ".och", ".ocl", ".oda", ".odh", ".otx"). Tries also to delete the
	 * container folder if the directory is empty. If files are locked, retry up to 10 times before to raise an exception.
	 */
	public void delete() {
		// CLOSE THE DATABASE BY REMOVING THE CURRENT USER
		if (status != STATUS.CLOSED) {
			if (getUsers() > 0) {
				while (removeUser() > 0)
					;
			}
		}
		close(true);

		try {
			Orient.instance().unregisterStorage(this);
		} catch (Exception e) {
			OLogManager.instance().error(this, "Cannot unregister storage", e);
		}

		final long timer = OProfiler.getInstance().startChrono();

		// GET REAL DIRECTORY
		File dbDir = new File(OSystemVariableResolver.resolveSystemVariables(url));
		if (!dbDir.exists() || !dbDir.isDirectory())
			dbDir = dbDir.getParentFile();

		lock.acquireExclusiveLock();
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
								if (!f.delete()) {
									notDeletedFiles++;
								}
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

				OLogManager
						.instance()
						.debug(
								this,
								"Cannot delete database files because they are still locked by the OrientDB process: waiting %d ms and retrying %d/%d...",
								DELETE_WAIT_TIME, i, DELETE_MAX_RETRIES);

				// FORCE FINALIZATION TO COLLECT ALL THE PENDING BUFFERS
				OMemoryWatchDog.freeMemory(DELETE_WAIT_TIME);
			}

			throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ". Database files seem locked");

		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("storage." + name + ".delete", timer);
		}
	}

	public void check(final OCommandOutputListener iListener) {
		int errors = 0;
		int warnings = 0;

		lock.acquireSharedLock();
		try {

			long totalRecors = 0;
			final long start = System.currentTimeMillis();

			iListener.onMessage("\nChecking database '" + getName() + "'...\n");

			iListener.onMessage("\n- Checking cluster coherence...\n");

			final OPhysicalPosition ppos = new OPhysicalPosition();

			// BROWSE ALL THE CLUSTERS
			for (OCluster c : clusters) {
				if (!(c instanceof OClusterLocal))
					continue;

				iListener.onMessage(" +- Checking cluster '" + c.getName() + "' (id=" + c.getId() + ")...\n");

				// BROWSE ALL THE RECORDS
				for (final OClusterPositionIterator it = c.absoluteIterator(); it.hasNext();) {
					final Long pos = it.next();
					totalRecors++;
					try {
						c.getPhysicalPosition(pos, ppos);

						if (ppos.dataSegmentId >= dataSegments.length) {
							OLogManager.instance().warn(this, "[OStorageLocal.check] Found wrong data segment %d", ppos.dataSegmentId);
							warnings++;
						}

						if (ppos.recordSize < 0) {
							OLogManager.instance().warn(this, "[OStorageLocal.check] Found wrong record size %d", ppos.recordSize);
							warnings++;
						}

						if (ppos.recordSize >= 1000000) {
							OLogManager.instance().warn(this, "[OStorageLocal.check] Found suspected big record size %d. Is it corrupted?",
									ppos.recordSize);
							warnings++;
						}

						if (ppos.dataChunkPosition > dataSegments[ppos.dataSegmentId].getFilledUpTo()) {
							OLogManager.instance().warn(this,
									"[OStorageLocal.check] Found wrong pointer to data chunk %d out of data segment size (%d)",
									ppos.dataChunkPosition, dataSegments[ppos.dataSegmentId].getFilledUpTo());
							warnings++;
						}

						if (ppos.version < -1) {
							OLogManager.instance().warn(this, "[OStorageLocal.check] Found wrong record version %d", ppos.version);
							warnings++;
						} else if (ppos.version == -1) {
							// CHECK IF THE HOLE EXISTS
							boolean found = false;
							int tot = ((OClusterLocal) c).holeSegment.getHoles();
							for (int i = 0; i < tot; ++i) {
								final long recycledPosition = ((OClusterLocal) c).holeSegment.getEntryPosition(i) / OClusterLocal.RECORD_SIZE;
								if (recycledPosition == pos) {
									// FOUND
									found = true;
									break;
								}
							}

							if (!found) {
								OLogManager.instance()
										.warn(this, "[OStorageLocal.check] Cannot find hole for deleted record %d:%d", c.getId(), pos);
								warnings++;
							}
						}

					} catch (IOException e) {
						OLogManager.instance().warn(this, "[OStorageLocal.check] Error while reading record #%d:%d", e, c.getId(), pos);
						warnings++;
					}
				}

				final int tot = ((OClusterLocal) c).holeSegment.getHoles();
				if (tot > 0) {
					iListener.onMessage("  +- Checking " + tot + " hole(s)...\n");
					// CHECK HOLES
					for (int i = 0; i < tot; ++i) {
						long recycledPosition = -1;
						try {
							recycledPosition = ((OClusterLocal) c).holeSegment.getEntryPosition(i) / OClusterLocal.RECORD_SIZE;
							c.getPhysicalPosition(recycledPosition, ppos);

							if (ppos.version != -1) {
								OLogManager.instance().warn(this,
										"[OStorageLocal.check] Found wrong hole %d/%d for deleted record %d:%d. The record seems good", i, tot - 1,
										c.getId(), recycledPosition);
								warnings++;
							}
						} catch (Exception e) {
							OLogManager.instance().warn(this,
									"[OStorageLocal.check] Found wrong hole %d/%d for deleted record %d:%d. The record not exists", i, tot - 1,
									c.getId(), recycledPosition);
							warnings++;
						}
					}
				}
			}

			int totalChunks = 0;
			iListener.onMessage("\n- Checking data chunks integrity...\n");
			for (ODataLocal d : dataSegments) {
				int pos = 0;
				while (pos < d.getFilledUpTo()) {
					totalChunks++;
					int recordSize = Integer.MIN_VALUE;
					try {
						recordSize = d.getRecordSize(pos);
						final ORecordId rid = d.getRecordRid(pos);

						if (recordSize < 0) {
							// HOLE: CHECK HOLE PRESENCE
							boolean found = false;
							for (ODataHoleInfo hole : getHolesList()) {
								if (hole.dataOffset == pos) {
									found = true;
									break;
								}
							}

							if (!found) {
								OLogManager.instance().warn(this, "[OStorageLocal.check] Cannot find hole for deleted chunk %d", pos);
								warnings++;
							}

							if (rid.isValid()) {
								OLogManager
										.instance()
										.warn(
												this,
												"[OStorageLocal.check] Deleted chunk at position %d (recordSize=%d) points to the valid RID %s instead of #-1:-1",
												pos, recordSize, rid);
								warnings++;
							}

							recordSize *= -1;
							pos += recordSize;

						} else {
							final byte[] buffer = d.getRecord(pos);
							if (buffer.length != recordSize) {
								OLogManager.instance().warn(this, "[OStorageLocal.check] Wrong record size: found %d but record length is %d",
										recordSize, buffer.length);
								warnings++;
							}

							if (!rid.isValid()) {
								OLogManager.instance().warn(this, "[OStorageLocal.check] Chunk at position %d points to invalid RID %s", pos, rid);
								warnings++;
							} else {
								if (clusters[rid.clusterId] == null) {
									OLogManager.instance().warn(this,
											"[OStorageLocal.check] Found ghost chunk at position %d pointed from %s. The cluster %d not exists", pos,
											rid, rid.clusterId);
									warnings++;
								} else {
									clusters[rid.clusterId].getPhysicalPosition(rid.clusterPosition, ppos);

									if (ppos.dataSegmentId != d.getId()) {
										OLogManager.instance().warn(this,
												"[OStorageLocal.check] Wrong record chunk data segment: found %d but current id is %d", ppos.dataSegmentId,
												d.getId());
										warnings++;
									}

									if (ppos.dataChunkPosition != pos) {
										OLogManager.instance().warn(this,
												"[OStorageLocal.check] Wrong chunk position: cluster record points to %d, but current chunk is at %d",
												ppos.dataChunkPosition, pos);
										warnings++;
									}
								}
							}
							pos += OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG + recordSize;
						}
					} catch (Exception e) {
						OLogManager.instance().warn(this, "[OStorageLocal.check] Found wrong chunk %d", pos);
						errors++;
						break;
					}
				}
			}

			iListener.onMessage("\nCheck of database completed in " + (System.currentTimeMillis() - start)
					+ "ms:\n- Total records checked: " + totalRecors + "\n- Total chunks checked.: " + totalChunks
					+ "\n- Warnings.............: " + warnings + "\n- Errors...............: " + errors + "\n");

		} finally {
			lock.releaseSharedLock();
		}
	}

	public ODataLocal getDataSegment(final int iDataSegmentId) {
		checkOpeness();

		lock.acquireSharedLock();
		try {

			if (iDataSegmentId >= dataSegments.length)
				throw new IllegalArgumentException("Data segment #" + iDataSegmentId + " does not exist in storage '" + name + "'");

			return dataSegments[iDataSegmentId];

		} finally {
			lock.releaseSharedLock();
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

		lock.acquireExclusiveLock();
		try {

			final OStorageDataConfiguration conf = new OStorageDataConfiguration(configuration, iSegmentName);
			configuration.dataSegments.add(conf);

			final int pos = registerDataSegment(conf);

			if (pos == -1)
				throw new OConfigurationException("Cannot add segment " + conf.name + " because it is already part of storage '" + name
						+ "'");

			dataSegments[pos].create(-1);
			configuration.update();

			return pos;
		} catch (Throwable e) {
			OLogManager.instance().error(this, "Error on creation of new data segment '" + iSegmentName + "' in: " + iSegmentFileName, e,
					OStorageException.class);
			return -1;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	/**
	 * Add a new cluster into the storage. Type can be: "physical" or "logical".
	 */
	public int addCluster(String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iParameters) {
		checkOpeness();

		try {
			iClusterName = iClusterName != null ? iClusterName.toLowerCase() : null;

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
		}

		return -1;
	}

	public ODataLocal[] getDataSegments() {
		return dataSegments;
	}

	public OStorageLocalTxExecuter getTxManager() {
		return txManager;
	}

	public boolean dropCluster(final int iClusterId) {
		lock.acquireExclusiveLock();
		try {

			if (iClusterId < 0 || iClusterId >= clusters.length)
				throw new IllegalArgumentException("Cluster id '" + iClusterId + "' is outside the of range of configured clusters (0-"
						+ (clusters.length - 1) + ") in storage '" + name + "'");

			final OCluster cluster = clusters[iClusterId];
			if (cluster == null)
				return false;

			getLevel2Cache().freeCluster(iClusterId);

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
			lock.releaseExclusiveLock();
		}

		return false;
	}

	public long count(final int[] iClusterIds) {
		checkOpeness();

		lock.acquireSharedLock();
		try {

			long tot = 0;

			for (int i = 0; i < iClusterIds.length; ++i) {
				if (iClusterIds[i] >= clusters.length)
					throw new OConfigurationException("Cluster id " + iClusterIds[i] + " was not found in storage '" + name + "'");

				final OCluster c = clusters[iClusterIds[i]];
				if (c != null)
					tot += c.getEntries();
			}

			return tot;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public long[] getClusterDataRange(final int iClusterId) {
		if (iClusterId == -1)
			throw new OStorageException("Cluster Id " + iClusterId + " is invalid in storage '" + name + "'");

		checkOpeness();

		lock.acquireSharedLock();
		try {

			return clusters[iClusterId] != null ? new long[] { clusters[iClusterId].getFirstEntryPosition(),
					clusters[iClusterId].getLastEntryPosition() } : new long[0];

		} finally {
			lock.releaseSharedLock();
		}
	}

	public long count(final int iClusterId) {
		if (iClusterId == -1)
			throw new OStorageException("Cluster Id " + iClusterId + " is invalid in storage '" + name + "'");

		// COUNT PHYSICAL CLUSTER IF ANY
		checkOpeness();

		lock.acquireSharedLock();
		try {

			return clusters[iClusterId] != null ? clusters[iClusterId].getEntries() : 0l;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public long createRecord(final ORecordId iRid, final byte[] iContent, final byte iRecordType, ORecordCallback<Long> iCallback) {
		checkOpeness();

		iRid.clusterPosition = createRecord(getClusterById(iRid.clusterId), iContent, iRecordType);
		return iRid.clusterPosition;
	}

	public ORawBuffer readRecord(final ORecordId iRid, final String iFetchPlan, ORecordCallback<ORawBuffer> iCallback) {
		checkOpeness();
		return readRecord(getClusterById(iRid.clusterId), iRid, true);
	}

	public int updateRecord(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType,
			ORecordCallback<Integer> iCallback) {
		checkOpeness();
		return updateRecord(getClusterById(iRid.clusterId), iRid, iContent, iVersion, iRecordType);
	}

	public boolean deleteRecord(final ORecordId iRid, final int iVersion, ORecordCallback<Boolean> iCallback) {
		checkOpeness();
		return deleteRecord(getClusterById(iRid.clusterId), iRid, iVersion);
	}

	public Set<String> getClusterNames() {
		checkOpeness();

		lock.acquireSharedLock();
		try {

			return clusterMap.keySet();

		} finally {
			lock.releaseSharedLock();
		}
	}

	public int getClusterIdByName(final String iClusterName) {
		checkOpeness();

		if (iClusterName == null)
			throw new IllegalArgumentException("Cluster name is null");

		if (iClusterName.length() == 0)
			throw new IllegalArgumentException("Cluster name is empty");

		if (Character.isDigit(iClusterName.charAt(0)))
			return Integer.parseInt(iClusterName);

		// SEARCH IT BETWEEN PHYSICAL CLUSTERS
		lock.acquireSharedLock();
		try {

			final OCluster segment = clusterMap.get(iClusterName.toLowerCase());
			if (segment != null)
				return segment.getId();

		} finally {
			lock.releaseSharedLock();
		}

		return -1;
	}

	public String getClusterTypeByName(final String iClusterName) {
		checkOpeness();

		if (iClusterName == null)
			throw new IllegalArgumentException("Cluster name is null");

		// SEARCH IT BETWEEN PHYSICAL CLUSTERS
		lock.acquireSharedLock();
		try {

			final OCluster segment = clusterMap.get(iClusterName.toLowerCase());
			if (segment != null)
				return segment.getType();

		} finally {
			lock.releaseSharedLock();
		}

		return null;
	}

	public void commit(final OTransaction iTx) {
		lock.acquireExclusiveLock();
		try {

			try {
				txManager.clearLogEntries(iTx);
				txManager.commitAllPendingRecords(iTx);

				incrementVersion();
				if (OGlobalConfiguration.TX_COMMIT_SYNCH.getValueAsBoolean())
					synch();

			} catch (RuntimeException e) {
				// WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
				rollback(iTx);
				throw e;
			} catch (IOException e) {
				// WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
				rollback(iTx);
				throw new OException(e);
			} finally {
				try {
					txManager.clearLogEntries(iTx);
				} catch (Exception e) {
					// XXX WHAT CAN WE DO HERE ? ROLLBACK IS NOT POSSIBLE
					// IF WE THROW EXCEPTION, A ROLLBACK WILL BE DONE AT DB LEVEL BUT NOT AT STORAGE LEVEL
					OLogManager.instance().error(this, "Clear tx log entries failed", e);
				}
			}
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void rollback(final OTransaction iTx) {
		try {
			txManager.getTxSegment().rollback(iTx);
			if (OGlobalConfiguration.TX_COMMIT_SYNCH.getValueAsBoolean())
				synch();
		} catch (IOException ioe) {
			OLogManager.instance().error(this,
					"Error executing rollback for transaction with id '" + iTx.getId() + "' cause: " + ioe.getMessage(), ioe);
		}
	}

	public void synch() {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();
		try {
			saveVersion();

			for (OCluster cluster : clusters)
				if (cluster != null)
					cluster.synch();

			for (ODataLocal data : dataSegments)
				if (data != null)
					data.synch();

		} catch (IOException e) {
			throw new OStorageException("Error on synch storage '" + name + "'", e);

		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("storage." + name + ".synch", timer);
		}
	}

	/**
	 * Returns the list of holes as pair of position & ODataHoleInfo
	 * 
	 * @throws IOException
	 */
	public List<ODataHoleInfo> getHolesList() {
		final List<ODataHoleInfo> holes = new ArrayList<ODataHoleInfo>();

		lock.acquireSharedLock();
		try {

			for (ODataLocal d : dataSegments)
				holes.addAll(d.getHolesList());

			return holes;

		} finally {
			lock.releaseSharedLock();
		}
	}

	/**
	 * Returns the total number of holes.
	 * 
	 * @throws IOException
	 */
	public long getHoles() {
		lock.acquireSharedLock();
		try {

			long holes = 0;
			for (ODataLocal d : dataSegments)
				holes += d.getHoles();
			return holes;

		} finally {
			lock.releaseSharedLock();
		}
	}

	/**
	 * Returns the total size used by holes
	 * 
	 * @throws IOException
	 */
	public long getHoleSize() {
		lock.acquireSharedLock();
		try {

			final List<ODataHoleInfo> holes = getHolesList();
			long size = 0;
			for (ODataHoleInfo h : holes)
				if (h.dataOffset > -1 && h.size > 0)
					size += h.size;

			return size;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		checkOpeness();

		lock.acquireSharedLock();
		try {

			if (iClusterId >= clusters.length)
				return null;

			return clusters[iClusterId] != null ? clusters[iClusterId].getName() : null;

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public OStorageConfiguration getConfiguration() {
		return configuration;
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public OCluster getClusterById(int iClusterId) {
		lock.acquireSharedLock();
		try {

			if (iClusterId == ORID.CLUSTER_ID_INVALID)
				// GET THE DEFAULT CLUSTER
				iClusterId = defaultClusterId;

			checkClusterSegmentIndexRange(iClusterId);

			final OCluster cluster = clusters[iClusterId];
			if (cluster == null)
				throw new IllegalArgumentException("Cluster " + iClusterId + " is null");

			return cluster;
		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public OCluster getClusterByName(final String iClusterName) {
		lock.acquireSharedLock();
		try {

			final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());

			if (cluster == null)
				throw new IllegalArgumentException("Cluster " + iClusterName + " does not exist in storage '" + name + "'");
			return cluster;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public long getSize() {
		lock.acquireSharedLock();
		try {

			long size = 0;

			for (ODataLocal d : dataSegments)
				if (d != null)
					size += d.getFilledUpTo();

			for (OCluster c : clusters)
				if (c != null)
					size += c.getSize();

			return size;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public OStorageConfigurationSegment getConfigurationSegment() {
		return configurationSegment;
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

	public int getClusters() {
		lock.acquireSharedLock();
		try {

			return clusters.length;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public Set<OCluster> getClusterInstances() {
		final Set<OCluster> result = new HashSet<OCluster>();

		lock.acquireSharedLock();
		try {

			// ADD ALL THE CLUSTERS
			for (OCluster c : clusters)
				result.add(c);

		} finally {
			lock.releaseSharedLock();
		}

		return result;
	}

	/**
	 * Method that completes the cluster rename operation. <strong>IT WILL NOT RENAME A CLUSTER, IT JUST CHANGES THE NAME IN THE
	 * INTERNAL MAPPING</strong>
	 */
	public void renameCluster(final String iOldName, final String iNewName) {
		clusterMap.put(iNewName, clusterMap.remove(iOldName));
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
		final ODataLocal segment = new ODataLocal(this, iConfig, pos);

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
		final int id;

		if (iCluster != null) {
			// CHECK FOR DUPLICATION OF NAMES
			if (clusterMap.containsKey(iCluster.getName()))
				throw new OConfigurationException("Cannot add segment '" + iCluster.getName()
						+ "' because it is already registered in storage '" + name + "'");
			// CREATE AND ADD THE NEW REF SEGMENT
			clusterMap.put(iCluster.getName(), iCluster);
			id = iCluster.getId();
		} else
			id = clusters.length;

		clusters = OArrays.copyOf(clusters, clusters.length + 1);
		clusters[id] = iCluster;

		return id;
	}

	private void checkClusterSegmentIndexRange(final int iClusterId) {
		if (iClusterId > clusters.length - 1)
			throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in storage '" + name + "'");
	}

	protected int getDataSegmentForRecord(final OCluster iCluster, final byte[] iContent) {
		// TODO: CREATE POLICY & STRATEGY TO ASSIGN THE BEST-MULTIPLE DATA
		// SEGMENT
		return 0;
	}

	protected long createRecord(final OCluster iClusterSegment, final byte[] iContent, final byte iRecordType) {
		checkOpeness();

		if (iContent == null)
			throw new IllegalArgumentException("Record is null");

		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireSharedLock();
		try {

			final int dataSegment = getDataSegmentForRecord(iClusterSegment, iContent);
			final ODataLocal data = getDataSegment(dataSegment);

			final ORecordId rid = new ORecordId(iClusterSegment.getId());
			rid.clusterPosition = iClusterSegment.addPhysicalPosition(-1, -1, iRecordType);

			final long dataOffset = data.addRecord(rid, iContent);

			// UPDATE THE POSITION IN CLUSTER WITH THE POSITION OF RECORD IN
			// DATA
			iClusterSegment.setPhysicalPosition(rid.clusterPosition, dataSegment, dataOffset, iRecordType, 0);

			incrementVersion();

			return rid.clusterPosition;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on creating record in cluster: " + iClusterSegment, e);
			return -1;
		} finally {
			lock.releaseSharedLock();

			OProfiler.getInstance().stopChrono(PROFILER_CREATE_RECORD, timer);
		}
	}

	@Override
	protected ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, boolean iAtomicLock) {
		if (iRid.clusterPosition < 0)
			throw new IllegalArgumentException("Cannot read record " + iRid + " since the position is invalid in storage '" + name + "'");

		// NOT FOUND: SEARCH IT IN THE STORAGE
		final long timer = OProfiler.getInstance().startChrono();

		// GET LOCK ONLY IF IT'S IN ATOMIC-MODE (SEE THE PARAMETER iAtomicLock)
		// USUALLY BROWSING OPERATIONS (QUERY) AVOID ATOMIC LOCKING
		// TO IMPROVE PERFORMANCES BY LOCKING THE ENTIRE CLUSTER FROM THE
		// OUTSIDE.
		if (iAtomicLock)
			lock.acquireSharedLock();

		try {

			lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.SHARED);
			try {

				final long lastPos = iClusterSegment.getLastEntryPosition();

				if (lastPos < 0)
					throw new ORecordNotFoundException("Record " + iRid + " is outside cluster range. The cluster '"
							+ iClusterSegment.getName() + "' is empty in storage '" + name + "'");

				if (iRid.clusterPosition > lastPos)
					throw new ORecordNotFoundException("Record " + iRid + " is outside cluster range. Valid range for cluster '"
							+ iClusterSegment.getName() + "' is 0-" + lastPos + " in storage '" + name + "'");

				final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());
				if (ppos == null || !checkForRecordValidity(ppos))
					// DELETED
					return null;

				final ODataLocal data = getDataSegment(ppos.dataSegmentId);
				return new ORawBuffer(data.getRecord(ppos.dataChunkPosition), ppos.version, ppos.type);

			} finally {
				lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.SHARED);
			}

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on reading record " + iRid + " (cluster: " + iClusterSegment + ")", e);
			return null;

		} finally {
			if (iAtomicLock)
				lock.releaseSharedLock();

			OProfiler.getInstance().stopChrono(PROFILER_READ_RECORD, timer);
		}
	}

	protected int updateRecord(final OCluster iClusterSegment, final ORecordId iRid, final byte[] iContent, final int iVersion,
			final byte iRecordType) {
		if (iClusterSegment == null)
			throw new OStorageException("Cluster not defined for record: " + iRid);

		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();

		try {
			lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			try {
				final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());
				if (!checkForRecordValidity(ppos))
					// DELETED
					return -1;

				// VERSION CONTROL CHECK
				switch (iVersion) {
				// DOCUMENT UPDATE, NO VERSION CONTROL
				case -1:
					++ppos.version;
					iClusterSegment.updateVersion(iRid.clusterPosition, ppos.version);
					break;

				// DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION UPDATE
				case -2:
					break;

				// DOCUMENT ROLLBACK, DECREMENT VERSION
				case -3:
					--ppos.version;
					iClusterSegment.updateVersion(iRid.clusterPosition, ppos.version);
					break;

				default:
					// MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
					if (iVersion > -1) {
						// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
						if (iVersion != ppos.version)
							throw new OConcurrentModificationException(
									"Cannot update record "
											+ iRid
											+ " in storage '"
											+ name
											+ "' because the version is not the latest. Probably you are updating an old record or it has been modified by another user (db=v"
											+ ppos.version + " your=v" + iVersion + ")", iRid, ppos.version, iVersion);

						++ppos.version;
						iClusterSegment.updateVersion(iRid.clusterPosition, ppos.version);
					} else
						throw new IllegalArgumentException("Cannot update record " + iRid + " in storage '" + name
								+ "' because the version is not correct: recieved=" + iVersion
								+ " expected=-1 (skip version control),-2 (skip version control and increment),-3 (rollback) or " + ppos.version
								+ "(current version)");

				}

				if (ppos.type != iRecordType)
					iClusterSegment.updateRecordType(iRid.clusterPosition, iRecordType);

				final long newDataSegmentOffset;
				if (ppos.dataChunkPosition == -1)
					// WAS EMPTY FIRST TIME, CREATE IT NOW
					newDataSegmentOffset = getDataSegment(ppos.dataSegmentId).addRecord(iRid, iContent);
				else
					// UPDATE IT
					newDataSegmentOffset = getDataSegment(ppos.dataSegmentId).setRecord(ppos.dataChunkPosition, iRid, iContent);

				if (newDataSegmentOffset != ppos.dataChunkPosition)
					// UPDATE DATA SEGMENT OFFSET WITH THE NEW PHYSICAL POSITION
					iClusterSegment.setPhysicalPosition(iRid.clusterPosition, ppos.dataSegmentId, newDataSegmentOffset, iRecordType,
							ppos.version);

				incrementVersion();

				return ppos.version;

			} finally {
				lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			}
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on updating record " + iRid + " (cluster: " + iClusterSegment + ")", e);

		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono(PROFILER_UPDATE_RECORD, timer);
		}

		return -1;
	}

	protected boolean deleteRecord(final OCluster iClusterSegment, final ORecordId iRid, final int iVersion) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();
		try {

			lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			try {

				final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());

				if (!checkForRecordValidity(ppos))
					// ALREADY DELETED
					return false;

				// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
				if (iVersion > -1 && ppos.version != iVersion)
					throw new OConcurrentModificationException(
							"Cannot delete the record "
									+ iRid
									+ " in storage '"
									+ name
									+ "' because the version is not the latest. Probably you are deleting an old record or it has been modified by another user (db=v"
									+ ppos.version + " your=v" + iVersion + ")", iRid, ppos.version, iVersion);

				iClusterSegment.removePhysicalPosition(iRid.clusterPosition, ppos);

				if (ppos.dataChunkPosition > -1)
					getDataSegment(ppos.dataSegmentId).deleteRecord(ppos.dataChunkPosition);

				incrementVersion();

				return true;

			} finally {
				lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
			}
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on deleting record " + iRid + "( cluster: " + iClusterSegment + ")", e);

		} finally {
			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono(PROFILER_DELETE_RECORD, timer);
		}

		return false;
	}

	/***
	 * Save the version number to disk
	 * 
	 * @throws IOException
	 */
	private void saveVersion() throws IOException {
		lock.acquireExclusiveLock();
		try {

			if (dataSegments.length > 0)
				dataSegments[0].saveVersion(version.get());

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	/**
	 * Read the storage version from disk;
	 * 
	 * @return Long as serial version number
	 * @throws IOException
	 */
	private long loadVersion() throws IOException {
		lock.acquireExclusiveLock();
		try {

			final long v = dataSegments[0].loadVersion();
			version.set(v);
			return v;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	/**
	 * Add a new physical cluster into the storage.
	 * 
	 * @throws IOException
	 */
	private int addPhysicalCluster(final String iClusterName, String iClusterFileName, final int iStartSize) throws IOException {
		lock.acquireExclusiveLock();
		try {

			final OClusterLocal cluster;

			if (iClusterName != null) {
				// FIND THE FIRST AVAILABLE CLUSTER ID
				int clusterPos = clusters.length;
				for (int i = 0; i < clusters.length; ++i)
					if (clusters[i] == null) {
						clusterPos = i;
						break;
					}

				final OStoragePhysicalClusterConfiguration config = new OStoragePhysicalClusterConfiguration(configuration, iClusterName,
						clusterPos);
				configuration.clusters.add(config);

				cluster = new OClusterLocal(this, config);
			} else
				cluster = null;

			final int id = registerCluster(cluster);

			if (iClusterName != null) {
				clusters[id].create(iStartSize);
				configuration.update();
			}

			return id;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	@Deprecated
	private int addLogicalCluster(final String iClusterName, final int iPhysicalCluster) throws IOException {
		lock.acquireExclusiveLock();
		try {

			final OClusterLogical cluster;

			if (iClusterName != null) {
				final OStorageLogicalClusterConfiguration config = new OStorageLogicalClusterConfiguration(iClusterName, clusters.length,
						iPhysicalCluster, null);

				configuration.clusters.add(config);

				cluster = new OClusterLogical(this, clusters.length, iClusterName, iPhysicalCluster);
				config.map = cluster.getRID();
			} else
				cluster = null;

			final int id = registerCluster(cluster);

			if (iClusterName != null)
				configuration.update();

			return id;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	private int addMemoryCluster(final String iClusterName) throws IOException {
		lock.acquireExclusiveLock();
		try {
			final OClusterMemory cluster;

			if (iClusterName != null) {
				final OStorageMemoryClusterConfiguration config = new OStorageMemoryClusterConfiguration(iClusterName, clusters.length);

				configuration.clusters.add(config);

				cluster = new OClusterMemory(clusters.length, iClusterName);
			} else
				cluster = null;

			final int id = registerCluster(cluster);

			if (iClusterName != null)
				configuration.update();

			return id;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	private void installProfilerHooks() {
		OProfiler.getInstance().registerHookValue("storage." + name + ".data.holes", new OProfilerHookValue() {
			public Object getValue() {
				return getHoles();
			}
		});
		OProfiler.getInstance().registerHookValue("storage." + name + ".data.holeSize", new OProfilerHookValue() {
			public Object getValue() {
				return getHoleSize();
			}
		});
	}
}
