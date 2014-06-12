/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.engine.OLocalHashTableIndexEngine;
import com.orientechnologies.orient.core.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteDiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OWOWCache;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.ODataLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractCheckPointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OCheckpointEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPagesRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFileCreatedCreatedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFullCheckpointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPaginatedClusterFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageBrokenException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTxListener;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OStorageLocalAbstract {
  private static final int                      ONE_KB                               = 1024;

  private static String[]                       ALL_FILE_EXTENSIONS                  = { ".ocf", ".pls", ".pcl", ".oda", ".odh",
      ".otx", ".ocs", ".oef", ".oem", ".oet", OWriteAheadLog.WAL_SEGMENT_EXTENSION, OWriteAheadLog.MASTER_RECORD_EXTENSION,
      OLocalHashTableIndexEngine.BUCKET_FILE_EXTENSION, OLocalHashTableIndexEngine.METADATA_FILE_EXTENSION,
      OLocalHashTableIndexEngine.TREE_FILE_EXTENSION, OLocalHashTableIndexEngine.NULL_BUCKET_FILE_EXTENSION,
      OClusterPositionMap.DEF_EXTENSION, OSBTreeIndexEngine.DATA_FILE_EXTENSION, OWOWCache.NAME_ID_MAP_EXTENSION,
      OIndexRIDContainer.INDEX_FILE_EXTENSION, OSBTreeCollectionManagerShared.DEFAULT_EXTENSION,
      OSBTreeIndexEngine.NULL_BUCKET_FILE_EXTENSION                                 };

  private final int                             DELETE_MAX_RETRIES;
  private final int                             DELETE_WAIT_TIME;

  private final ConcurrentMap<String, OCluster> clusterMap                           = new ConcurrentHashMap<String, OCluster>();
  private CopyOnWriteArrayList<OCluster>        clusters                             = new CopyOnWriteArrayList<OCluster>();

  private final OStorageVariableParser          variableParser;

  private String                                storagePath;
  private volatile int                          defaultClusterId                     = -1;
  private final OModificationLock               modificationLock                     = new OModificationLock();

  private final OPaginatedStorageDirtyFlag      dirtyFlag;

  private ScheduledExecutorService              fuzzyCheckpointExecutor;
  private ExecutorService                       checkpointExecutor;

  private volatile boolean                      wereDataRestoredAfterOpen            = false;

  private boolean                               makeFullCheckPointAfterClusterCreate = OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CLUSTER_CREATE
                                                                                         .getValueAsBoolean();

  public OLocalPaginatedStorage(final String name, final String filePath, final String mode) throws IOException {
    super(name, filePath, mode);

    File f = new File(url);

    if (f.exists() || !exists(f.getParent())) {
      // ALREADY EXISTS OR NOT LEGACY
      storagePath = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getPath()));
    } else {
      // LEGACY DB
      storagePath = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getParent()));
    }

    storagePath = OIOUtils.getPathFromDatabaseName(storagePath);

    variableParser = new OStorageVariableParser(storagePath);
    configuration = new OStorageConfigurationSegment(this);

    DELETE_MAX_RETRIES = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();
    DELETE_WAIT_TIME = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();

    dirtyFlag = new OPaginatedStorageDirtyFlag(storagePath + File.separator + "dirty.fl");
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
    addUser();

    if (status == STATUS.OPEN)
      // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
      // REUSED
      return;

    lock.acquireExclusiveLock();
    try {
      if (status == STATUS.OPEN)
        // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
        // REUSED
        return;

      status = STATUS.OPENING;

      if (!exists())
        throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);

      configuration.load();
      componentsFactory = new OCurrentStorageComponentsFactory(configuration);

      initWal();

      if (configuration.binaryFormatVersion >= 11) {
        if (dirtyFlag.exits())
          dirtyFlag.open();
        else {
          dirtyFlag.create();
          dirtyFlag.makeDirty();
        }
      } else {
        if (dirtyFlag.exits())
          dirtyFlag.open();
        else {
          dirtyFlag.create();
          dirtyFlag.clearDirty();
        }
      }

      // OPEN BASIC SEGMENTS
      int pos;
      addDefaultClusters();

      // REGISTER CLUSTER
      for (int i = 0; i < configuration.clusters.size(); ++i) {
        final OStorageClusterConfiguration clusterConfig = configuration.clusters.get(i);

        if (clusterConfig != null) {
          pos = createClusterFromConfig(clusterConfig);

          try {
            if (pos == -1) {
              clusters.get(i).open();
            } else {
              if (clusterConfig.getName().equals(CLUSTER_DEFAULT_NAME))
                defaultClusterId = pos;

              clusters.get(pos).open();
            }
          } catch (FileNotFoundException e) {
            OLogManager.instance().warn(
                this,
                "Error on loading cluster '" + clusters.get(i).getName() + "' (" + i
                    + "): file not found. It will be excluded from current database '" + getName() + "'.");

            clusterMap.remove(clusters.get(i).getName().toLowerCase());

            setCluster(i, null);
          }
        } else {
          setCluster(i, null);
        }
      }

      restoreIfNeeded();
      dirtyFlag.clearDirty();

      status = STATUS.OPEN;
    } catch (Exception e) {
      status = STATUS.CLOSED;
      throw new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode, e);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  private void setCluster(int id, OCluster cluster) {
    if (clusters.size() <= id) {
      while (clusters.size() < id)
        clusters.add(null);

      clusters.add(cluster);
    } else
      clusters.set(id, cluster);
  }

  public boolean wereDataRestoredAfterOpen() {
    return wereDataRestoredAfterOpen;
  }

  public void create(final Map<String, Object> iProperties) {
    lock.acquireExclusiveLock();
    try {

      if (status != STATUS.CLOSED)
        throw new OStorageException("Cannot create new storage '" + name + "' because it is not closed");

      addUser();

      final File storageFolder = new File(storagePath);
      if (!storageFolder.exists())
        storageFolder.mkdirs();

      if (exists())
        throw new OStorageException("Cannot create new storage '" + name + "' because it already exists");

      componentsFactory = new OCurrentStorageComponentsFactory(configuration);
      initWal();

      dirtyFlag.create();
      status = STATUS.OPEN;

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      doAddCluster(OMetadataDefault.CLUSTER_INTERNAL_NAME, null, false, null);

      configuration.create();

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      doAddCluster(OMetadataDefault.CLUSTER_INDEX_NAME, null, false, null);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      doAddCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null, false, null);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = doAddCluster(CLUSTER_DEFAULT_NAME, null, false, null);

      dirtyFlag.makeDirty();
      if (OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE.getValueAsBoolean())
        makeFullCheckpoint();

    } catch (OStorageException e) {
      close();
      throw e;
    } catch (IOException e) {
      close();
      throw new OStorageException("Error on creation of storage '" + name + "'", e);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void reload() {
  }

  public boolean exists() {
    return exists(storagePath);
  }

  @Override
  public void close(final boolean force, boolean onDelete) {
    doClose(force, onDelete);
  }

  public void delete() {
    final long timer = Orient.instance().getProfiler().startChrono();
    File dbDir = null;

    lock.acquireExclusiveLock();
    try {
      // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
      if (status != STATUS.CLOSED) {
        if (getUsers() > 0) {
          while (removeUser() > 0)
            ;
        }
      }

      doClose(true, true);

      try {
        Orient.instance().unregisterStorage(this);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Cannot unregister storage", e);
      }

      // GET REAL DIRECTORY
      dbDir = new File(OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url)));
      if (!dbDir.exists() || !dbDir.isDirectory())
        dbDir = dbDir.getParentFile();

      if (writeAheadLog != null)
        writeAheadLog.delete();

      if (diskCache != null)
        diskCache.delete();

      dirtyFlag.delete();

      // RETRIES
      for (int i = 0; i < DELETE_MAX_RETRIES; ++i) {
        if (dbDir != null && dbDir.exists() && dbDir.isDirectory()) {
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
        OMemoryWatchDog.freeMemoryForResourceCleanup(DELETE_WAIT_TIME);
      }

      throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ". Database files seem locked");

    } catch (IOException e) {
      throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ".", e);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");
    }
  }

  public boolean check(final boolean verbose, final OCommandOutputListener listener) {
    lock.acquireExclusiveLock();

    try {
      final long start = System.currentTimeMillis();

      OPageDataVerificationError[] pageErrors = diskCache.checkStoredPages(verbose ? listener : null);

      listener.onMessage("Check of storage completed in " + (System.currentTimeMillis() - start) + "ms. "
          + (pageErrors.length > 0 ? pageErrors.length + " with errors." : " without errors."));

      return pageErrors.length == 0;
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public ODataLocal getDataSegmentById(final int dataSegmentId) {
    OLogManager.instance().error(
        this,
        "getDataSegmentById: Local paginated storage does not support data segments. "
            + "null will be returned for data segment %d.", dataSegmentId);
    return null;
  }

  public int getDataSegmentIdByName(final String dataSegmentName) {
    OLogManager.instance().debug(
						this,
						"getDataSegmentIdByName: Local paginated storage does not support data segments. "
										+ "-1 will be returned for data segment %s.", dataSegmentName);

    return -1;
  }

  /**
   * Add a new data segment in the default segment directory and with filename equals to the cluster name.
   */
  public int addDataSegment(final String iDataSegmentName) {
    return addDataSegment(iDataSegmentName, null);
  }

  public void enableFullCheckPointAfterClusterCreate() {
    checkOpeness();
    lock.acquireExclusiveLock();
    try {
      makeFullCheckPointAfterClusterCreate = true;
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void disableFullCheckPointAfterClusterCreate() {
    checkOpeness();
    lock.acquireExclusiveLock();
    try {
      makeFullCheckPointAfterClusterCreate = false;
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean isMakeFullCheckPointAfterClusterCreate() {
    checkOpeness();
    lock.acquireSharedLock();
    try {
      return makeFullCheckPointAfterClusterCreate;
    } finally {
      lock.releaseSharedLock();
    }
  }

  public int addDataSegment(final String segmentName, final String directory) {
    OLogManager.instance().debug(
        this,
        "addDataSegment: Local paginated storage does not support data"
            + " segments, segment %s will not be added in directory %s.", segmentName, directory);

    return -1;
  }

  public int addCluster(final String clusterType, String clusterName, final String location, final String dataSegmentName,
      boolean forceListBased, final Object... parameters) {
    checkOpeness();

    lock.acquireExclusiveLock();
    try {

      dirtyFlag.makeDirty();
      return doAddCluster(clusterName, location, true, parameters);

    } catch (Exception e) {
      OLogManager.instance().exception("Error in creation of new cluster '" + clusterName + "' of type: " + clusterType, e,
          OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();
    }

    return -1;
  }

  public int addCluster(String clusterType, String clusterName, int requestedId, String location, String dataSegmentName,
      boolean forceListBased, Object... parameters) {

    lock.acquireExclusiveLock();
    try {
      if (requestedId < 0) {
        throw new OConfigurationException("Cluster id must be positive!");
      }
      if (requestedId < clusters.size() && clusters.get(requestedId) != null) {
        throw new OConfigurationException("Requested cluster ID [" + requestedId + "] is occupied by cluster with name ["
            + clusters.get(requestedId).getName() + "]");
      }

      dirtyFlag.makeDirty();
      return addClusterInternal(clusterName, requestedId, location, true, parameters);

    } catch (Exception e) {
      OLogManager.instance().exception("Error in creation of new cluster '" + clusterName + "' of type: " + clusterType, e,
							OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();
    }

    return -1;
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    lock.acquireExclusiveLock();
    try {

      if (iClusterId < 0 || iClusterId >= clusters.size())
        throw new IllegalArgumentException("Cluster id '" + iClusterId + "' is outside the of range of configured clusters (0-"
            + (clusters.size() - 1) + ") in database '" + name + "'");

      final OCluster cluster = clusters.get(iClusterId);
      if (cluster == null)
        return false;

      getLevel2Cache().freeCluster(iClusterId);

      if (iTruncate)
        cluster.truncate();
      cluster.delete();

      dirtyFlag.makeDirty();
      clusterMap.remove(cluster.getName().toLowerCase());
      clusters.set(iClusterId, null);

      // UPDATE CONFIGURATION
      configuration.dropCluster(iClusterId);

      makeFullCheckpoint();
      return true;
    } catch (Exception e) {
      OLogManager.instance().exception("Error while removing cluster '" + iClusterId + "'", e, OStorageException.class);

    } finally {
      lock.releaseExclusiveLock();
    }

    return false;
  }

  public boolean dropDataSegment(final String iName) {
    throw new UnsupportedOperationException("dropDataSegment");
  }

  public long count(final int iClusterId) {
    return count(iClusterId, false);
  }

  @Override
  public long count(int clusterId, boolean countTombstones) {
    if (clusterId == -1)
      throw new OStorageException("Cluster Id " + clusterId + " is invalid in database '" + name + "'");

    // COUNT PHYSICAL CLUSTER IF ANY
    checkOpeness();

    final OCluster cluster = clusters.get(clusterId);
    if (cluster == null)
      return 0;

    if (countTombstones)
      return cluster.getEntries();

    return cluster.getEntries() - cluster.getTombstonesCount();
  }

  public OClusterPosition[] getClusterDataRange(final int iClusterId) {
    if (iClusterId == -1)
      return new OClusterPosition[] { OClusterPosition.INVALID_POSITION, OClusterPosition.INVALID_POSITION };

    checkOpeness();
    try {
      return clusters.get(iClusterId) != null ? new OClusterPosition[] { clusters.get(iClusterId).getFirstPosition(),
          clusters.get(iClusterId).getLastPosition() } : new OClusterPosition[0];

    } catch (IOException ioe) {
      throw new OStorageException("Can not retrieve information about data range", ioe);
    }
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    checkOpeness();

    long tot = 0;

    for (int iClusterId : iClusterIds) {
      if (iClusterId >= clusters.size())
        throw new OConfigurationException("Cluster id " + iClusterId + " was not found in database '" + name + "'");

      if (iClusterId > -1) {
        final OCluster c = clusters.get(iClusterId);
        if (c != null)
          tot += c.getEntries() - (countTombstones ? 0L : c.getTombstonesCount());
      }
    }

    return tot;
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final int dataSegmentId, final ORecordId rid,
      final byte[] content, ORecordVersion recordVersion, final byte recordType, final int mode,
      final ORecordCallback<OClusterPosition> callback) {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    final OCluster cluster = getClusterById(rid.clusterId);
    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        checkOpeness();

        if (content == null)
          throw new IllegalArgumentException("Record is null");

        OPhysicalPosition ppos = new OPhysicalPosition(-1, -1, recordType);
        try {
          lock.acquireSharedLock();
          try {
            if (recordVersion.getCounter() > -1)
              recordVersion.increment();
            else
              recordVersion = OVersionFactory.instance().createVersion();

            dirtyFlag.makeDirty();
            atomicOperationsManager.startAtomicOperation();
            try {
              ppos = cluster.createRecord(content, recordVersion, recordType);
              rid.clusterPosition = ppos.clusterPosition;

              final ORecordSerializationContext context = ORecordSerializationContext.getContext();
              if (context != null)
                context.executeOperations(this);
              atomicOperationsManager.endAtomicOperation(false);
            } catch (RuntimeException e) {
              atomicOperationsManager.endAtomicOperation(true);
              throw e;
            }

            if (callback != null)
              callback.call(rid, ppos.clusterPosition);

            return new OStorageOperationResult<OPhysicalPosition>(ppos);
          } finally {
            lock.releaseSharedLock();
          }
        } catch (IOException ioe) {
          try {
            if (ppos.clusterPosition != null && ppos.clusterPosition.compareTo(OClusterPosition.INVALID_POSITION) != 0)
              cluster.deleteRecord(ppos.clusterPosition);
          } catch (IOException e) {
            OLogManager.instance().error(this, "Error on removing record in cluster: " + cluster, e);
          }

          OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, ioe);
          return null;
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer, "db.*.createRecord");
    }
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    if (rid.isNew())
      throw new OStorageException("Passed record with id " + rid + " is new and can not be stored.");

    checkOpeness();

    final OCluster cluster = getClusterById(rid.getClusterId());
    lock.acquireSharedLock();
    try {
      lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.SHARED);
      try {
        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
        if (ppos == null)
          return null;

        return new ORecordMetadata(rid, ppos.recordVersion);
      } finally {
        lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.SHARED);
      }
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
    } finally {
      lock.releaseSharedLock();
    }

    return null;
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones, LOCKING_STRATEGY iLockingStrategy) {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecord(getClusterById(iRid.clusterId), iRid, true, loadTombstones,
        iLockingStrategy));
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId rid, final byte[] content,
      final ORecordVersion version, final byte recordType, final int mode, ORecordCallback<ORecordVersion> callback) {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    final OCluster cluster = getClusterById(rid.clusterId);

    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        lock.acquireSharedLock();
        try {
          // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
          lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
          try {
            // UPDATE IT
            final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
            if (!checkForRecordValidity(ppos)) {
              final ORecordVersion recordVersion = OVersionFactory.instance().createUntrackedVersion();
              if (callback != null)
                callback.call(rid, recordVersion);

              return new OStorageOperationResult<ORecordVersion>(recordVersion);
            }

            // VERSION CONTROL CHECK
            switch (version.getCounter()) {
            // DOCUMENT UPDATE, NO VERSION CONTROL
            case -1:
              ppos.recordVersion.increment();
              break;

            // DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION UPDATE
            case -2:
              ppos.recordVersion.setCounter(-2);
              break;

            default:
              // MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
              // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
              if (!version.equals(ppos.recordVersion))
                if (OFastConcurrentModificationException.enabled())
                  throw OFastConcurrentModificationException.instance();
                else
                  throw new OConcurrentModificationException(rid, ppos.recordVersion, version, ORecordOperation.UPDATED);
              ppos.recordVersion.increment();
            }

            dirtyFlag.makeDirty();
            atomicOperationsManager.startAtomicOperation();
            try {
              cluster.updateRecord(rid.clusterPosition, content, ppos.recordVersion, recordType);

              final ORecordSerializationContext context = ORecordSerializationContext.getContext();
              if (context != null)
                context.executeOperations(this);
              atomicOperationsManager.endAtomicOperation(false);
            } catch (RuntimeException e) {
              atomicOperationsManager.endAtomicOperation(true);
              throw e;
            }

            if (callback != null)
              callback.call(rid, ppos.recordVersion);

            return new OStorageOperationResult<ORecordVersion>(ppos.recordVersion);

          } finally {
            lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Error on updating record " + rid + " (cluster: " + cluster + ")", e);

          final ORecordVersion recordVersion = OVersionFactory.instance().createUntrackedVersion();
          if (callback != null)
            callback.call(rid, recordVersion);

          return new OStorageOperationResult<ORecordVersion>(recordVersion);
        } finally {
          lock.releaseSharedLock();
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer, "db.*.updateRecord");
    }
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId rid, final ORecordVersion version, final int mode,
      ORecordCallback<Boolean> callback) {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    final OCluster cluster = getClusterById(rid.clusterId);

    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        lock.acquireSharedLock();
        try {
          lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
          try {
            final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));

            if (ppos == null)
              // ALREADY DELETED
              return new OStorageOperationResult<Boolean>(false);

            // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
            if (version.getCounter() > -1 && !ppos.recordVersion.equals(version))
              if (OFastConcurrentModificationException.enabled())
                throw OFastConcurrentModificationException.instance();
              else
                throw new OConcurrentModificationException(rid, ppos.recordVersion, version, ORecordOperation.DELETED);

            dirtyFlag.makeDirty();
            atomicOperationsManager.startAtomicOperation();
            try {
              final ORecordSerializationContext context = ORecordSerializationContext.getContext();
              if (context != null)
                context.executeOperations(this);

              cluster.deleteRecord(ppos.clusterPosition);
              atomicOperationsManager.endAtomicOperation(false);
            } catch (RuntimeException e) {
              atomicOperationsManager.endAtomicOperation(true);
            }

            return new OStorageOperationResult<Boolean>(true);
          } finally {
            lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
          }
        } finally {
          lock.releaseSharedLock();
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
    }

    return new OStorageOperationResult<Boolean>(false);
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(final ORecordId rid, final int mode, ORecordCallback<Boolean> callback) {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    final OCluster cluster = getClusterById(rid.clusterId);

    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        lock.acquireSharedLock();
        try {
          lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
          try {
            final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));

            if (ppos == null)
              // ALREADY HIDDEN
              return new OStorageOperationResult<Boolean>(false);

            dirtyFlag.makeDirty();
            atomicOperationsManager.startAtomicOperation();
            try {
              final ORecordSerializationContext context = ORecordSerializationContext.getContext();
              if (context != null)
                context.executeOperations(this);

              cluster.hideRecord(ppos.clusterPosition);
              atomicOperationsManager.endAtomicOperation(false);
            } catch (RuntimeException e) {
              atomicOperationsManager.endAtomicOperation(true);
            }

            return new OStorageOperationResult<Boolean>(true);
          } finally {
            lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
          }
        } finally {
          lock.releaseSharedLock();
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
    }

    return new OStorageOperationResult<Boolean>(false);
  }

  public boolean updateReplica(final int dataSegmentId, final ORecordId rid, final byte[] content,
      final ORecordVersion recordVersion, final byte recordType) throws IOException {
    throw new OStorageException("Support of hash based clusters is required.");
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    if (iExclusiveLock) {
      modificationLock.requestModificationLock();
      try {
        return super.callInLock(iCallable, true);
      } finally {
        modificationLock.releaseModificationLock();
      }
    } else {
      return super.callInLock(iCallable, false);
    }
  }

  @Override
  public <V> V callInRecordLock(Callable<V> callable, ORID rid, boolean exclusiveLock) {
    if (exclusiveLock)
      modificationLock.requestModificationLock();

    try {
      if (exclusiveLock) {
        lock.acquireExclusiveLock();
      } else
        lock.acquireSharedLock();
      try {
        lockManager
            .acquireLock(Thread.currentThread(), rid, exclusiveLock ? OLockManager.LOCK.EXCLUSIVE : OLockManager.LOCK.SHARED);
        try {
          return callable.call();
        } finally {
          lockManager.releaseLock(Thread.currentThread(), rid, exclusiveLock ? OLockManager.LOCK.EXCLUSIVE
              : OLockManager.LOCK.SHARED);
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new OException("Error on nested call in lock", e);
      } finally {
        if (exclusiveLock) {
          lock.releaseExclusiveLock();
        } else
          lock.releaseSharedLock();
      }
    } finally {
      if (exclusiveLock)
        modificationLock.releaseModificationLock();
    }
  }

  public Set<String> getClusterNames() {
    checkOpeness();
    return new HashSet<String>(clusterMap.keySet());
  }

  public int getClusterIdByName(final String сlusterName) {
    checkOpeness();

    if (сlusterName == null)
      throw new IllegalArgumentException("Cluster name is null");

    if (сlusterName.length() == 0)
      throw new IllegalArgumentException("Cluster name is empty");

    if (Character.isDigit(сlusterName.charAt(0)))
      return Integer.parseInt(сlusterName);

    // SEARCH IT BETWEEN PHYSICAL CLUSTERS

    final OCluster segment = clusterMap.get(сlusterName.toLowerCase());
    if (segment != null)
      return segment.getId();

    return -1;
  }

  public String getClusterTypeByName(final String clusterName) {
    checkOpeness();

    if (clusterName == null)
      throw new IllegalArgumentException("Cluster name is null");

    // SEARCH IT BETWEEN PHYSICAL CLUSTERS
    final OCluster segment = clusterMap.get(clusterName.toLowerCase());
    if (segment != null)
      return segment.getType();

    return null;
  }

  public void commit(final OTransaction clientTx, Runnable callback) {
    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        if (writeAheadLog == null)
          throw new OStorageException("WAL mode is not active. Transactions are not supported in given mode");

        dirtyFlag.makeDirty();
        startStorageTx(clientTx);

        final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

        while (clientTx.getCurrentRecordEntries().iterator().hasNext()) {
          for (ORecordOperation txEntry : clientTx.getCurrentRecordEntries())
            tmpEntries.add(txEntry);

          clientTx.clearRecordEntries();

          for (ORecordOperation txEntry : tmpEntries)
            // COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
            commitEntry(clientTx, txEntry);
        }

        if (callback != null)
          callback.run();

        endStorageTx();

        OTransactionAbstract.updateCacheFromEntries(clientTx, clientTx.getAllRecordEntries(), false);

      } catch (Exception e) {
        // WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
        OLogManager.instance().debug(this, "Error during transaction commit, transaction will be rolled back (tx-id=%d)", e,
            clientTx.getId());
        rollback(clientTx);
        if (e instanceof OException)
          throw ((OException) e);
        else
          throw new OStorageException("Error during transaction commit.", e);
      } finally {
        transaction.set(null);
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public void rollback(final OTransaction clientTx) {
    checkOpeness();
    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        if (transaction.get() == null)
          return;

        if (writeAheadLog == null)
          throw new OStorageException("WAL mode is not active. Transactions are not supported in given mode");

        if (transaction.get().getClientTx().getId() != clientTx.getId())
          throw new OStorageException(
              "Passed in and active transaction are different transactions. Passed in transaction can not be rolled back.");

        dirtyFlag.makeDirty();
        rollbackStorageTx();

        OTransactionAbstract.updateCacheFromEntries(clientTx, clientTx.getAllRecordEntries(), false);

      } catch (IOException e) {
        throw new OStorageException("Error during transaction rollback.", e);
      } finally {
        transaction.set(null);
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return ppos != null && !ppos.recordVersion.isTombstone();
  }

  public void synch() {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();
    modificationLock.prohibitModifications();
    try {
      lock.acquireSharedLock();
      try {
        if (writeAheadLog != null) {
          makeFullCheckpoint();
          return;
        }

        diskCache.flushBuffer();

        if (configuration != null)
          configuration.synch();

        dirtyFlag.clearDirty();
      } catch (IOException e) {
        throw new OStorageException("Error on synch storage '" + name + "'", e);

      } finally {
        lock.releaseSharedLock();

        Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
      }
    } finally {
      modificationLock.allowModifications();
    }
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    checkOpeness();

    if (iClusterId >= clusters.size())
      return null;

    return clusters.get(iClusterId) != null ? clusters.get(iClusterId).getName() : null;
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  public OCluster getClusterById(int iClusterId) {
    if (iClusterId == ORID.CLUSTER_ID_INVALID)
      // GET THE DEFAULT CLUSTER
      iClusterId = defaultClusterId;

    checkClusterSegmentIndexRange(iClusterId);

    final OCluster cluster = clusters.get(iClusterId);
    if (cluster == null)
      throw new IllegalArgumentException("Cluster " + iClusterId + " is null");

    return cluster;
  }

  @Override
  public OCluster getClusterByName(final String сlusterName) {
    final OCluster cluster = clusterMap.get(сlusterName.toLowerCase());

    if (cluster == null)
      throw new IllegalArgumentException("Cluster " + сlusterName + " does not exist in database '" + name + "'");
    return cluster;
  }

  @Override
  public String getURL() {
    return OEngineLocalPaginated.NAME + ":" + url;
  }

  public long getSize() {
    try {

      long size = 0;

      for (OCluster c : clusters)
        if (c != null)
          size += c.getRecordsSize();

      return size;

    } catch (IOException ioe) {
      throw new OStorageException("Can not calculate records size");
    }
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
    return clusterMap.size();
  }

  public Set<OCluster> getClusterInstances() {
    final Set<OCluster> result = new HashSet<OCluster>();

    // ADD ALL THE CLUSTERS
    for (OCluster c : clusters)
      if (c != null)
        result.add(c);

    return result;
  }

  /**
   * Method that completes the cluster rename operation. <strong>IT WILL NOT RENAME A CLUSTER, IT JUST CHANGES THE NAME IN THE
   * INTERNAL MAPPING</strong>
   */
  public void renameCluster(final String oldName, final String newName) {
    clusterMap.put(newName.toLowerCase(), clusterMap.remove(oldName.toLowerCase()));
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, iMode, callback).getResult();
  }

  public void freeze(boolean throwException) {
    modificationLock.prohibitModifications(throwException);
    synch();

    try {
      unlock();

      diskCache.setSoftlyClosed(true);

      if (configuration != null)
        configuration.setSoftlyClosed(true);

    } catch (IOException e) {
      modificationLock.allowModifications();
      try {
        lock();
      } catch (IOException ignored) {
      }
      throw new OStorageException("Error on freeze of storage '" + name + "'", e);
    }
  }

  public void release() {
    try {
      lock();

      diskCache.setSoftlyClosed(false);

      if (configuration != null)
        configuration.setSoftlyClosed(false);

    } catch (IOException e) {
      throw new OStorageException("Error on release of storage '" + name + "'", e);
    }

    modificationLock.allowModifications();
  }

  public void makeFuzzyCheckpoint() {
    // if (writeAheadLog == null)
    // return;
    //
    // try {
    // lock.acquireExclusiveLock();
    // try {
    // writeAheadLog.flush();
    //
    // writeAheadLog.logFuzzyCheckPointStart();
    //
    // diskCache.forceSyncStoredChanges();
    // diskCache.logDirtyPagesTable();
    //
    // writeAheadLog.logFuzzyCheckPointEnd();
    //
    // writeAheadLog.flush();
    // } finally {
    // lock.releaseExclusiveLock();
    // }
    // } catch (IOException ioe) {
    // throw new OStorageException("Error during fuzzy checkpoint creation for storage " + name, ioe);
    // }
  }

  public void makeFullCheckpoint() {
    if (writeAheadLog == null)
      return;

    try {
      modificationLock.prohibitModifications();

      lock.acquireSharedLock();
      try {
        writeAheadLog.flush();

        if (configuration != null)
          configuration.synch();

        final OLogSequenceNumber lastLSN = writeAheadLog.logFullCheckpointStart();

        diskCache.flushBuffer();

        writeAheadLog.logFullCheckpointEnd();
        writeAheadLog.flush();

        writeAheadLog.cutTill(lastLSN);
        dirtyFlag.clearDirty();
      } catch (IOException ioe) {
        throw new OStorageException("Error during checkpoint creation for storage " + name, ioe);
      } finally {
        lock.releaseSharedLock();
      }
    } finally {
      modificationLock.allowModifications();
    }
  }

  public void scheduleFullCheckpoint() {
    if (checkpointExecutor != null)
      checkpointExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            makeFullCheckpoint();
          } catch (Throwable t) {
            OLogManager.instance().error(this, "Error during background checkpoint creation for storage " + name, t);
          }
        }
      });
  }

  @Override
  public String getType() {
    return OEngineLocalPaginated.NAME;
  }

  @Override
  public Class<OSBTreeCollectionManagerShared> getCollectionManagerClass() {
    return OSBTreeCollectionManagerShared.class;
  }

  public ODiskCache getDiskCache() {
    return diskCache;
  }

  public void freeze(boolean throwException, int clusterId) {
    final OCluster cluster = getClusterById(clusterId);

    final String name = cluster.getName();
    if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(name) || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(name)) {
      throw new IllegalArgumentException("It is impossible to freeze and release index or manual index cluster!");
    }

    cluster.getExternalModificationLock().prohibitModifications(throwException);

    try {
      cluster.synch();
      cluster.setSoftlyClosed(true);
    } catch (IOException e) {
      throw new OStorageException("Error on synch cluster '" + name + "'", e);
    }
  }

  public void release(int clusterId) {
    final OCluster cluster = getClusterById(clusterId);

    final String name = cluster.getName();
    if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(name) || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(name)) {
      throw new IllegalArgumentException("It is impossible to freeze and release index or manualindex cluster!");
    }

    try {
      cluster.setSoftlyClosed(false);
    } catch (IOException e) {
      throw new OStorageException("Error on unfreeze storage '" + name + "'", e);
    }

    cluster.getExternalModificationLock().allowModifications();

  }

  /**
   * Locks all the clusters to avoid access outside current process.
   */
  protected void lock() throws IOException {
    OLogManager.instance().debug(this, "Locking storage %s...", name);
    configuration.lock();
    diskCache.lock();
  }

  /**
   * Unlocks all the clusters to allow access outside current process.
   */
  protected void unlock() throws IOException {
    OLogManager.instance().debug(this, "Unlocking storage %s...", name);
    configuration.unlock();
    diskCache.unlock();
  }

  @Override
  protected ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid, boolean atomicLock, boolean loadTombstones,
      LOCKING_STRATEGY iLockingStrategy) {
    checkOpeness();

    if (!rid.isPersistent())
      throw new IllegalArgumentException("Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    final long timer = Orient.instance().getProfiler().startChrono();

    clusterSegment.getExternalModificationLock().requestModificationLock();
    try {
      if (atomicLock)
        lock.acquireSharedLock();

      try {
        switch (iLockingStrategy) {
        case DEFAULT:
        case KEEP_SHARED_LOCK:
          rid.lock(false);
          break;
        case NONE:
          // DO NOTHING
          break;
        case KEEP_EXCLUSIVE_LOCK:
          rid.lock(true);
        }

        try {
          return clusterSegment.readRecord(rid.clusterPosition);
        } finally {
          switch (iLockingStrategy) {
          case DEFAULT:
            rid.unlock();
            break;

          case KEEP_EXCLUSIVE_LOCK:
          case NONE:
          case KEEP_SHARED_LOCK:
            // DO NOTHING
            break;
          }
        }

      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on reading record " + rid + " (cluster: " + clusterSegment + ')', e);
        return null;
      } finally {
        if (atomicLock)
          lock.releaseSharedLock();
      }
    } finally {
      clusterSegment.getExternalModificationLock().releaseModificationLock();

      Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
    }
  }

  @Override
  protected OPhysicalPosition updateRecord(OCluster cluster, ORecordId rid, byte[] recordContent, ORecordVersion recordVersion,
      byte recordType) {
    throw new UnsupportedOperationException("updateRecord");
  }

  @Override
  protected OPhysicalPosition createRecord(ODataLocal dataSegment, OCluster cluster, byte[] recordContent, byte recordType,
      ORecordId rid, ORecordVersion recordVersion) {
    throw new UnsupportedOperationException("createRecord");
  }

  private void initWal() throws IOException {
    if (OGlobalConfiguration.USE_WAL.getValueAsBoolean()) {
      fuzzyCheckpointExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r);
          thread.setDaemon(true);
          return thread;
        }
      });

      checkpointExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r);
          thread.setDaemon(true);
          return thread;
        }
      });

      writeAheadLog = new OWriteAheadLog(this);

      final int fuzzyCheckpointDelay = OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger();
      fuzzyCheckpointExecutor.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            makeFuzzyCheckpoint();
          } catch (Throwable e) {
            OLogManager.instance().error(this, "Error during background FUZZY checkpoint creation for storage " + name, e);
          }

        }
      }, fuzzyCheckpointDelay, fuzzyCheckpointDelay, TimeUnit.SECONDS);
    } else
      writeAheadLog = null;

    long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
    long writeCacheSize = (long) Math.floor((((double) OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0)
        * diskCacheSize);
    long readCacheSize = diskCacheSize - writeCacheSize;

    diskCache = new OReadWriteDiskCache(name, readCacheSize, writeCacheSize,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_TTL.getValueAsLong() * 1000,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL.getValueAsInteger(), this, writeAheadLog, false, true);

    atomicOperationsManager = new OAtomicOperationsManager(writeAheadLog);
  }

  private void restoreIfNeeded() throws Exception {
    if (dirtyFlag.isDirty()) {
      OLogManager.instance().warn(this, "Storage " + name + " was not closed properly. Will try to restore from write ahead log.");
      try {
        restoreFromWAL();
        makeFullCheckpoint();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during storage data restore.", e);
        throw e;
      }

      OLogManager.instance().info(this, "Storage data restore was completed");
    }
  }

  private void restoreFromWAL() throws IOException {
    if (writeAheadLog == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead logging is switched off.");
      return;
    }

    if (writeAheadLog.begin() == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead log is empty.");
      return;
    }

    OLogManager.instance().info(this, "Looking for last checkpoint...");

    OLogSequenceNumber lastCheckPoint;
    try {
      lastCheckPoint = writeAheadLog.getLastCheckpoint();
    } catch (OWALPageBrokenException e) {
      lastCheckPoint = null;
    }

    if (lastCheckPoint == null) {
      OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
      restoreFromBegging();
      return;
    }

    OWALRecord checkPointRecord;
    try {
      checkPointRecord = writeAheadLog.read(lastCheckPoint);
    } catch (OWALPageBrokenException e) {
      checkPointRecord = null;
    }

    if (checkPointRecord == null) {
      OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
      restoreFromBegging();
      return;
    }

    if (checkPointRecord instanceof OFuzzyCheckpointStartRecord) {
      OLogManager.instance().info(this, "Found FUZZY checkpoint.");

      boolean fuzzyCheckPointIsComplete = checkFuzzyCheckPointIsComplete(lastCheckPoint);
      if (!fuzzyCheckPointIsComplete) {
        OLogManager.instance().warn(this, "FUZZY checkpoint is not complete.");

        OLogSequenceNumber previousCheckpoint = ((OFuzzyCheckpointStartRecord) checkPointRecord).getPreviousCheckpoint();
        checkPointRecord = null;

        if (previousCheckpoint != null)
          checkPointRecord = writeAheadLog.read(previousCheckpoint);

        if (checkPointRecord != null) {
          OLogManager.instance().warn(this, "Restore will start from the previous checkpoint.");
          restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
        } else {
          OLogManager.instance().warn(this, "Restore will start from the beginning.");
          restoreFromBegging();
        }
      } else
        restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);

      return;
    }

    if (checkPointRecord instanceof OFullCheckpointStartRecord) {
      OLogManager.instance().info(this, "FULL checkpoint found.");
      boolean fullCheckPointIsComplete = checkFullCheckPointIsComplete(lastCheckPoint);
      if (!fullCheckPointIsComplete) {
        OLogManager.instance().warn(this, "FULL checkpoint has not completed.");

        OLogSequenceNumber previousCheckpoint = ((OFullCheckpointStartRecord) checkPointRecord).getPreviousCheckpoint();
        checkPointRecord = null;
        if (previousCheckpoint != null)
          checkPointRecord = writeAheadLog.read(previousCheckpoint);

        if (checkPointRecord != null) {
          OLogManager.instance().warn(this, "Restore will start from the previous checkpoint.");

        } else {
          OLogManager.instance().warn(this, "Restore will start from the beginning.");
          restoreFromBegging();
        }
      } else
        restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);

      return;
    }

    throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());

  }

  private boolean checkFullCheckPointIsComplete(OLogSequenceNumber lastCheckPoint) throws IOException {
    try {
      OLogSequenceNumber lsn = writeAheadLog.next(lastCheckPoint);

      while (lsn != null) {
        OWALRecord walRecord = writeAheadLog.read(lsn);
        if (walRecord instanceof OCheckpointEndRecord)
          return true;

        lsn = writeAheadLog.next(lsn);
      }
    } catch (OWALPageBrokenException e) {
      return false;
    }

    return false;
  }

  private boolean checkFuzzyCheckPointIsComplete(OLogSequenceNumber lastCheckPoint) throws IOException {
    try {
      OLogSequenceNumber lsn = writeAheadLog.next(lastCheckPoint);

      while (lsn != null) {
        OWALRecord walRecord = writeAheadLog.read(lsn);
        if (walRecord instanceof OFuzzyCheckpointEndRecord)
          return true;

        lsn = writeAheadLog.next(lsn);
      }
    } catch (OWALPageBrokenException e) {
      return false;
    }

    return false;
  }

  private void restoreFromCheckPoint(OAbstractCheckPointStartRecord checkPointRecord) throws IOException {
    if (checkPointRecord instanceof OFuzzyCheckpointStartRecord) {
      restoreFromFuzzyCheckPoint((OFuzzyCheckpointStartRecord) checkPointRecord);
      return;
    }

    if (checkPointRecord instanceof OFullCheckpointStartRecord) {
      restoreFromFullCheckPoint((OFullCheckpointStartRecord) checkPointRecord);
      return;
    }

    throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());
  }

  private void restoreFromFullCheckPoint(OFullCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from full checkpoint is started. Restore is performed from LSN %s",
        checkPointRecord.getLsn());

    final OLogSequenceNumber lsn = writeAheadLog.next(checkPointRecord.getLsn());
    restoreFrom(lsn);
  }

  private void restoreFromFuzzyCheckPoint(OFuzzyCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from FUZZY checkpoint is started.");
    OLogSequenceNumber dirtyPagesLSN = writeAheadLog.next(checkPointRecord.getLsn());
    ODirtyPagesRecord dirtyPagesRecord = (ODirtyPagesRecord) writeAheadLog.read(dirtyPagesLSN);
    OLogSequenceNumber startLSN;

    Set<ODirtyPage> dirtyPages = dirtyPagesRecord.getDirtyPages();
    if (dirtyPages.isEmpty()) {
      startLSN = dirtyPagesLSN;
    } else {
      ODirtyPage[] pages = dirtyPages.toArray(new ODirtyPage[dirtyPages.size()]);

      Arrays.sort(pages, new Comparator<ODirtyPage>() {
        @Override
        public int compare(ODirtyPage pageOne, ODirtyPage pageTwo) {
          return pageOne.getLsn().compareTo(pageTwo.getLsn());
        }
      });

      startLSN = pages[0].getLsn();
    }

    if (startLSN.compareTo(writeAheadLog.begin()) < 0)
      startLSN = writeAheadLog.begin();

    restoreFrom(startLSN);
  }

  private void restoreFromBegging() throws IOException {
    OLogManager.instance().info(this, "Data restore procedure is started.");
    OLogSequenceNumber lsn = writeAheadLog.begin();

    restoreFrom(lsn);
  }

  private void restoreFrom(OLogSequenceNumber lsn) throws IOException {
    wereDataRestoredAfterOpen = true;

    long recordsProcessed = 0;
    int reportInterval = OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();

    final AtomicBoolean lowMemoryFlag = new AtomicBoolean(false);

    OMemoryWatchDog.Listener listener = new OMemoryWatchDog.Listener() {
      @Override
      public void lowMemory(long iFreeMemory, long iFreeMemoryPercentage) {
        lowMemoryFlag.set(true);
      }
    };

    listener = Orient.instance().getMemoryWatchDog().addListener(listener);

    Map<OOperationUnitId, List<OLogSequenceNumber>> operationUnits = new HashMap<OOperationUnitId, List<OLogSequenceNumber>>();
    List<OWALRecord> batch = new ArrayList<OWALRecord>();

    try {
      while (lsn != null) {
        OWALRecord walRecord = writeAheadLog.read(lsn);
        batch.add(walRecord);

        if (lowMemoryFlag.get()) {
          OLogManager.instance().info(this, "Heap memory is low apply batch of operations are read from WAL.");
          recordsProcessed = restoreWALBatch(batch, operationUnits, recordsProcessed, reportInterval);
          batch = new ArrayList<OWALRecord>();
          lowMemoryFlag.set(false);
        }

        lsn = writeAheadLog.next(lsn);
      }

      if (!batch.isEmpty()) {
        OLogManager.instance().info(this, "Apply last batch of operations are read from WAL.");
        restoreWALBatch(batch, operationUnits, recordsProcessed, reportInterval);
        batch = null;
      }
    } catch (OWALPageBrokenException e) {
      OLogManager.instance().error(this,
          "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.");
    }

    rollbackAllUnfinishedWALOperations(operationUnits);
    operationUnits.clear();

    Orient.instance().getMemoryWatchDog().removeListener(listener);
  }

  private long restoreWALBatch(List<OWALRecord> batch, Map<OOperationUnitId, List<OLogSequenceNumber>> operationUnits,
      long recordsProcessed, int reportInterval) throws IOException {
    for (OWALRecord walRecord : batch) {
      final OLogSequenceNumber lsn = walRecord.getLsn();

      if (walRecord instanceof OAtomicUnitStartRecord) {
        List<OLogSequenceNumber> operationList = new ArrayList<OLogSequenceNumber>();
        operationUnits.put(((OAtomicUnitStartRecord) walRecord).getOperationUnitId(), operationList);
        operationList.add(lsn);
      } else if (walRecord instanceof OOperationUnitRecord) {
        OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) walRecord;
        OOperationUnitId unitId = operationUnitRecord.getOperationUnitId();
        List<OLogSequenceNumber> records = operationUnits.get(unitId);

        assert records != null;

        records.add(lsn);

        if (operationUnitRecord instanceof OUpdatePageRecord) {
          final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) operationUnitRecord;

          final long fileId = updatePageRecord.getFileId();
          final long pageIndex = updatePageRecord.getPageIndex();

          if (!diskCache.isOpen(fileId))
            diskCache.openFile(fileId);

          final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, true);
          final OCachePointer cachePointer = cacheEntry.getCachePointer();
          cachePointer.acquireExclusiveLock();
          try {
            ODurablePage durablePage = new ODurablePage(cachePointer.getDataPointer(), ODurablePage.TrackMode.NONE);
            durablePage.restoreChanges(updatePageRecord.getChanges());
            durablePage.setLsn(lsn);

            cacheEntry.markDirty();
          } finally {
            cachePointer.releaseExclusiveLock();
            diskCache.release(cacheEntry);
          }

        } else if (operationUnitRecord instanceof OFileCreatedCreatedWALRecord) {

          final OFileCreatedCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedCreatedWALRecord) operationUnitRecord;
          diskCache.openFile(fileCreatedCreatedRecord.getFileName(), fileCreatedCreatedRecord.getFileId());

        } else if (operationUnitRecord instanceof OAtomicUnitEndRecord) {
          final OAtomicUnitEndRecord atomicUnitEndRecord = (OAtomicUnitEndRecord) walRecord;

          if (atomicUnitEndRecord.isRollback())
            undoOperation(records);

          operationUnits.remove(unitId);
        } else {
          OLogManager.instance().error(this, "Invalid WAL record type was passed %s. Given record will be skipped.",
              operationUnitRecord.getClass());
          assert false : "Invalid WAL record type was passed " + operationUnitRecord.getClass().getName();
        }
      } else
        OLogManager.instance().warn(this, "Record %s will be skipped during data restore.", walRecord);

      recordsProcessed++;
      if (reportInterval > 0 && recordsProcessed % reportInterval == 0)
        OLogManager.instance().info(this, "%d operations were processed, current LSN is %s last LSN is %s", recordsProcessed, lsn,
            writeAheadLog.end());
    }

    return recordsProcessed;
  }

  private void rollbackAllUnfinishedWALOperations(Map<OOperationUnitId, List<OLogSequenceNumber>> operationUnits)
      throws IOException {
    for (List<OLogSequenceNumber> operationUnit : operationUnits.values()) {
      if (operationUnit.isEmpty())
        continue;

      final OAtomicUnitStartRecord atomicUnitStartRecord = (OAtomicUnitStartRecord) writeAheadLog.read(operationUnit.get(0));
      if (!atomicUnitStartRecord.isRollbackSupported())
        continue;

      final OAtomicUnitEndRecord atomicUnitEndRecord = new OAtomicUnitEndRecord(atomicUnitStartRecord.getOperationUnitId(), true);
      final OLogSequenceNumber logSequenceNumber = writeAheadLog.log(atomicUnitEndRecord);

      operationUnit.add(logSequenceNumber);

      undoOperation(operationUnit);
    }
  }

  private boolean exists(String path) {
    return new File(path + "/" + OMetadataDefault.CLUSTER_INTERNAL_NAME + OPaginatedCluster.DEF_EXTENSION).exists();
  }

  private void doClose(boolean force, boolean onDelete) {
    if (status == STATUS.CLOSED)
      return;

    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {
      if (!checkForClose(force))
        return;

      status = STATUS.CLOSING;

      if (!onDelete)
        makeFullCheckpoint();

      if (writeAheadLog != null) {
        fuzzyCheckpointExecutor.shutdown();
        if (!fuzzyCheckpointExecutor.awaitTermination(
            OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.SECONDS))
          throw new OStorageException("Can not terminate fuzzy checkpoint task");

        checkpointExecutor.shutdown();
        if (!checkpointExecutor.awaitTermination(OGlobalConfiguration.WAL_FULL_CHECKPOINT_SHUTDOWN_TIMEOUT.getValueAsInteger(),
            TimeUnit.SECONDS))
          throw new OStorageException("Can not terminate full checkpoint task");
      }

      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.close(!onDelete);

      clusters.clear();
      clusterMap.clear();

      if (configuration != null)
        configuration.close();

      level2Cache.shutdown();

      super.close(force, onDelete);

      if (!onDelete)
        diskCache.close();
      else
        diskCache.delete();

      if (writeAheadLog != null && onDelete)
        writeAheadLog.delete();

      if (onDelete)
        dirtyFlag.delete();
      else {
        dirtyFlag.clearDirty();
        dirtyFlag.close();
      }

      status = STATUS.CLOSED;
    } catch (InterruptedException ie) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, ie, OStorageException.class);
      Thread.interrupted();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, e, OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
    }
  }

  private int doAddCluster(String clusterName, String location, boolean fullCheckPoint, Object[] parameters) throws IOException {
    // FIND THE FIRST AVAILABLE CLUSTER ID
    int clusterPos = clusters.size();
    for (int i = 0; i < clusters.size(); ++i) {
      if (clusters.get(i) == null) {
        clusterPos = i;
        break;
      }
    }

    return addClusterInternal(clusterName, clusterPos, location, fullCheckPoint, parameters);
  }

  private int addClusterInternal(String clusterName, int clusterPos, String location, boolean fullCheckPoint, Object... parameters)
      throws IOException {

    final OCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase();

      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(configuration.version);
      cluster.configure(this, clusterPos, clusterName, location, -1, parameters);

      if (clusterName.equals(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME.toLowerCase())) {
        cluster.set(OCluster.ATTRIBUTES.USE_WAL, false);
        cluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 5);
        cluster.set(OCluster.ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR, 2);
      }

    } else {
      cluster = null;
    }

    final int createdClusterId = registerCluster(cluster);

    if (cluster != null) {
      if (!cluster.exists()) {
        cluster.create(-1);
        if (makeFullCheckPointAfterClusterCreate && fullCheckPoint)
          makeFullCheckpoint();
      } else {
        cluster.open();
      }

      configuration.update();
    }

    return createdClusterId;
  }

  private void commitEntry(final OTransaction clientTx, final ORecordOperation txEntry) throws IOException {

    if (txEntry.type != ORecordOperation.DELETED && !txEntry.getRecord().isDirty())
      return;

    final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

    ORecordSerializationContext.pushContext();
    try {
      if (rid.clusterId == ORID.CLUSTER_ID_INVALID && txEntry.getRecord() instanceof ODocument
          && ((ODocument) txEntry.getRecord()).getSchemaClass() != null) {
        // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS
        rid.clusterId = ((ODocument) txEntry.getRecord()).getSchemaClass().getDefaultClusterId();
      }

      final OCluster cluster = getClusterById(rid.clusterId);

      if (cluster.getName().equals(OMetadataDefault.CLUSTER_INDEX_NAME)
          || cluster.getName().equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME))
        // AVOID TO COMMIT INDEX STUFF
        return;

      if (txEntry.getRecord() instanceof OTxListener)
        ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

      switch (txEntry.type) {
      case ORecordOperation.LOADED:
        break;

      case ORecordOperation.CREATED: {
        // CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD

        byte[] stream = txEntry.getRecord().toStream();
        if (stream == null) {
          OLogManager.instance().warn(this, "Null serialization on committing new record %s in transaction", rid);
          break;
        }

        final ORecordId oldRID = rid.isNew() ? rid.copy() : rid;

        if (rid.isNew()) {
          rid.clusterId = cluster.getId();
          final OPhysicalPosition ppos;
          ppos = createRecord(-1, rid, stream, txEntry.getRecord().getRecordVersion(), txEntry.getRecord().getRecordType(), -1,
              null).getResult();

          rid.clusterPosition = ppos.clusterPosition;
          txEntry.getRecord().getRecordVersion().copyFrom(ppos.recordVersion);

          clientTx.updateIdentityAfterCommit(oldRID, rid);
        } else {
          txEntry
              .getRecord()
              .getRecordVersion()
              .copyFrom(
                  updateRecord(rid, stream, txEntry.getRecord().getRecordVersion(), txEntry.getRecord().getRecordType(), -1, null)
                      .getResult());
        }

        break;
      }

      case ORecordOperation.UPDATED: {
        byte[] stream = txEntry.getRecord().toStream();
        if (stream == null) {
          OLogManager.instance().warn(this, "Null serialization on committing updated record %s in transaction", rid);
          break;
        }

        txEntry
            .getRecord()
            .getRecordVersion()
            .copyFrom(
                updateRecord(rid, stream, txEntry.getRecord().getRecordVersion(), txEntry.getRecord().getRecordType(), -1, null)
                    .getResult());

        break;
      }

      case ORecordOperation.DELETED: {
        deleteRecord(rid, txEntry.getRecord().getRecordVersion(), -1, null);
        break;
      }
      }
    } finally {
      ORecordSerializationContext.pullContext();
    }

    txEntry.getRecord().unsetDirty();

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId > clusters.size() - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration config) throws IOException {
    OCluster cluster = clusterMap.get(config.getName().toLowerCase());

    if (cluster != null) {
      cluster.configure(this, config);
      return -1;
    }

    cluster = OPaginatedClusterFactory.INSTANCE.createCluster(configuration.version);
    cluster.configure(this, config);

    return registerCluster(cluster);
  }

  /**
   * Register the cluster internally.
   * 
   * @param cluster
   *          OCluster implementation
   * @return The id (physical position into the array) of the new cluster just created. First is 0.
   * @throws IOException
   */
  private int registerCluster(final OCluster cluster) throws IOException {
    final int id;

    if (cluster != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (clusterMap.containsKey(cluster.getName().toLowerCase()))
        throw new OConfigurationException("Cannot add segment '" + cluster.getName()
            + "' because it is already registered in database '" + name + "'");
      // CREATE AND ADD THE NEW REF SEGMENT
      clusterMap.put(cluster.getName().toLowerCase(), cluster);
      id = cluster.getId();
    } else {
      id = clusters.size();
    }

    setCluster(id, cluster);

    return id;
  }

  private void addDefaultClusters() throws IOException {
    final String storageCompression = OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getValueAsString();
    createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        OMetadataDefault.CLUSTER_INTERNAL_NAME, null, true, 20, 4, storageCompression));

    createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        OMetadataDefault.CLUSTER_INDEX_NAME, null, false, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
        OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, storageCompression));

    createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null, false, 1, 1, storageCompression));

    defaultClusterId = createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        CLUSTER_DEFAULT_NAME, null, true, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
        OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, storageCompression));
  }
}
