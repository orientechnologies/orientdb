/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OLowDiskSpaceException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTxListener;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public abstract class OAbstractPaginatedStorage extends OStorageAbstract implements OLowDiskSpaceListener,
    OFullCheckpointRequestListener {
  private static final int                                    RECORD_LOCK_TIMEOUT                        = OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT
                                                                                                             .getValueAsInteger();

  private final OLockManager<ORID, OAbstractPaginatedStorage> lockManager;
  private final String                                        PROFILER_CREATE_RECORD;
  private final String                                        PROFILER_READ_RECORD;
  private final String                                        PROFILER_UPDATE_RECORD;
  private final String                                        PROFILER_DELETE_RECORD;
  private final ConcurrentMap<String, OCluster>               clusterMap                                 = new ConcurrentHashMap<String, OCluster>();
  private final ThreadLocal<OStorageTransaction>              transaction                                = new ThreadLocal<OStorageTransaction>();
  private final OModificationLock                             modificationLock                           = new OModificationLock();
  private final AtomicBoolean                                 checkpointInProgress                       = new AtomicBoolean();
  protected volatile OWriteAheadLog                           writeAheadLog;
  protected volatile ODiskCache                               diskCache;
  private ORecordConflictStrategy                             recordConflictStrategy                     = Orient
                                                                                                             .instance()
                                                                                                             .getRecordConflictStrategy()
                                                                                                             .newInstanceOfDefaultClass();
  private CopyOnWriteArrayList<OCluster>                      clusters                                   = new CopyOnWriteArrayList<OCluster>();
  private volatile int                                        defaultClusterId                           = -1;
  private volatile OAtomicOperationsManager                   atomicOperationsManager;
  private volatile boolean                                    wereDataRestoredAfterOpen                  = false;
  private volatile boolean                                    wereNonTxOperationsPerformedInPreviousOpen = false;
  private boolean                                             makeFullCheckPointAfterClusterCreate       = OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CLUSTER_CREATE
                                                                                                             .getValueAsBoolean();
  private volatile OLowDiskSpaceInformation                   lowDiskSpace                               = null;
  private volatile boolean                                    checkpointRequest                          = false;

  public OAbstractPaginatedStorage(String name, String filePath, String mode) {
    super(name, filePath, mode, OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsInteger());

    lockManager = new OLockManager<ORID, OAbstractPaginatedStorage>(true, -1) {
      @Override
      protected ORID getImmutableResourceId(ORID iResourceId) {
        return new ORecordId(iResourceId);
      }
    };

    PROFILER_CREATE_RECORD = "db." + this.name + ".createRecord";
    PROFILER_READ_RECORD = "db." + this.name + ".readRecord";
    PROFILER_UPDATE_RECORD = "db." + this.name + ".updateRecord";
    PROFILER_DELETE_RECORD = "db." + this.name + ".deleteRecord";
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
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

      if (!exists())
        throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);

      configuration.load();
      componentsFactory = new OCurrentStorageComponentsFactory(configuration);

      preOpenSteps();

      initWalAndDiskCache();

      atomicOperationsManager = new OAtomicOperationsManager(this);

      restoreIfNeeded();

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

      if (OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_OPEN.getValueAsBoolean())
        makeFullCheckpoint();

      diskCache.startFuzzyCheckpoints();

      status = STATUS.OPEN;
    } catch (Exception e) {
      status = STATUS.CLOSED;
      throw new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode, e);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void open(final OToken iToken, final Map<String, Object> iProperties) {
    open(iToken.getUserName(), "", iProperties);
  }

  public void create(final Map<String, Object> iProperties) {
    lock.acquireExclusiveLock();
    try {

      if (status != STATUS.CLOSED)
        throw new OStorageException("Cannot create new storage '" + name + "' because it is not closed");

      if (exists())
        throw new OStorageException("Cannot create new storage '" + name + "' because it already exists");

      if (!configuration.getContextConfiguration().getContextKeys()
          .contains(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey()))

        // SAVE COMPRESSION IN STORAGE CFG
        configuration.getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD,
            OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getValue());

      componentsFactory = new OCurrentStorageComponentsFactory(configuration);
      initWalAndDiskCache();

      atomicOperationsManager = new OAtomicOperationsManager(this);

      preCreateSteps();

      status = STATUS.OPEN;

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      doAddCluster(OMetadataDefault.CLUSTER_INTERNAL_NAME, false, null);

      configuration.create();

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      doAddCluster(OMetadataDefault.CLUSTER_INDEX_NAME, false, null);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      doAddCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, false, null);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = doAddCluster(CLUSTER_DEFAULT_NAME, false, null);

      clearStorageDirty();
      if (OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE.getValueAsBoolean())
        makeFullCheckpoint();

      diskCache.startFuzzyCheckpoints();
      postCreateSteps();

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

  public void startAtomicOperation() throws IOException {
    lock.acquireSharedLock();
    try {
      makeStorageDirty();

      atomicOperationsManager.startAtomicOperation(null);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public void commitAtomicOperation() throws IOException {
    lock.acquireSharedLock();
    try {
      atomicOperationsManager.endAtomicOperation(false);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public void rollbackAtomicOperation() throws IOException {
    lock.acquireSharedLock();
    try {
      atomicOperationsManager.endAtomicOperation(true);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public void markDirty() throws IOException {
    makeStorageDirty();
  }

  @Override
  public void close(final boolean force, boolean onDelete) {
    doClose(force, onDelete);
  }

  public void delete() {
    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {
      // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
      doClose(true, true);

      try {
        Orient.instance().unregisterStorage(this);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Cannot unregister storage", e);
      }

      if (writeAheadLog != null)
        writeAheadLog.delete();

      if (diskCache != null)
        diskCache.delete();
      postDeleteSteps();

    } catch (IOException e) {
      throw new OStorageException("Cannot delete database '" + name + "'.", e);
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

  public int addCluster(String clusterName, boolean forceListBased, final Object... parameters) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    lock.acquireExclusiveLock();
    try {

      makeStorageDirty();
      return doAddCluster(clusterName, true, parameters);

    } catch (Exception e) {
      throw new OStorageException("Error in creation of new cluster '" + clusterName, e);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public int addCluster(String clusterName, int requestedId, boolean forceListBased, Object... parameters) {
    checkLowDiskSpaceAndFullCheckpointRequests();

    lock.acquireExclusiveLock();
    try {
      if (requestedId < 0) {
        throw new OConfigurationException("Cluster id must be positive!");
      }
      if (requestedId < clusters.size() && clusters.get(requestedId) != null) {
        throw new OConfigurationException("Requested cluster ID [" + requestedId + "] is occupied by cluster with name ["
            + clusters.get(requestedId).getName() + "]");
      }

      makeStorageDirty();
      return addClusterInternal(clusterName, requestedId, true, parameters);

    } catch (Exception e) {
      throw new OStorageException("Error in creation of new cluster '" + clusterName + "'", e);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean dropCluster(final int clusterId, final boolean iTruncate) {
    checkLowDiskSpaceAndFullCheckpointRequests();

    lock.acquireExclusiveLock();
    try {

      if (clusterId < 0 || clusterId >= clusters.size())
        throw new IllegalArgumentException("Cluster id '" + clusterId + "' is outside the of range of configured clusters (0-"
            + (clusters.size() - 1) + ") in database '" + name + "'");

      final OCluster cluster = clusters.get(clusterId);
      if (cluster == null)
        return false;

      if (iTruncate)
        cluster.truncate();
      cluster.delete();

      makeStorageDirty();
      clusterMap.remove(cluster.getName().toLowerCase());
      clusters.set(clusterId, null);

      // UPDATE CONFIGURATION
      configuration.dropCluster(clusterId);

      makeFullCheckpoint();
      return true;
    } catch (Exception e) {
      throw new OStorageException("Error while removing cluster '" + clusterId + "'", e);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean setClusterStatus(final int clusterId, final OStorageClusterConfiguration.STATUS iStatus) {
    lock.acquireExclusiveLock();
    try {

      if (clusterId < 0 || clusterId >= clusters.size())
        throw new IllegalArgumentException("Cluster id '" + clusterId + "' is outside the of range of configured clusters (0-"
            + (clusters.size() - 1) + ") in database '" + name + "'");

      final OCluster cluster = clusters.get(clusterId);
      if (cluster == null)
        return false;

      if (iStatus == OStorageClusterConfiguration.STATUS.OFFLINE && cluster instanceof OOfflineCluster
          || iStatus == OStorageClusterConfiguration.STATUS.ONLINE && !(cluster instanceof OOfflineCluster))
        return false;

      final OCluster newCluster;
      if (iStatus == OStorageClusterConfiguration.STATUS.OFFLINE) {
        cluster.close(true);
        newCluster = new OOfflineCluster(this, clusterId, cluster.getName());
      } else {

        newCluster = OPaginatedClusterFactory.INSTANCE.createCluster(configuration.version, this);
        newCluster.configure(this, clusterId, cluster.getName());
        newCluster.open();
      }

      clusterMap.put(cluster.getName().toLowerCase(), newCluster);
      clusters.set(clusterId, newCluster);

      // UPDATE CONFIGURATION
      makeStorageDirty();
      configuration.setClusterStatus(clusterId, iStatus);

      makeFullCheckpoint();
      return true;
    } catch (Exception e) {
      throw new OStorageException("Error while removing cluster '" + clusterId + "'", e);

    } finally {
      lock.releaseExclusiveLock();
    }
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

  public long[] getClusterDataRange(final int iClusterId) {
    if (iClusterId == -1)
      return new long[] { ORID.CLUSTER_POS_INVALID, ORID.CLUSTER_POS_INVALID };

    checkOpeness();
    try {
      return clusters.get(iClusterId) != null ? new long[] { clusters.get(iClusterId).getFirstPosition(),
          clusters.get(iClusterId).getLastPosition() } : new long[0];

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

  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId rid, final byte[] content,
      ORecordVersion recordVersion, final byte recordType, final int mode, final ORecordCallback<Long> callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final OPhysicalPosition ppos = new OPhysicalPosition(recordType);
    final OCluster cluster = getClusterById(rid.clusterId);

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer, "db.*.createRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        lock.acquireSharedLock();
        try {
          return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos);
        } finally {
          lock.releaseSharedLock();
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
    lockManager.acquireLock(this, rid, OLockManager.LOCK.SHARED);
    try {
      lock.acquireSharedLock();
      try {
        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
        if (ppos == null)
          return null;

        return new ORecordMetadata(rid, ppos.recordVersion);
      } finally {
        lock.releaseSharedLock();
      }
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
    } finally {
      lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
    }

    return null;
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback) {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecord(getClusterById(iRid.clusterId), iRid));
  }

  @Override
  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId rid, boolean updateContent, byte[] content,
      final ORecordVersion version, final byte recordType, final int mode, ORecordCallback<ORecordVersion> callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final OCluster cluster = getClusterById(rid.clusterId);
    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doUpdateRecord(rid, updateContent, content, version, recordType, callback, cluster);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer, "db.*.updateRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
        lockManager.acquireLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          lock.acquireSharedLock();
          try {
            // UPDATE IT
            return doUpdateRecord(rid, updateContent, content, version, recordType, callback, cluster);
          } finally {
            lock.releaseSharedLock();
          }
        } finally {
          lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer, "db.*.updateRecord");
    }
  }

  public OStorageTransaction getStorageTransaction() {
    return transaction.get();
  }

  public OAtomicOperationsManager getAtomicOperationsManager() {
    return atomicOperationsManager;
  }

  public OWriteAheadLog getWALInstance() {
    return writeAheadLog;
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId rid, final ORecordVersion version, final int mode,
      ORecordCallback<Boolean> callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final OCluster cluster = getClusterById(rid.clusterId);

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doDeleteRecord(rid, version, cluster);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        lockManager.acquireLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          lock.acquireSharedLock();
          try {
            return doDeleteRecord(rid, version, cluster);
          } finally {
            lock.releaseSharedLock();
          }
        } finally {
          lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
    }
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(final ORecordId rid, final int mode, ORecordCallback<Boolean> callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final OCluster cluster = getClusterById(rid.clusterId);

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doHideRecord(rid, cluster);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    cluster.getExternalModificationLock().requestModificationLock();
    try {
      modificationLock.requestModificationLock();
      try {
        lockManager.acquireLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          lock.acquireSharedLock();
          try {
            return doHideRecord(rid, cluster);
          } finally {
            lock.releaseSharedLock();
          }
        } finally {
          lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      cluster.getExternalModificationLock().releaseModificationLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
    }
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

  public Set<String> getClusterNames() {
    checkOpeness();
    return new HashSet<String>(clusterMap.keySet());
  }

  public int getClusterIdByName(final String clusterName) {
    checkOpeness();

    if (clusterName == null)
      throw new IllegalArgumentException("Cluster name is null");

    if (clusterName.length() == 0)
      throw new IllegalArgumentException("Cluster name is empty");

    if (Character.isDigit(clusterName.charAt(0)))
      return Integer.parseInt(clusterName);

    // SEARCH IT BETWEEN PHYSICAL CLUSTERS

    final OCluster segment = clusterMap.get(clusterName.toLowerCase());
    if (segment != null)
      return segment.getId();

    return -1;
  }

  public void commit(final OTransaction clientTx, Runnable callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final ODatabaseDocumentInternal databaseRecord = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (databaseRecord != null)
      ((OMetadataInternal) databaseRecord.getMetadata()).makeThreadLocalSchemaSnapshot();

    try {
      modificationLock.requestModificationLock();
      try {
        lock.acquireExclusiveLock();
        try {
          if (writeAheadLog == null && clientTx.isUsingLog())
            throw new OStorageException("WAL mode is not active. Transactions are not supported in given mode");

          makeStorageDirty();
          startStorageTx(clientTx);

          final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

          while (clientTx.getCurrentRecordEntries().iterator().hasNext()) {
            for (ORecordOperation txEntry : clientTx.getCurrentRecordEntries())
              tmpEntries.add(txEntry);

            clientTx.clearRecordEntries();

            for (ORecordOperation txEntry : tmpEntries) {
              if (txEntry.type == ORecordOperation.CREATED || txEntry.type == ORecordOperation.UPDATED) {
                final ORecord record = txEntry.getRecord();
                if (record instanceof ODocument)
                  ((ODocument) record).validate();
              }
            }
            for (ORecordOperation txEntry : tmpEntries)
              // COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
              commitEntry(clientTx, txEntry);

          }

          if (callback != null)
            callback.run();

          endStorageTx();

          OTransactionAbstract.updateCacheFromEntries(clientTx, clientTx.getAllRecordEntries(), true);

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
    } finally {
      if (databaseRecord != null)
        ((OMetadataInternal) databaseRecord.getMetadata()).clearThreadLocalSchemaSnapshot();
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

        makeStorageDirty();
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

        clearStorageDirty();
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

    if (iClusterId < 0 || iClusterId >= clusters.size())
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
  public OCluster getClusterByName(final String clusterName) {
    final OCluster cluster = clusterMap.get(clusterName.toLowerCase());

    if (cluster == null)
      throw new IllegalArgumentException("Cluster " + clusterName + " does not exist in database '" + name + "'");
    return cluster;
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

  public boolean wereDataRestoredAfterOpen() {
    return wereDataRestoredAfterOpen;
  }

  public boolean wereNonTxOperationsPerformedInPreviousOpen() {
    return wereNonTxOperationsPerformedInPreviousOpen;
  }

  public void reload() {
  }

  public String getMode() {
    return mode;
  }

  @Override
  public void lowDiskSpace(OLowDiskSpaceInformation information) {
    lowDiskSpace = information;
  }

  @Override
  public void requestCheckpoint() {
    checkpointRequest = true;
  }

  /**
   * Executes the command request and return the result back.
   */
  public Object command(final OCommandRequestText iCommand) {
    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    // COPY THE CONTEXT FROM THE REQUEST
    executor.setContext(iCommand.getContext());

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    return executeCommand(iCommand, executor);
  }

  public Object executeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor) {
    if (iCommand.isIdempotent() && !executor.isIdempotent())
      throw new OCommandExecutionException("Cannot execute non idempotent command");

    long beginTime = Orient.instance().getProfiler().startChrono();

    try {

      return executor.execute(iCommand.getParameters());

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);

    } finally {
      if (Orient.instance().getProfiler().isRecording())
        Orient
            .instance()
            .getProfiler()
            .stopChrono("db." + ODatabaseRecordThreadLocal.INSTANCE.get().getName() + ".command." + iCommand.toString(),
                "Command executed against the database", beginTime, "db.*.command.*");
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(currentClusterId);
      return cluster.higherPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(clusterId);
      return cluster.ceilingPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(currentClusterId);

      return cluster.lowerPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return null;

    checkOpeness();

    lock.acquireSharedLock();
    try {
      final OCluster cluster = getClusterById(clusterId);

      return cluster.floorPositions(physicalPosition);
    } catch (IOException ioe) {
      throw new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\'', ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public void acquireWriteLock(final ORID rid) {
    assert !lock.assertSharedLockHold() && !lock.assertExclusiveLockHold() : " a record lock should not be taken inside a storage lock";

    lockManager.acquireLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
  }

  public void releaseWriteLock(final ORID rid) {
    assert !lock.assertSharedLockHold() && !lock.assertExclusiveLockHold() : " a record lock should not be released inside a storage lock";
    lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
  }

  public void acquireReadLock(final ORID rid) {
    assert !lock.assertSharedLockHold() && !lock.assertExclusiveLockHold() : " a record lock should not be taken inside a storage lock";
    lockManager.acquireLock(this, rid, OLockManager.LOCK.SHARED);

  }

  public void releaseReadLock(final ORID iRid) {
    assert !lock.assertSharedLockHold() && !lock.assertExclusiveLockHold() : " a record lock should not be released inside a storage lock";
    lockManager.releaseLock(this, iRid, OLockManager.LOCK.SHARED);
  }

  public ORecordConflictStrategy getConflictStrategy() {
    return recordConflictStrategy;
  }

  public void setConflictStrategy(final ORecordConflictStrategy conflictResolver) {
    this.recordConflictStrategy = conflictResolver;
  }

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  protected void checkOpeness() {
    if (status != STATUS.OPEN)
      throw new OStorageException("Storage " + name + " is not opened.");
  }

  protected void makeFullCheckpoint() throws IOException {
    if (writeAheadLog == null)
      return;

    try {
      writeAheadLog.flush();

      if (configuration != null)
        configuration.synch();

      final OLogSequenceNumber lastLSN = writeAheadLog.logFullCheckpointStart();
      diskCache.flushBuffer();
      writeAheadLog.logFullCheckpointEnd();
      writeAheadLog.flush();

      writeAheadLog.cutTill(lastLSN);

      clearStorageDirty();
    } catch (IOException ioe) {
      throw new OStorageException("Error during checkpoint creation for storage " + name, ioe);
    }
  }

  protected void preOpenSteps() throws IOException {
  }

  protected void postCreateSteps() {
  }

  protected void preCreateSteps() throws IOException {
  }

  protected abstract void initWalAndDiskCache() throws IOException;

  protected void postCloseSteps(boolean onDelete) throws IOException {
  }

  protected void preCloseSteps() throws IOException {
  }

  protected void postDeleteSteps() {
  }

  protected void makeStorageDirty() throws IOException {
  }

  protected void clearStorageDirty() throws IOException {
  }

  protected boolean isDirty() throws IOException {
    return false;
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

  private ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid) {
    checkOpeness();

    if (!rid.isPersistent())
      throw new IllegalArgumentException("Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        // Disabled this assert have no meaning anymore
        // assert iLockingStrategy.equals(LOCKING_STRATEGY.DEFAULT);
        return doReadRecord(clusterSegment, rid);
      } finally {
        Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    clusterSegment.getExternalModificationLock().requestModificationLock();
    try {
      lockManager.acquireLock(this, rid, OLockManager.LOCK.SHARED);
      try {
        ORawBuffer buff;
        lock.acquireSharedLock();
        try {
          buff = doReadRecord(clusterSegment, rid);
          return buff;
        } finally {
          lock.releaseSharedLock();
        }
      } finally {
        lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
      }
    } finally {
      clusterSegment.getExternalModificationLock().releaseModificationLock();

      Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
    }
  }

  private void endStorageTx() throws IOException {
    atomicOperationsManager.endAtomicOperation(false);

    assert atomicOperationsManager.getCurrentOperation() == null;
  }

  private void startStorageTx(OTransaction clientTx) throws IOException {
    if (writeAheadLog == null)
      return;

    final OStorageTransaction storageTx = transaction.get();
    if (storageTx != null && storageTx.getClientTx().getId() != clientTx.getId())
      rollback(clientTx);

    assert atomicOperationsManager.getCurrentOperation() == null;

    transaction.set(new OStorageTransaction(clientTx));
    try {
      atomicOperationsManager.startAtomicOperation(null);
    } catch (RuntimeException e) {
      transaction.set(null);
      throw e;
    }
  }

  private void rollbackStorageTx() throws IOException {
    if (writeAheadLog == null || transaction.get() == null)
      return;

    atomicOperationsManager.endAtomicOperation(true);

    assert atomicOperationsManager.getCurrentOperation() == null;
  }

  private void restoreIfNeeded() throws Exception {
    if (isDirty()) {
      OLogManager.instance().warn(this, "Storage " + name + " was not closed properly. Will try to restore from write ahead log.");
      try {
        wereDataRestoredAfterOpen = restoreFromWAL();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during storage data restore.", e);
        throw e;
      }

      OLogManager.instance().info(this, "Storage data restore was completed");
    }
  }

  private OStorageOperationResult<OPhysicalPosition> doCreateRecord(ORecordId rid, byte[] content, ORecordVersion recordVersion,
      byte recordType, ORecordCallback<Long> callback, OCluster cluster, OPhysicalPosition ppos) {
    if (content == null)
      throw new IllegalArgumentException("Record is null");

    try {
      if (recordVersion.getCounter() > -1)
        recordVersion.increment();
      else
        recordVersion = OVersionFactory.instance().createVersion();

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation(null);
      try {
        ppos = cluster.createRecord(content, recordVersion, recordType);
        rid.clusterPosition = ppos.clusterPosition;

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
        atomicOperationsManager.endAtomicOperation(false);
      } catch (Throwable throwable) {
        atomicOperationsManager.endAtomicOperation(true);

        if (throwable instanceof OOfflineClusterException)
          throw (OOfflineClusterException) throwable;

        OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, throwable);

        try {
          if (ppos.clusterPosition != ORID.CLUSTER_POS_INVALID)
            cluster.deleteRecord(ppos.clusterPosition);
        } catch (IOException e) {
          OLogManager.instance().error(this, "Error on removing record in cluster: " + cluster, e);
        }

        return null;
      }

      if (callback != null)
        callback.call(rid, ppos.clusterPosition);

      return new OStorageOperationResult<OPhysicalPosition>(ppos);
    } catch (IOException ioe) {
      try {
        if (ppos.clusterPosition != ORID.CLUSTER_POS_INVALID)
          cluster.deleteRecord(ppos.clusterPosition);
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, e);
      }

      OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, ioe);

      throw new OStorageException("Error during record deletion", ioe);
    }
  }

  private OStorageOperationResult<ORecordVersion> doUpdateRecord(ORecordId rid, boolean updateContent, byte[] content,
      ORecordVersion version, byte recordType, ORecordCallback<ORecordVersion> callback, OCluster cluster) {

    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
      if (!checkForRecordValidity(ppos)) {
        final ORecordVersion recordVersion = OVersionFactory.instance().createUntrackedVersion();
        if (callback != null)
          callback.call(rid, recordVersion);

        return new OStorageOperationResult<ORecordVersion>(recordVersion);
      }

      boolean contentModified = false;
      if (updateContent) {
        final byte[] newContent = checkAndIncrementVersion(cluster, rid, version, ppos.recordVersion, content, recordType);
        if (newContent != null) {
          contentModified = true;
          content = newContent;
        }
      }

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation(null);
      try {
        if (updateContent)
          cluster.updateRecord(rid.clusterPosition, content, ppos.recordVersion, recordType);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
        atomicOperationsManager.endAtomicOperation(false);
      } catch (Throwable e) {
        atomicOperationsManager.endAtomicOperation(true);

        OLogManager.instance().error(this, "Error on updating record " + rid + " (cluster: " + cluster + ")", e);

        final ORecordVersion recordVersion = OVersionFactory.instance().createUntrackedVersion();
        if (callback != null)
          callback.call(rid, recordVersion);

        return new OStorageOperationResult<ORecordVersion>(recordVersion);
      }

      if (callback != null)
        callback.call(rid, ppos.recordVersion);

      if (contentModified)
        return new OStorageOperationResult<ORecordVersion>(ppos.recordVersion, content, false);
      else
        return new OStorageOperationResult<ORecordVersion>(ppos.recordVersion);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on updating record " + rid + " (cluster: " + cluster + ")", ioe);

      final ORecordVersion recordVersion = OVersionFactory.instance().createUntrackedVersion();
      if (callback != null)
        callback.call(rid, recordVersion);

      return new OStorageOperationResult<ORecordVersion>(recordVersion);
    }
  }

  private OStorageOperationResult<Boolean> doDeleteRecord(ORecordId rid, ORecordVersion version, OCluster cluster) {
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

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation(null);
      try {
        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);

        cluster.deleteRecord(ppos.clusterPosition);
        atomicOperationsManager.endAtomicOperation(false);
      } catch (Throwable e) {
        atomicOperationsManager.endAtomicOperation(true);
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);
        return new OStorageOperationResult<Boolean>(false);
      }

      return new OStorageOperationResult<Boolean>(true);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
    }
  }

  private OStorageOperationResult<Boolean> doHideRecord(ORecordId rid, OCluster cluster) {
    try {
      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation(null);
      try {
        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);

        cluster.hideRecord(rid.clusterPosition);
        atomicOperationsManager.endAtomicOperation(false);
      } catch (Throwable e) {
        atomicOperationsManager.endAtomicOperation(true);
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);

        return new OStorageOperationResult<Boolean>(false);
      }

      return new OStorageOperationResult<Boolean>(true);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
    }
  }

  private ORawBuffer doReadRecord(OCluster clusterSegment, ORecordId rid) {
    try {
      ORawBuffer buff;
      buff = clusterSegment.readRecord(rid.clusterPosition);
      return buff;
    } catch (IOException e) {
      throw new OStorageException("Error during read of record with rid = " + rid, e);
    }
  }

  private void addDefaultClusters() throws IOException {
    final String storageCompression = getConfiguration().getContextConfiguration().getValueAsString(
        OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);

    final String stgConflictStrategy = getConflictStrategy().getName();

    createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        OMetadataDefault.CLUSTER_INTERNAL_NAME, null, true, 20, 4, storageCompression, stgConflictStrategy,
        OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        OMetadataDefault.CLUSTER_INDEX_NAME, null, false, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
        OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, storageCompression, stgConflictStrategy,
        OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null, false, 1, 1, storageCompression, stgConflictStrategy,
        OStorageClusterConfiguration.STATUS.ONLINE));

    defaultClusterId = createClusterFromConfig(new OStoragePaginatedClusterConfiguration(configuration, clusters.size(),
        CLUSTER_DEFAULT_NAME, null, true, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
        OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, storageCompression, stgConflictStrategy,
        OStorageClusterConfiguration.STATUS.ONLINE));
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration config) throws IOException {
    OCluster cluster = clusterMap.get(config.getName().toLowerCase());

    if (cluster != null) {
      cluster.configure(this, config);
      return -1;
    }

    if (config.getStatus() == OStorageClusterConfiguration.STATUS.ONLINE)
      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(configuration.version, this);
    else
      cluster = new OOfflineCluster(this, config.getId(), config.getName());
    cluster.configure(this, config);

    return registerCluster(cluster);
  }

  private void setCluster(int id, OCluster cluster) {
    if (clusters.size() <= id) {
      while (clusters.size() < id)
        clusters.add(null);

      clusters.add(cluster);
    } else
      clusters.set(id, cluster);
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
        throw new OConfigurationException("Cannot add cluster '" + cluster.getName()
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

  private int doAddCluster(String clusterName, boolean fullCheckPoint, Object[] parameters) throws IOException {
    // FIND THE FIRST AVAILABLE CLUSTER ID
    int clusterPos = clusters.size();
    for (int i = 0; i < clusters.size(); ++i) {
      if (clusters.get(i) == null) {
        clusterPos = i;
        break;
      }
    }

    return addClusterInternal(clusterName, clusterPos, fullCheckPoint, parameters);
  }

  private int addClusterInternal(String clusterName, int clusterPos, boolean fullCheckPoint, Object... parameters)
      throws IOException {

    final OCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase();

      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(configuration.version, this);
      cluster.configure(this, clusterPos, clusterName, parameters);

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

  private void doClose(boolean force, boolean onDelete) {
    if (!force && !onDelete)
      return;

    if (status == STATUS.CLOSED)
      return;

    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {
      if (status == STATUS.CLOSED)
        return;

      status = STATUS.CLOSING;

      if (!onDelete)
        makeFullCheckpoint();

      preCloseSteps();

      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.close(!onDelete);

      clusters.clear();
      clusterMap.clear();

      if (configuration != null)
        configuration.close();

      super.close(force, onDelete);

      diskCache.removeLowDiskSpaceListener(this);
      if (writeAheadLog != null)
        writeAheadLog.removeFullCheckpointListener(this);

      if (!onDelete)
        diskCache.close();
      else
        diskCache.delete();

      if (writeAheadLog != null) {
        writeAheadLog.close();
        if (onDelete)
          writeAheadLog.delete();
      }

      postCloseSteps(onDelete);

      status = STATUS.CLOSED;
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, e, OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
    }
  }

  private byte[] checkAndIncrementVersion(final OCluster iCluster, final ORecordId rid, final ORecordVersion version,
      final ORecordVersion iDatabaseVersion, final byte[] iRecordContent, final byte iRecordType) {
    // VERSION CONTROL CHECK
    final int v = version.getCounter();

    switch (v) {
    // DOCUMENT UPDATE, NO VERSION CONTROL
    case -1:
      iDatabaseVersion.increment();
      break;

    // DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION UPDATE
    case -2:
      iDatabaseVersion.setCounter(-2);
      break;

    default:
      // MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (v < -2) {
        // OVERWRITE VERSION: THIS IS USED IN CASE OF FIX OF RECORDS IN DISTRIBUTED MODE
        version.clearRollbackMode();
        iDatabaseVersion.setCounter(version.getCounter());
      } else if (!version.equals(iDatabaseVersion)) {
        final ORecordConflictStrategy strategy = iCluster.getRecordConflictStrategy() != null ? iCluster
            .getRecordConflictStrategy() : recordConflictStrategy;
        return strategy.onUpdate(this, iRecordType, rid, version, iRecordContent, iDatabaseVersion);
      } else
        // OK, INCREMENT DB VERSION
        iDatabaseVersion.increment();
    }

    return null;
  }

  private void commitEntry(final OTransaction clientTx, final ORecordOperation txEntry) throws IOException {

    final ORecord rec = txEntry.getRecord();
    if (txEntry.type != ORecordOperation.DELETED && !rec.isDirty())
      return;

    ORecordId rid = (ORecordId) rec.getIdentity();

    if (txEntry.type == ORecordOperation.UPDATED && rid.isNew())
      // OVERWRITE OPERATION AS CREATE
      txEntry.type = ORecordOperation.CREATED;

    ORecordSerializationContext.pushContext();
    try {
      int clusterId = rid.clusterId;
      if (rid.clusterId == ORID.CLUSTER_ID_INVALID && rec instanceof ODocument
          && ODocumentInternal.getImmutableSchemaClass(((ODocument) rec)) != null) {
        // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS

        final OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) rec));
        clusterId = schemaClass.getClusterForNewInstance((ODocument) rec);
      }

      final OCluster cluster = getClusterById(clusterId);

      if (cluster.getName().equals(OMetadataDefault.CLUSTER_INDEX_NAME)
          || cluster.getName().equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME))
        // AVOID TO COMMIT INDEX STUFF
        return;

      if (rec instanceof OTxListener)
        ((OTxListener) rec).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

      switch (txEntry.type) {
      case ORecordOperation.LOADED:
        break;

      case ORecordOperation.CREATED: {
        // CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD

        final byte[] stream = rec.toStream();
        if (stream == null) {
          OLogManager.instance().warn(this, "Null serialization on committing new record %s in transaction", rid);
          break;
        }
        final ORecordId oldRID = rid.isNew() ? rid.copy() : rid;

        if (rid.isNew()) {
          rid = rid.copy();
          rid.clusterId = cluster.getId();
          final OPhysicalPosition ppos;

          final byte recordType = ORecordInternal.getRecordType(rec);
          ppos = doCreateRecord(rid, stream, rec.getRecordVersion(), recordType, null, cluster, new OPhysicalPosition(recordType))
              .getResult();

          rid.clusterPosition = ppos.clusterPosition;
          rec.getRecordVersion().copyFrom(ppos.recordVersion);
          clientTx.updateIdentityAfterCommit(oldRID, rid);
        } else {
          // ORecordInternal.setContentChanged(rec, true);
          rec.getRecordVersion().copyFrom(
              updateRecord(rid, ORecordInternal.isContentChanged(rec), stream, rec.getRecordVersion(),
                  ORecordInternal.getRecordType(rec), -1, null).getResult());
        }
        break;
      }

      case ORecordOperation.UPDATED: {
        final byte[] stream = rec.toStream();
        if (stream == null) {
          OLogManager.instance().warn(this, "Null serialization on committing updated record %s in transaction", rid);
          break;
        }

        rec.getRecordVersion().copyFrom(
            doUpdateRecord(rid, ORecordInternal.isContentChanged(rec), stream, rec.getRecordVersion(),
                ORecordInternal.getRecordType(rec), null, cluster).getResult());
        break;
      }

      case ORecordOperation.DELETED: {
        deleteRecord(rid, rec.getRecordVersion(), -1, null);
        break;
      }
      }
    } finally {
      ORecordSerializationContext.pullContext();
    }

    // RESET TRACKING
    if (rec instanceof ODocument && ((ODocument) rec).isTrackingChanges()) {
      ((ODocument) rec).setTrackingChanges(false);
      ((ODocument) rec).setTrackingChanges(true);
    }

    ORecordInternal.unsetDirty(rec);

    if (rec instanceof OTxListener)
      ((OTxListener) rec).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId < 0 || iClusterId > clusters.size() - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  private List<OLogSequenceNumber> readOperationUnit(OLogSequenceNumber startLSN, OOperationUnitId unitId) throws IOException {
    final OLogSequenceNumber beginSequence = writeAheadLog.begin();

    if (startLSN == null)
      startLSN = beginSequence;

    if (startLSN.compareTo(beginSequence) < 0)
      startLSN = beginSequence;

    List<OLogSequenceNumber> operationUnit = new ArrayList<OLogSequenceNumber>();

    OLogSequenceNumber lsn = startLSN;
    while (lsn != null) {
      try {
        OWALRecord record = writeAheadLog.read(lsn);
        if (!(record instanceof OOperationUnitRecord)) {
          lsn = writeAheadLog.next(lsn);
          continue;
        }

        OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) record;
        if (operationUnitRecord.getOperationUnitId().equals(unitId)) {
          operationUnit.add(lsn);
          if (record instanceof OAtomicUnitEndRecord)
            break;
        }
        lsn = writeAheadLog.next(lsn);
      } catch (OWALPageBrokenException e) {
        break;
      }
    }

    return operationUnit;
  }

  private boolean restoreFromWAL() throws IOException {
    if (writeAheadLog == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead logging is switched off.");
      return true;
    }

    if (writeAheadLog.begin() == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead log is empty.");
      return false;
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
      return restoreFromBegging();
    }

    OWALRecord checkPointRecord;
    try {
      checkPointRecord = writeAheadLog.read(lastCheckPoint);
    } catch (OWALPageBrokenException e) {
      checkPointRecord = null;
    }

    if (checkPointRecord == null) {
      OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
      return restoreFromBegging();
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
          return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
        } else {
          OLogManager.instance().warn(this, "Restore will start from the beginning.");
          return restoreFromBegging();
        }
      } else
        return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
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
          return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
        } else {
          OLogManager.instance().warn(this, "Restore will start from the beginning.");
          return restoreFromBegging();
        }
      } else
        return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
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

  private boolean restoreFromCheckPoint(OAbstractCheckPointStartRecord checkPointRecord) throws IOException {
    if (checkPointRecord instanceof OFuzzyCheckpointStartRecord) {
      return restoreFromFuzzyCheckPoint((OFuzzyCheckpointStartRecord) checkPointRecord);
    }

    if (checkPointRecord instanceof OFullCheckpointStartRecord) {
      return restoreFromFullCheckPoint((OFullCheckpointStartRecord) checkPointRecord);
    }

    throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());
  }

  private boolean restoreFromFullCheckPoint(OFullCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from full checkpoint is started. Restore is performed from LSN %s",
        checkPointRecord.getLsn());

    final OLogSequenceNumber lsn = writeAheadLog.next(checkPointRecord.getLsn());
    return restoreFrom(lsn);
  }

  private boolean restoreFromFuzzyCheckPoint(OFuzzyCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from FUZZY checkpoint is started.");
    OLogSequenceNumber flushedLsn = checkPointRecord.getFlushedLsn();

    if (flushedLsn.compareTo(writeAheadLog.begin()) < 0)
      flushedLsn = writeAheadLog.begin();

    return restoreFrom(flushedLsn);
  }

  private boolean restoreFromBegging() throws IOException {
    OLogManager.instance().info(this, "Data restore procedure is started.");
    OLogSequenceNumber lsn = writeAheadLog.begin();

    return restoreFrom(lsn);
  }

  private boolean restoreFrom(OLogSequenceNumber lsn) throws IOException {
    final OModifiableBoolean atLeastOnePageUpdate = new OModifiableBoolean(false);

    long recordsProcessed = 0;

    final int reportInterval = OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();
    final Map<OOperationUnitId, List<OWALRecord>> operationUnits = new HashMap<OOperationUnitId, List<OWALRecord>>();

    try {
      while (lsn != null) {
        OWALRecord walRecord = writeAheadLog.read(lsn);

        if (walRecord instanceof OAtomicUnitEndRecord) {
          OAtomicUnitEndRecord atomicUnitEndRecord = (OAtomicUnitEndRecord) walRecord;
          List<OWALRecord> atomicUnit = operationUnits.remove(atomicUnitEndRecord.getOperationUnitId());

          // in case of data restore from fuzzy checkpoint part of operations may be already flushed to the disk
          if (atomicUnit != null) {
            atomicUnit.add(walRecord);
            restoreAtomicUnit(atomicUnit, atLeastOnePageUpdate);
          }

        } else if (walRecord instanceof OAtomicUnitStartRecord) {
          List<OWALRecord> operationList = new ArrayList<OWALRecord>();

          assert !operationUnits.containsKey(((OAtomicUnitStartRecord) walRecord).getOperationUnitId());

          operationUnits.put(((OAtomicUnitStartRecord) walRecord).getOperationUnitId(), operationList);
          operationList.add(walRecord);
        } else if (walRecord instanceof OOperationUnitRecord) {
          OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) walRecord;

          // in case of data restore from fuzzy checkpoint part of operations may be already flushed to the disk
          List<OWALRecord> operationList = operationUnits.get(operationUnitRecord.getOperationUnitId());
          if (operationList == null) {
            operationList = new ArrayList<OWALRecord>();
            operationUnits.put(operationUnitRecord.getOperationUnitId(), operationList);
          }

          operationList.add(operationUnitRecord);
        } else if (walRecord instanceof ONonTxOperationPerformedWALRecord) {
          if (!wereNonTxOperationsPerformedInPreviousOpen) {
            OLogManager.instance().warn(this, "Non tx operation was used during data modification we will need index rebuild.");
            wereNonTxOperationsPerformedInPreviousOpen = true;
          }
        } else
          OLogManager.instance().warn(this, "Record %s will be skipped during data restore.", walRecord);

        recordsProcessed++;

        if (reportInterval > 0 && recordsProcessed % reportInterval == 0)
          OLogManager.instance().info(this, "%d operations were processed, current LSN is %s last LSN is %s", recordsProcessed,
              lsn, writeAheadLog.end());

        lsn = writeAheadLog.next(lsn);
      }
    } catch (OWALPageBrokenException e) {
      OLogManager.instance().error(this,
          "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.");
    }

    return atLeastOnePageUpdate.getValue();
  }

  protected void restoreAtomicUnit(List<OWALRecord> atomicUnit, OModifiableBoolean atLeastOnePageUpdate) throws IOException {
    assert atomicUnit.get(atomicUnit.size() - 1) instanceof OAtomicUnitEndRecord;

    for (OWALRecord walRecord : atomicUnit) {
      if (walRecord instanceof OFileDeletedWALRecord) {
        OFileDeletedWALRecord fileDeletedWALRecord = (OFileDeletedWALRecord) walRecord;
        if (diskCache.exists(fileDeletedWALRecord.getFileId()))
          diskCache.deleteFile(fileDeletedWALRecord.getFileId());
      } else if (walRecord instanceof OFileCreatedWALRecord) {
        OFileCreatedWALRecord fileCreatedCreatedWALRecord = (OFileCreatedWALRecord) walRecord;
        if (diskCache.exists(fileCreatedCreatedWALRecord.getFileName())) {
          diskCache.openFile(fileCreatedCreatedWALRecord.getFileName(), fileCreatedCreatedWALRecord.getFileId());
        } else {
          diskCache.addFile(fileCreatedCreatedWALRecord.getFileName(), fileCreatedCreatedWALRecord.getFileId());
        }
      } else if (walRecord instanceof OUpdatePageRecord) {
        final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) walRecord;

        final long fileId = updatePageRecord.getFileId();
        final long pageIndex = updatePageRecord.getPageIndex();

        if (!diskCache.isOpen(fileId))
          diskCache.openFile(fileId);

        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, true);
        if (cacheEntry == null) {
          do {
            if (cacheEntry != null)
              diskCache.release(cacheEntry);

            cacheEntry = diskCache.allocateNewPage(fileId);
          } while (cacheEntry.getPageIndex() != pageIndex);
        }

        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.acquireExclusiveLock();
        try {
          ODurablePage durablePage = new ODurablePage(cacheEntry, null);
          durablePage.restoreChanges(updatePageRecord.getChanges());
          durablePage.setLsn(updatePageRecord.getLsn());
        } finally {
          cachePointer.releaseExclusiveLock();
          diskCache.release(cacheEntry);
        }

        atLeastOnePageUpdate.setValue(true);
      } else if (walRecord instanceof OAtomicUnitStartRecord) {
        continue;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        continue;
      } else if (walRecord instanceof OFileTruncatedWALRecord) {
        final OFileTruncatedWALRecord fileTruncatedWALRecord = (OFileTruncatedWALRecord) walRecord;
        diskCache.truncateFile(fileTruncatedWALRecord.getFileId());
      } else {
        OLogManager.instance().error(this, "Invalid WAL record type was passed %s. Given record will be skipped.",
            walRecord.getClass());

        assert false : "Invalid WAL record type was passed " + walRecord.getClass().getName();
      }
    }
  }

  private void checkLowDiskSpaceAndFullCheckpointRequests() {
    if (transaction.get() != null)
      return;

    if (lowDiskSpace != null) {
      if (checkpointInProgress.compareAndSet(false, true)) {
        try {
          diskCache.makeFuzzyCheckpoint();

          if (diskCache.checkLowDiskSpace()) {
            synch();

            if (diskCache.checkLowDiskSpace()) {
              throw new OLowDiskSpaceException("Error occurred while executing a write operation to database '" + name
                  + "' due to limited free space on the disk (" + (lowDiskSpace.freeSpace / (1024 * 1024))
                  + " MB). The database is now working in read-only mode."
                  + " Please close the database (or stop OrientDB), make room on your hard drive and then reopen the database. "
                  + "The minimal required space is " + (lowDiskSpace.requiredSpace / (1024 * 1024)) + " MB. "
                  + "Required space is now set to " + OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsInteger()
                  + "MB (you can change it by setting parameter " + OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getKey()
                  + ") .");
            } else {
              lowDiskSpace = null;
            }
          } else
            lowDiskSpace = null;
        } finally {
          checkpointInProgress.set(false);
        }
      }
    }

    if (checkpointRequest && writeAheadLog instanceof ODiskWriteAheadLog) {
      if (checkpointInProgress.compareAndSet(false, true)) {
        try {
          final ODiskWriteAheadLog diskWriteAheadLog = (ODiskWriteAheadLog) writeAheadLog;
          final long size = diskWriteAheadLog.size();

          diskCache.makeFuzzyCheckpoint();
          if (size <= diskWriteAheadLog.size())
            synch();

          checkpointRequest = false;
        } finally {
          checkpointInProgress.set(false);
        }
      }
    }
  }
}
