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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
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
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OLowDiskSpaceException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OIdentifiableStorage;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
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
import com.orientechnologies.orient.core.version.OSimpleVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public abstract class OAbstractPaginatedStorage extends OStorageAbstract implements OLowDiskSpaceListener,
    OFullCheckpointRequestListener, OIdentifiableStorage, OOrientStartupListener, OOrientShutdownListener {
  private static final int RECORD_LOCK_TIMEOUT = OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger();

  private final OLockManager<ORID>                  lockManager;
  private final String                              PROFILER_CREATE_RECORD;
  private final String                              PROFILER_READ_RECORD;
  private final String                              PROFILER_UPDATE_RECORD;
  private final String                              PROFILER_DELETE_RECORD;
  private final Map<String, OCluster>               clusterMap           = new HashMap<String, OCluster>();
  private volatile ThreadLocal<OStorageTransaction> transaction          = new ThreadLocal<OStorageTransaction>();
  private final OModificationLock                   modificationLock     = new OModificationLock();
  private final AtomicBoolean                       checkpointInProgress = new AtomicBoolean();
  protected volatile OWriteAheadLog                 writeAheadLog;

  protected volatile OReadCache  readCache;
  protected volatile OWriteCache writeCache;

  private volatile ORecordConflictStrategy  recordConflictStrategy                     = Orient.instance()
      .getRecordConflictStrategy().newInstanceOfDefaultClass();
  private List<OCluster>                    clusters                                   = new ArrayList<OCluster>();
  private volatile int                      defaultClusterId                           = -1;
  private volatile OAtomicOperationsManager atomicOperationsManager;
  private volatile boolean                  wereDataRestoredAfterOpen                  = false;
  private volatile boolean                  wereNonTxOperationsPerformedInPreviousOpen = false;
  private boolean                           makeFullCheckPointAfterClusterCreate       = OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CLUSTER_CREATE
      .getValueAsBoolean();
  private volatile OLowDiskSpaceInformation lowDiskSpace                               = null;
  private volatile boolean                  checkpointRequest                          = false;

  private final int id;

  public OAbstractPaginatedStorage(String name, String filePath, String mode, int id) {
    super(name, filePath, mode, OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsInteger());

    this.id = id;
    lockManager = new OLockManager<ORID>(true, -1) {
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
    stateLock.acquireReadLock();
    try {
      if (status == STATUS.OPEN)
        // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
        // REUSED
        return;
    } finally {
      stateLock.releaseReadLock();
    }

    stateLock.acquireWriteLock();
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
      try {
        atomicOperationsManager.registerMBean();
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for atomic operations manager cannot be registered.", e);
      }

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
            OLogManager.instance().warn(this, "Error on loading cluster '" + clusters.get(i).getName() + "' (" + i
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

      writeCache.startFuzzyCheckpoints();

      status = STATUS.OPEN;
    } catch (Exception e) {
      status = STATUS.CLOSED;
      throw new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode, e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public void open(final OToken iToken, final Map<String, Object> iProperties) {
    open(iToken.getUserName(), "", iProperties);
  }

  public void create(final Map<String, Object> iProperties) {
    stateLock.acquireWriteLock();
    try {

      if (status != STATUS.CLOSED)
        throw new OStorageException("Cannot create new storage '" + getURL() + "' because it is not closed");

      if (exists())
        throw new OStorageException("Cannot create new storage '" + getURL() + "' because it already exists");

      if (!configuration.getContextConfiguration().getContextKeys()
          .contains(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey()))

        // SAVE COMPRESSION IN STORAGE CFG
        configuration.getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD,
            OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getValue());

      componentsFactory = new OCurrentStorageComponentsFactory(configuration);
      initWalAndDiskCache();

      atomicOperationsManager = new OAtomicOperationsManager(this);
      try {
        atomicOperationsManager.registerMBean();
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for atomic operations manager cannot be registered.", e);
      }

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

      writeCache.startFuzzyCheckpoints();
      postCreateSteps();

    } catch (OStorageException e) {
      close();
      throw e;
    } catch (IOException e) {
      close();
      throw new OStorageException("Error on creation of storage '" + name + "'", e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public void onShutdown() {
    transaction = null;
  }

  @Override
  public void onStartup() {
    if (transaction == null)
      transaction = new ThreadLocal<OStorageTransaction>();
  }

  public void startAtomicOperation(boolean rollbackOnlyMode) throws IOException {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();
      makeStorageDirty();

      atomicOperationsManager.startAtomicOperation((String) null, rollbackOnlyMode);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void commitAtomicOperation() throws IOException {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();
      atomicOperationsManager.endAtomicOperation(false, null);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void rollbackAtomicOperation() throws IOException {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();
      atomicOperationsManager.endAtomicOperation(true, null);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void markDirty() throws IOException {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();
      makeStorageDirty();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public void close(final boolean force, boolean onDelete) {
    doClose(force, onDelete);
  }

  public void delete() {
    final long timer = Orient.instance().getProfiler().startChrono();

    stateLock.acquireWriteLock();
    try {
      dataLock.acquireExclusiveLock();
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

        if (writeCache != null) {
          if (readCache != null)
            readCache.deleteStorage(writeCache);
          else
            writeCache.delete();
        }

        postDeleteSteps();

      } catch (IOException e) {
        throw new OStorageException("Cannot delete database '" + name + "'.", e);
      } finally {
        dataLock.releaseExclusiveLock();

        Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");
      }
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public boolean check(final boolean verbose, final OCommandOutputListener listener) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      dataLock.acquireExclusiveLock();
      try {
        checkOpeness();
        final long start = System.currentTimeMillis();

        OPageDataVerificationError[] pageErrors = writeCache.checkStoredPages(verbose ? listener : null);

        listener.onMessage("Check of storage completed in " + (System.currentTimeMillis() - start) + "ms. "
            + (pageErrors.length > 0 ? pageErrors.length + " with errors." : " without errors."));

        return pageErrors.length == 0;
      } finally {
        dataLock.releaseExclusiveLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void enableFullCheckPointAfterClusterCreate() {
    checkOpeness();
    stateLock.acquireWriteLock();
    try {
      checkOpeness();
      makeFullCheckPointAfterClusterCreate = true;
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public void disableFullCheckPointAfterClusterCreate() {
    checkOpeness();
    stateLock.acquireWriteLock();
    try {

      checkOpeness();
      makeFullCheckPointAfterClusterCreate = false;
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public boolean isMakeFullCheckPointAfterClusterCreate() {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      return makeFullCheckPointAfterClusterCreate;
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public int addCluster(String clusterName, boolean forceListBased, final Object... parameters) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    stateLock.acquireWriteLock();
    try {
      checkOpeness();

      makeStorageDirty();
      return doAddCluster(clusterName, true, parameters);

    } catch (Exception e) {
      throw new OStorageException("Error in creation of new cluster '" + clusterName, e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public int addCluster(String clusterName, int requestedId, boolean forceListBased, Object... parameters) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();
    stateLock.acquireWriteLock();
    try {
      checkOpeness();

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
      stateLock.releaseWriteLock();
    }
  }

  public boolean dropCluster(final int clusterId, final boolean iTruncate) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    stateLock.acquireWriteLock();
    try {

      checkOpeness();
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
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public int getId() {
    return id;
  }

  public boolean setClusterStatus(final int clusterId, final OStorageClusterConfiguration.STATUS iStatus) {
    checkOpeness();
    stateLock.acquireWriteLock();
    try {
      checkOpeness();
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

        newCluster = OPaginatedClusterFactory.INSTANCE.createCluster(cluster.getName(), configuration.version, this);
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
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public Class<OSBTreeCollectionManagerShared> getCollectionManagerClass() {
    return OSBTreeCollectionManagerShared.class;
  }

  public OReadCache getReadCache() {
    return readCache;
  }

  public OWriteCache getWriteCache() {
    return writeCache;
  }

  public void freeze(boolean throwException, int clusterId) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final OCluster cluster = getClusterById(clusterId);

      final String name = cluster.getName();
      if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(name) || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(name)) {
        throw new IllegalArgumentException("It is impossible to freeze and release index or manual index cluster!");
      }

      cluster.getExternalModificationLock().prohibitModifications(throwException);

      try {
        cluster.synch();
      } catch (IOException e) {
        throw new OStorageException("Error on synch cluster '" + name + "'", e);
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void release(int clusterId) {
    stateLock.acquireReadLock();
    try {
      final OCluster cluster = getClusterById(clusterId);

      final String name = cluster.getName();
      if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(name) || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(name)) {
        throw new IllegalArgumentException("It is impossible to freeze and release index or manualindex cluster!");
      }

      cluster.getExternalModificationLock().allowModifications();
    } finally {
      stateLock.releaseReadLock();
    }
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
    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {
        checkOpeness();

        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null)
          return 0;

        if (countTombstones)
          return cluster.getEntries();

        return cluster.getEntries() - cluster.getTombstonesCount();
      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public long[] getClusterDataRange(final int iClusterId) {
    if (iClusterId == -1)
      return new long[] { ORID.CLUSTER_POS_INVALID, ORID.CLUSTER_POS_INVALID };

    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      return clusters.get(iClusterId) != null
          ? new long[] { clusters.get(iClusterId).getFirstPosition(), clusters.get(iClusterId).getLastPosition() }
          : OCommonConst.EMPTY_LONG_ARRAY;

    } catch (IOException ioe) {
      throw new OStorageException("Cannot retrieve information about data range", ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    checkOpeness();

    long tot = 0;

    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {
        checkOpeness();

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

      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }

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
        Orient.instance().getProfiler().stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer,
            "db.*.createRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      cluster.getExternalModificationLock().requestModificationLock();
      try {
        modificationLock.requestModificationLock();
        try {
          dataLock.acquireSharedLock();
          try {
            checkOpeness();

            return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos);
          } finally {
            dataLock.releaseSharedLock();
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      } finally {
        cluster.getExternalModificationLock().releaseModificationLock();
        Orient.instance().getProfiler().stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer,
            "db.*.createRecord");
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    if (rid.isNew())
      throw new OStorageException("Passed record with id " + rid + " is new and cannot be stored.");

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      final OCluster cluster = getClusterById(rid.getClusterId());
      lockManager.acquireLock(rid, OLockManager.LOCK.SHARED);
      try {
        dataLock.acquireSharedLock();
        try {
          checkOpeness();

          final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
          if (ppos == null)
            return null;

          return new ORecordMetadata(rid, ppos.recordVersion);
        } finally {
          dataLock.releaseSharedLock();
        }
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
      }
    } finally {
      stateLock.releaseReadLock();
    }

    return null;
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback) {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecord(getClusterById(iRid.clusterId), iRid));
  }

  /**
   * Returns the requested records. The returned order could be different than the requested.
   *
   * @param iRids
   *          Set of rids to load in one shot
   */
  public Collection<OPair<ORecordId, ORawBuffer>> readRecords(final Collection<ORecordId> iRids) {
    checkOpeness();

    final List<OPair<ORecordId, ORawBuffer>> records = new ArrayList<OPair<ORecordId, ORawBuffer>>();

    if (iRids == null || iRids.isEmpty())
      return records;

    if (transaction.get() != null) {
      for (ORecordId rid : iRids) {
        if (!rid.isPersistent())
          throw new IllegalArgumentException(
              "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

        records.add(new OPair<ORecordId, ORawBuffer>(rid, doReadRecord(getClusterById(rid.clusterId), rid)));
      }
      return records;
    }

    // CREATE GROUP OF RIDS PER CLUSTER TO REDUCE LOCKS
    final Map<Integer, List<ORecordId>> ridsPerCluster = getRidsGroupedByCluster(iRids);

    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {

        for (Map.Entry<Integer, List<ORecordId>> entry : ridsPerCluster.entrySet()) {
          final int clusterId = entry.getKey();
          final OCluster clusterSegment = getClusterById(clusterId);

          clusterSegment.getExternalModificationLock().requestModificationLock();
          try {

            for (ORecordId rid : entry.getValue()) {

              lockManager.acquireLock(rid, OLockManager.LOCK.SHARED);
              try {
                records.add(new OPair<ORecordId, ORawBuffer>(rid, doReadRecord(clusterSegment, rid)));
              } finally {
                lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
              }
            }

          } finally {
            clusterSegment.getExternalModificationLock().releaseModificationLock();
          }
        }

      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }

    return records;
  }

  protected Map<Integer, List<ORecordId>> getRidsGroupedByCluster(Collection<ORecordId> iRids) {
    final Map<Integer, List<ORecordId>> ridsPerCluster = new HashMap<Integer, List<ORecordId>>();
    for (ORecordId rid : iRids) {
      List<ORecordId> rids = ridsPerCluster.get(rid.clusterId);
      if (rids == null) {
        rids = new ArrayList<ORecordId>(iRids.size());
        ridsPerCluster.put(rid.clusterId, rids);
      }
      rids.add(rid);
    }
    return ridsPerCluster;
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(ORecordId rid, String fetchPlan, boolean ignoreCache,
      ORecordVersion recordVersion) throws ORecordNotFoundException {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecordIfNotLatest(getClusterById(rid.clusterId), rid, recordVersion));
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
        Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer,
            "db.*.updateRecord");
      }
    }

    stateLock.acquireReadLock();
    try {
      final long timer = Orient.instance().getProfiler().startChrono();
      cluster.getExternalModificationLock().requestModificationLock();
      try {
        modificationLock.requestModificationLock();
        try {
          // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
          lockManager.acquireLock(rid, OLockManager.LOCK.EXCLUSIVE);
          try {
            dataLock.acquireSharedLock();
            try {
              checkOpeness();

              // UPDATE IT
              return doUpdateRecord(rid, updateContent, content, version, recordType, callback, cluster);
            } finally {
              dataLock.releaseSharedLock();
            }
          } finally {
            lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      } finally {
        cluster.getExternalModificationLock().releaseModificationLock();
        Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer,
            "db.*.updateRecord");
      }
    } finally {
      stateLock.releaseReadLock();
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
        Orient.instance().getProfiler().stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer,
            "db.*.deleteRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      cluster.getExternalModificationLock().requestModificationLock();
      try {
        modificationLock.requestModificationLock();
        try {
          lockManager.acquireLock(rid, OLockManager.LOCK.EXCLUSIVE);
          try {
            dataLock.acquireSharedLock();
            try {
              checkOpeness();

              return doDeleteRecord(rid, version, cluster);
            } finally {
              dataLock.releaseSharedLock();
            }
          } finally {
            lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      } finally {
        cluster.getExternalModificationLock().releaseModificationLock();
        Orient.instance().getProfiler().stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer,
            "db.*.deleteRecord");
      }
    } finally {
      stateLock.releaseReadLock();
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
        return doHideMethod(rid, cluster);
      } finally {
        Orient.instance().getProfiler().stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer,
            "db.*.deleteRecord");
      }
    }

    stateLock.acquireReadLock();
    try {
      final long timer = Orient.instance().getProfiler().startChrono();
      cluster.getExternalModificationLock().requestModificationLock();
      try {
        modificationLock.requestModificationLock();
        try {
          lockManager.acquireLock(rid, OLockManager.LOCK.EXCLUSIVE);
          try {
            dataLock.acquireSharedLock();
            try {
              checkOpeness();

              return doHideMethod(rid, cluster);
            } finally {
              dataLock.releaseSharedLock();
            }
          } finally {
            lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      } finally {
        cluster.getExternalModificationLock().releaseModificationLock();
        Orient.instance().getProfiler().stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer,
            "db.*.deleteRecord");
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    stateLock.acquireReadLock();
    try {
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
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public Set<String> getClusterNames() {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      return new HashSet<String>(clusterMap.keySet());
    } finally {
      stateLock.releaseReadLock();
    }

  }

  public int getClusterIdByName(final String clusterName) {
    checkOpeness();

    if (clusterName == null)
      throw new IllegalArgumentException("Cluster name is null");

    if (clusterName.length() == 0)
      throw new IllegalArgumentException("Cluster name is empty");

    if (Character.isDigit(clusterName.charAt(0)))
      return Integer.parseInt(clusterName);

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      // SEARCH IT BETWEEN PHYSICAL CLUSTERS

      final OCluster segment = clusterMap.get(clusterName.toLowerCase());
      if (segment != null)
        return segment.getId();

      return -1;
    } finally {
      stateLock.releaseReadLock();
    }

  }

  public void commit(final OTransaction clientTx, Runnable callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final ODatabaseDocumentInternal databaseRecord = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (databaseRecord != null)
      ((OMetadataInternal) databaseRecord.getMetadata()).makeThreadLocalSchemaSnapshot();

    stateLock.acquireReadLock();
    try {
      try {
        modificationLock.requestModificationLock();
        try {
          dataLock.acquireExclusiveLock();
          try {

            checkOpeness();

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
            dataLock.releaseExclusiveLock();
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      } finally {
        if (databaseRecord != null)
          ((OMetadataInternal) databaseRecord.getMetadata()).clearThreadLocalSchemaSnapshot();
      }

    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void rollback(final OTransaction clientTx) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      modificationLock.requestModificationLock();
      try {
        dataLock.acquireExclusiveLock();
        try {
          checkOpeness();

          if (transaction.get() == null)
            return;

          if (writeAheadLog == null)
            throw new OStorageException("WAL mode is not active. Transactions are not supported in given mode");

          if (transaction.get().getClientTx().getId() != clientTx.getId())
            throw new OStorageException(
                "Passed in and active transaction are different transactions. Passed in transaction cannot be rolled back.");

          makeStorageDirty();
          rollbackStorageTx();

          OTransactionAbstract.updateCacheFromEntries(clientTx, clientTx.getAllRecordEntries(), false);

        } catch (IOException e) {
          throw new OStorageException("Error during transaction rollback.", e);
        } finally {
          transaction.set(null);
          dataLock.releaseExclusiveLock();
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return ppos != null && !ppos.recordVersion.isTombstone();
  }

  public void synch() {
    checkOpeness();

    stateLock.acquireReadLock();
    try {
      final long timer = Orient.instance().getProfiler().startChrono();
      modificationLock.prohibitModifications();
      try {
        dataLock.acquireSharedLock();
        try {
          checkOpeness();

          if (writeAheadLog != null) {
            makeFullCheckpoint();
            return;
          }

          writeCache.flush();

          if (configuration != null)
            configuration.synch();

          clearStorageDirty();
        } catch (IOException e) {
          throw new OStorageException("Error on synch storage '" + name + "'", e);

        } finally {
          dataLock.releaseSharedLock();

          Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
        }
      } finally {
        modificationLock.allowModifications();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      if (iClusterId < 0 || iClusterId >= clusters.size())
        return null;

      return clusters.get(iClusterId) != null ? clusters.get(iClusterId).getName() : null;
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  public OCluster getClusterById(int iClusterId) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      checkClusterSegmentIndexRange(iClusterId);

      final OCluster cluster = clusters.get(iClusterId);
      if (cluster == null)
        throw new IllegalArgumentException("Cluster " + iClusterId + " is null");

      return cluster;
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public OCluster getClusterByName(final String clusterName) {
    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      final OCluster cluster = clusterMap.get(clusterName.toLowerCase());

      if (cluster == null)
        throw new IllegalArgumentException("Cluster " + clusterName + " does not exist in database '" + name + "'");
      return cluster;
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public long getSize() {
    try {

      long size = 0;

      for (OCluster c : clusters)
        if (c != null)
          size += c.getRecordsSize();

      return size;

    } catch (IOException ioe) {
      throw new OStorageException("Cannot calculate records size", ioe);
    }
  }

  public int getClusters() {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return clusterMap.size();
    } finally {
      stateLock.releaseReadLock();
    }

  }

  public Set<OCluster> getClusterInstances() {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();
      final Set<OCluster> result = new HashSet<OCluster>();

      // ADD ALL THE CLUSTERS
      for (OCluster c : clusters)
        if (c != null)
          result.add(c);

      return result;

    } finally {
      stateLock.releaseReadLock();
    }
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
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      modificationLock.prohibitModifications(throwException);
      synch();

      try {
        unlock();

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
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void release() {
    try {
      lock();

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

      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

      // CALL BEFORE COMMAND
      Iterable<ODatabaseListener> listeners = db.getListeners();
      for (ODatabaseListener oDatabaseListener : listeners) {
        oDatabaseListener.onBeforeCommand(iCommand, executor);
      }

      Object result = executor.execute(iCommand.getParameters());

      // CALL AFTER COMMAND
      for (ODatabaseListener oDatabaseListener : listeners) {
        oDatabaseListener.onAfterCommand(iCommand, executor, result);
      }

      return result;

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);

    } finally {
      if (Orient.instance().getProfiler().isRecording()) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
        if (db != null) {
          final OSecurityUser user = db.getUser();
          final String userString = user != null ? user.toString() : null;
          Orient.instance().getProfiler().stopChrono(
              "db." + ODatabaseRecordThreadLocal.INSTANCE.get().getName() + ".command." + iCommand.toString(),
              "Command executed against the database", beginTime, "db.*.command.*", null, userString);
        }
      }
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return null;

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {
        checkOpeness();

        final OCluster cluster = getClusterById(currentClusterId);
        return cluster.higherPositions(physicalPosition);
      } catch (IOException ioe) {
        throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return null;

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {
        checkOpeness();

        final OCluster cluster = getClusterById(clusterId);
        return cluster.ceilingPositions(physicalPosition);
      } catch (IOException ioe) {
        throw new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\'', ioe);
      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }

  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return null;

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {
        checkOpeness();

        final OCluster cluster = getClusterById(currentClusterId);

        return cluster.lowerPositions(physicalPosition);
      } catch (IOException ioe) {
        throw new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\'', ioe);
      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return null;

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      dataLock.acquireSharedLock();
      try {
        checkOpeness();

        final OCluster cluster = getClusterById(clusterId);

        return cluster.floorPositions(physicalPosition);
      } catch (IOException ioe) {
        throw new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\'', ioe);
      } finally {
        dataLock.releaseSharedLock();
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void acquireWriteLock(final ORID rid) {
    assert!dataLock.assertSharedLockHold()
        && !dataLock.assertExclusiveLockHold() : " a record lock should not be taken inside a storage lock";
    lockManager.acquireLock(rid, OLockManager.LOCK.EXCLUSIVE, RECORD_LOCK_TIMEOUT);
  }

  public void releaseWriteLock(final ORID rid) {
    assert!dataLock.assertSharedLockHold()
        && !dataLock.assertExclusiveLockHold() : " a record lock should not be released inside a storage lock";
    lockManager.releaseLock(this, rid, OLockManager.LOCK.EXCLUSIVE);
  }

  public void acquireReadLock(final ORID rid) {
    lockManager.acquireLock(rid, OLockManager.LOCK.SHARED, RECORD_LOCK_TIMEOUT);
  }

  public void releaseReadLock(final ORID rid) {
    assert!dataLock.assertSharedLockHold()
        && !dataLock.assertExclusiveLockHold() : " a record lock should not be released inside a storage lock";
    lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
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
      writeCache.flush();
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
    writeCache.lock();
  }

  /**
   * Unlocks all the clusters to allow access outside current process.
   */
  protected void unlock() throws IOException {
    OLogManager.instance().debug(this, "Unlocking storage %s...", name);
    configuration.unlock();
    writeCache.unlock();
  }

  private ORawBuffer readRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final ORecordVersion recordVersion)
      throws ORecordNotFoundException {
    checkOpeness();

    if (!rid.isPersistent())
      throw new IllegalArgumentException(
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doReadRecordIfNotLatest(cluster, rid, recordVersion);
      } finally {
        Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
      }
    }

    stateLock.acquireReadLock();
    try {
      final long timer = Orient.instance().getProfiler().startChrono();
      cluster.getExternalModificationLock().requestModificationLock();
      try {
        lockManager.acquireLock(rid, OLockManager.LOCK.SHARED);
        try {
          ORawBuffer buff;
          dataLock.acquireSharedLock();
          try {
            checkOpeness();

            buff = doReadRecordIfNotLatest(cluster, rid, recordVersion);
            return buff;
          } finally {
            dataLock.releaseSharedLock();
          }
        } finally {
          lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
        }
      } finally {
        cluster.getExternalModificationLock().releaseModificationLock();

        Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
      }
    } finally {
      stateLock.releaseReadLock();
    }

  }

  private ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid) {
    checkOpeness();

    if (!rid.isPersistent())
      throw new IllegalArgumentException(
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

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

    stateLock.acquireReadLock();
    try {
      final long timer = Orient.instance().getProfiler().startChrono();
      clusterSegment.getExternalModificationLock().requestModificationLock();
      try {
        lockManager.acquireLock(rid, OLockManager.LOCK.SHARED);
        try {
          ORawBuffer buff;
          dataLock.acquireSharedLock();
          try {
            checkOpeness();

            buff = doReadRecord(clusterSegment, rid);
            return buff;
          } finally {
            dataLock.releaseSharedLock();
          }
        } finally {
          lockManager.releaseLock(this, rid, OLockManager.LOCK.SHARED);
        }
      } finally {
        clusterSegment.getExternalModificationLock().releaseModificationLock();

        Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private void endStorageTx() throws IOException {
    atomicOperationsManager.endAtomicOperation(false, null);

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
      atomicOperationsManager.startAtomicOperation((String) null, false);
    } catch (RuntimeException e) {
      transaction.set(null);
      throw e;
    }
  }

  private void rollbackStorageTx() throws IOException {
    if (writeAheadLog == null || transaction.get() == null)
      return;

    atomicOperationsManager.endAtomicOperation(true, null);

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
      atomicOperationsManager.startAtomicOperation((String) null, false);
      try {
        ppos = cluster.createRecord(content, recordVersion, recordType);
        rid.clusterPosition = ppos.clusterPosition;

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);

        if (e instanceof OOfflineClusterException)
          throw (OOfflineClusterException) e;

        OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, e);

        try {
          if (ppos.clusterPosition != ORID.CLUSTER_POS_INVALID)
            cluster.deleteRecord(ppos.clusterPosition);
        } catch (IOException ioe) {
          OLogManager.instance().error(this, "Error on removing record in cluster: " + cluster, ioe);
        }

        return null;
      }

      if (callback != null)
        callback.call(rid, ppos.clusterPosition);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Created record %s v.%s size=%d bytes", rid, recordVersion, content.length);

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
        final ORecordVersion oldVersion = ppos.recordVersion.copy();

        final byte[] newContent = checkAndIncrementVersion(cluster, rid, version, ppos.recordVersion, content, recordType);
        assert ppos.recordVersion.compareTo(oldVersion) >= 0;

        if (newContent != null) {
          contentModified = true;
          content = newContent;
        }
      }

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, false);
      try {
        if (updateContent)
          cluster.updateRecord(rid.clusterPosition, content, ppos.recordVersion, recordType);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);

        OLogManager.instance().error(this, "Error on updating record " + rid + " (cluster: " + cluster + ")", e);

        final ORecordVersion recordVersion = OVersionFactory.instance().createUntrackedVersion();
        if (callback != null)
          callback.call(rid, recordVersion);

        return new OStorageOperationResult<ORecordVersion>(recordVersion);
      }

      if (callback != null)
        callback.call(rid, ppos.recordVersion);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Updated record %s v.%s size=%d", rid, ppos.recordVersion, content.length);

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
      atomicOperationsManager.startAtomicOperation((String) null, false);
      try {
        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);

        cluster.deleteRecord(ppos.clusterPosition);
        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);
        return new OStorageOperationResult<Boolean>(false);
      }

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Deleted record %s v.%s", rid, version);

      return new OStorageOperationResult<Boolean>(true);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
    }
  }

  private OStorageOperationResult<Boolean> doHideMethod(ORecordId rid, OCluster cluster) {
    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));

      if (ppos == null)
        // ALREADY HIDDEN
        return new OStorageOperationResult<Boolean>(false);

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, false);
      try {
        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);

        cluster.hideRecord(ppos.clusterPosition);
        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);

        return new OStorageOperationResult<Boolean>(false);
      }

      return new OStorageOperationResult<Boolean>(true);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
    }
  }

  private ORawBuffer doReadRecord(final OCluster clusterSegment, final ORecordId rid) {
    try {
      ORawBuffer buff;
      buff = clusterSegment.readRecord(rid.clusterPosition);

      if (buff != null && OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Read record %s v.%s size=%d bytes", rid, buff.version,
            buff.buffer != null ? buff.buffer.length : 0);

      return buff;
    } catch (IOException e) {
      throw new OStorageException("Error during read of record with rid = " + rid, e);
    }
  }

  private ORawBuffer doReadRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final ORecordVersion recordVersion)
      throws ORecordNotFoundException {
    try {
      return cluster.readRecordIfVersionIsNotLatest(rid.clusterPosition, recordVersion);
    } catch (IOException e) {
      throw new OStorageException("Error during read of record with rid = " + rid, e);
    }
  }

  private void addDefaultClusters() throws IOException {
    final String storageCompression = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);

    final String stgConflictStrategy = getConflictStrategy().getName();

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), OMetadataDefault.CLUSTER_INTERNAL_NAME, null,
            true, 20, 4, storageCompression, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), OMetadataDefault.CLUSTER_INDEX_NAME, null, false,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            storageCompression, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null,
            false, 1, 1, storageCompression, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));

    defaultClusterId = createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), CLUSTER_DEFAULT_NAME, null, true,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            storageCompression, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration config) throws IOException {
    OCluster cluster = clusterMap.get(config.getName().toLowerCase());

    if (cluster != null) {
      cluster.configure(this, config);
      return -1;
    }

    if (config.getStatus() == OStorageClusterConfiguration.STATUS.ONLINE)
      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(config.getName(), configuration.version, this);
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
        throw new OConfigurationException(
            "Cannot add cluster '" + cluster.getName() + "' because it is already registered in database '" + name + "'");
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

      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(clusterName, configuration.version, this);
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

    stateLock.acquireWriteLock();
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

      writeCache.removeLowDiskSpaceListener(this);
      if (writeAheadLog != null)
        writeAheadLog.removeFullCheckpointListener(this);

      if (!onDelete)
        readCache.closeStorage(writeCache);
      else
        readCache.deleteStorage(writeCache);

      if (writeAheadLog != null) {
        writeAheadLog.close();
        if (onDelete)
          writeAheadLog.delete();
      }

      postCloseSteps(onDelete);

      try {
        atomicOperationsManager.unregisterMBean();
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for atomic opeations manager cannot be unregistered.", e);
      }

      status = STATUS.CLOSED;
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, e, OStorageException.class);

    } finally {
      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
      stateLock.releaseWriteLock();
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
      break;

    default:
      // MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (v < -2) {
        // OVERWRITE VERSION: THIS IS USED IN CASE OF FIX OF RECORDS IN DISTRIBUTED MODE
        version.clearRollbackMode();
        iDatabaseVersion.setCounter(version.getCounter());
      } else if (!version.equals(iDatabaseVersion)) {
        final ORecordConflictStrategy strategy = iCluster.getRecordConflictStrategy() != null ? iCluster.getRecordConflictStrategy()
            : recordConflictStrategy;
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
          // USE -2 AS VESION TO AVOID INCREMENTING THE VERSION
          rec.getRecordVersion().copyFrom(updateRecord(rid, ORecordInternal.isContentChanged(rec), stream, new OSimpleVersion(-2),
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

        rec.getRecordVersion().copyFrom(doUpdateRecord(rid, ORecordInternal.isContentChanged(rec), stream, rec.getRecordVersion(),
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

          assert!operationUnits.containsKey(((OAtomicUnitStartRecord) walRecord).getOperationUnitId());

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

        if (reportInterval > 0 && recordsProcessed % reportInterval == 0) {
          OLogManager.instance().info(this, "%d operations were processed, current LSN is %s last LSN is %s", recordsProcessed, lsn,
              writeAheadLog.end());

        }

        lsn = writeAheadLog.next(lsn);
      }
    } catch (OWALPageBrokenException e) {
      OLogManager.instance().error(this,
          "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.");
    } catch (Exception e) {
      OLogManager.instance().error(this,
          "Data restore was paused because of exception. The rest of changes will be rolled back and WAL files will be backed up."
              + " Please report issue about this exception to bug tracker and provide WAL files which are backed up in 'wal_backup' directory.");
      backUpWAL(e);
    }

    return atLeastOnePageUpdate.getValue();
  }

  private void backUpWAL(Exception e) {
    try {
      final File rootDir = new File(configuration.getDirectory());
      final File backUpDir = new File(rootDir, "wal_backup");
      if (!backUpDir.exists()) {
        final boolean created = backUpDir.mkdir();
        if (!created) {
          OLogManager.instance().error(this, "Cannot create directory for backup files " + backUpDir.getAbsolutePath());
          return;
        }
      }

      final Date date = new Date();
      final SimpleDateFormat dateFormat = new SimpleDateFormat("dd_MM_yy_HH_mm_ss");
      final String strDate = dateFormat.format(date);
      final String archiveName = "wal_backup_" + strDate + ".zip";
      final String metadataName = "wal_metadata_" + strDate + ".txt";

      final File archiveFile = new File(backUpDir, archiveName);
      if (!archiveFile.createNewFile()) {
        OLogManager.instance().error(this, "Cannot create backup file " + archiveFile.getAbsolutePath());
        return;
      }

      final FileOutputStream archiveOutputStream = new FileOutputStream(archiveFile);
      final ZipOutputStream archiveZipOutputStream = new ZipOutputStream(new BufferedOutputStream(archiveOutputStream));

      final ZipEntry metadataEntry = new ZipEntry(metadataName);

      archiveZipOutputStream.putNextEntry(metadataEntry);

      final PrintWriter metadataFileWriter = new PrintWriter(archiveZipOutputStream);
      metadataFileWriter.append("Storage name : ").append(getName()).append("\r\n");
      metadataFileWriter.append("Date : ").append(strDate).append("\r\n");
      metadataFileWriter.append("Stacktrace : \r\n");
      e.printStackTrace(metadataFileWriter);
      metadataFileWriter.flush();
      archiveZipOutputStream.closeEntry();

      final List<String> walPaths = ((ODiskWriteAheadLog) writeAheadLog).getWalFiles();
      for (String walSegment : walPaths) {
        archiveEntry(archiveZipOutputStream, walSegment);
      }

      archiveEntry(archiveZipOutputStream, ((ODiskWriteAheadLog) writeAheadLog).getWMRFile());

      archiveZipOutputStream.close();
    } catch (Exception ioe) {
      OLogManager.instance().error(this, "Error during WAL backup.", ioe);
    }

  }

  private void archiveEntry(ZipOutputStream archiveZipOutputStream, String walSegment) throws IOException {
    final File walFile = new File(walSegment);
    final ZipEntry walZipEntry = new ZipEntry(walFile.getName());
    archiveZipOutputStream.putNextEntry(walZipEntry);

    final FileInputStream walInputStream = new FileInputStream(walFile);
    final BufferedInputStream walBufferedInputStream = new BufferedInputStream(walInputStream);

    final byte[] buffer = new byte[1024];
    int readBytes = 0;

    while ((readBytes = walBufferedInputStream.read(buffer)) > -1) {
      archiveZipOutputStream.write(buffer, 0, readBytes);
    }

    walBufferedInputStream.close();

    archiveZipOutputStream.closeEntry();
  }

  protected void restoreAtomicUnit(List<OWALRecord> atomicUnit, OModifiableBoolean atLeastOnePageUpdate) throws IOException {
    assert atomicUnit.get(atomicUnit.size() - 1) instanceof OAtomicUnitEndRecord;

    for (OWALRecord walRecord : atomicUnit) {
      if (walRecord instanceof OFileDeletedWALRecord) {
        OFileDeletedWALRecord fileDeletedWALRecord = (OFileDeletedWALRecord) walRecord;
        if (writeCache.exists(fileDeletedWALRecord.getFileId()))
          readCache.deleteFile(fileDeletedWALRecord.getFileId(), writeCache);
      } else if (walRecord instanceof OFileCreatedWALRecord) {
        OFileCreatedWALRecord fileCreatedCreatedWALRecord = (OFileCreatedWALRecord) walRecord;
        if (writeCache.exists(fileCreatedCreatedWALRecord.getFileName())) {
          readCache.openFile(fileCreatedCreatedWALRecord.getFileName(), fileCreatedCreatedWALRecord.getFileId(), writeCache);
        } else {
          readCache.addFile(fileCreatedCreatedWALRecord.getFileName(), fileCreatedCreatedWALRecord.getFileId(), writeCache);
        }
      } else if (walRecord instanceof OUpdatePageRecord) {
        final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) walRecord;

        final long fileId = updatePageRecord.getFileId();
        final long pageIndex = updatePageRecord.getPageIndex();

        if (!writeCache.isOpen(fileId))
          readCache.openFile(fileId, writeCache);

        OCacheEntry cacheEntry = readCache.load(fileId, pageIndex, true, writeCache);
        if (cacheEntry == null) {
          do {
            if (cacheEntry != null)
              readCache.release(cacheEntry, writeCache);

            cacheEntry = readCache.allocateNewPage(fileId, writeCache);
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
          readCache.release(cacheEntry, writeCache);
        }

        atLeastOnePageUpdate.setValue(true);
      } else if (walRecord instanceof OAtomicUnitStartRecord) {
        continue;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        continue;
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
          writeCache.makeFuzzyCheckpoint();

          if (writeCache.checkLowDiskSpace()) {
            synch();

            if (writeCache.checkLowDiskSpace()) {
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

          writeCache.makeFuzzyCheckpoint();
          if (size <= diskWriteAheadLog.size())
            synch();

          checkpointRequest = false;
        } finally {
          checkpointInProgress.set(false);
        }
      }
    }
  }

  @Override
  public boolean isRemote() {
    return false;
  }
}
