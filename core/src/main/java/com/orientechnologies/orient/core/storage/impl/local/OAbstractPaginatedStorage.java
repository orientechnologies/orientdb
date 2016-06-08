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

import com.orientechnologies.common.concur.lock.OComparableLockManager;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.OUncompletedCommit;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.cache.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;
import com.orientechnologies.orient.core.tx.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public abstract class OAbstractPaginatedStorage extends OStorageAbstract
    implements OLowDiskSpaceListener, OFullCheckpointRequestListener, OIdentifiableStorage, OOrientStartupListener,
    OOrientShutdownListener {
  private static final int RECORD_LOCK_TIMEOUT = OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger();

  private final OComparableLockManager<ORID> lockManager;

  /**
   * Lock is used to atomically update record versions.
   */
  private final OPartitionedLockManager<ORID> recordVersionManager;

  private final String PROFILER_CREATE_RECORD;
  private final String PROFILER_READ_RECORD;
  private final String PROFILER_UPDATE_RECORD;
  private final String PROFILER_DELETE_RECORD;
  private final String PROFILER_RECYCLE_RECORD;
  private final Map<String, OCluster> clusterMap = new HashMap<String, OCluster>();
  private       List<OCluster>        clusters   = new ArrayList<OCluster>();

  private volatile ThreadLocal<OStorageTransaction> transaction          = new ThreadLocal<OStorageTransaction>();
  private final    AtomicBoolean                    checkpointInProgress = new AtomicBoolean();
  protected final OSBTreeCollectionManagerShared sbTreeCollectionManager;

  private final OPerformanceStatisticManager performanceStatisticManager = new OPerformanceStatisticManager(this,
      OGlobalConfiguration.STORAGE_PROFILER_SNAPSHOT_INTERVAL.getValueAsInteger() * 1000000L,
      OGlobalConfiguration.STORAGE_PROFILER_CLEANUP_INTERVAL.getValueAsInteger() * 1000000L);

  protected volatile OWriteAheadLog          writeAheadLog;
  private            OStorageRecoverListener recoverListener;

  protected volatile OReadCache  readCache;
  protected volatile OWriteCache writeCache;

  private volatile ORecordConflictStrategy recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
      .newInstanceOfDefaultClass();

  private volatile int defaultClusterId = -1;
  protected volatile OAtomicOperationsManager atomicOperationsManager;
  private volatile boolean                  wereNonTxOperationsPerformedInPreviousOpen = false;
  private volatile OLowDiskSpaceInformation lowDiskSpace                               = null;
  private volatile boolean                  checkpointRequest                          = false;

  private final int id;

  private Map<String, OIndexEngine> indexEngineNameMap        = new HashMap<String, OIndexEngine>();
  private List<OIndexEngine>        indexEngines              = new ArrayList<OIndexEngine>();
  private boolean                   wereDataRestoredAfterOpen = false;

  private volatile long fullCheckpointCount;

  public OAbstractPaginatedStorage(String name, String filePath, String mode, int id) {
    super(name, filePath, mode, OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsInteger());

    this.id = id;
    lockManager = new ORIDOLockManager(OGlobalConfiguration.COMPONENTS_LOCK_CACHE.getValueAsInteger());
    recordVersionManager = new OPartitionedLockManager<ORID>();

    PROFILER_CREATE_RECORD = "db." + this.name + ".createRecord";
    PROFILER_READ_RECORD = "db." + this.name + ".readRecord";
    PROFILER_UPDATE_RECORD = "db." + this.name + ".updateRecord";
    PROFILER_DELETE_RECORD = "db." + this.name + ".deleteRecord";
    PROFILER_RECYCLE_RECORD = "db." + this.name + ".recyclePosition";

    sbTreeCollectionManager = new OSBTreeCollectionManagerShared(this);
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

      configuration.load(iProperties);
      componentsFactory = new OCurrentStorageComponentsFactory(configuration);

      preOpenSteps();

      try {
        performanceStatisticManager.registerMBean(name, id);
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for profiler cannot be registered.");
      }

      initWalAndDiskCache();

      atomicOperationsManager = new OAtomicOperationsManager(this);
      try {
        atomicOperationsManager.registerMBean();
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for atomic operations manager cannot be registered", e);
      }

      recoverIfNeeded();

      openClusters();
      openIndexes();

      if (OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_OPEN.getValueAsBoolean())
        makeFullCheckpoint();

      writeCache.startFuzzyCheckpoints();

      status = STATUS.OPEN;

      readCache.loadCacheState(writeCache);
    } catch (Exception e) {
      for (OCluster c : clusters) {
        try {
          c.close(false);
        } catch (IOException e1) {
          OLogManager.instance().error(this, "Cannot close cluster after exception on open");
        }
      }

      try {
        status = STATUS.OPEN;
        close(true, false);
      } catch (RuntimeException re) {
        OLogManager.instance().error(this, "Error during storage close", e);
      }

      status = STATUS.CLOSED;

      throw OException.wrapException(new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode), e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  protected void openIndexes() {
    OCurrentStorageComponentsFactory cf = componentsFactory;
    if (cf == null)
      throw new OStorageException("Storage '" + name + "' is not properly initialized");

    final Set<String> indexNames = configuration.indexEngines();
    for (String indexName : indexNames) {
      final OStorageConfiguration.IndexEngineData engineData = configuration.getIndexEngine(indexName);
      final OIndexEngine engine = OIndexes
          .createIndexEngine(engineData.getName(), engineData.getAlgorithm(), engineData.getIndexType(),
              engineData.getDurableInNonTxMode(), this, engineData.getVersion(), engineData.getEngineProperties(), null);

      try {
        engine.load(engineData.getName(), cf.binarySerializerFactory.getObjectSerializer(engineData.getValueSerializerId()),
            engineData.isAutomatic(), cf.binarySerializerFactory.getObjectSerializer(engineData.getKeySerializedId()),
            engineData.getKeyTypes(), engineData.isNullValuesSupport(), engineData.getKeySize(), engineData.getEngineProperties());

        indexEngineNameMap.put(engineData.getName().toLowerCase(configuration.getLocaleInstance()), engine);
        indexEngines.add(engine);
      } catch (RuntimeException e) {
        OLogManager.instance()
            .error(this, "Index '" + engineData.getName() + "' cannot be created and will be removed from configuration", e);

        engine.deleteWithoutLoad(engineData.getName());
      }
    }
  }

  protected void openClusters() throws IOException {
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

          clusterMap.remove(clusters.get(i).getName().toLowerCase(configuration.getLocaleInstance()));

          setCluster(i, null);
        }
      } else {
        setCluster(i, null);
      }
    }
  }

  public void open(final OToken iToken, final Map<String, Object> iProperties) {
    open(iToken.getUserName(), "", iProperties);
  }

  public void create(final Map<String, Object> iProperties) {
    stateLock.acquireWriteLock();
    try {

      if (status != STATUS.CLOSED)
        throw new OStorageExistsException("Cannot create new storage '" + getURL() + "' because it is not closed");

      if (exists())
        throw new OStorageExistsException("Cannot create new storage '" + getURL() + "' because it already exists");

      if (!configuration.getContextConfiguration().getContextKeys()
          .contains(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey())) {
        final String compression = iProperties != null ?
            (String) iProperties
                .get(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey().toLowerCase(configuration.getLocaleInstance())) :
            null;

        if (compression != null)
          configuration.getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, compression);
        else
          configuration.getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD,
              OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getValue());
      }

      if (!configuration.getContextConfiguration().getContextKeys()
          .contains(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey())) {
        final String encryption = iProperties != null ?
            (String) iProperties
                .get(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey().toLowerCase(configuration.getLocaleInstance())) :
            null;

        if (encryption != null)
          configuration.getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, encryption);
        else
          configuration.getContextConfiguration()
              .setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getValue());
      }

      // SAVE COMPRESSION OPTIONS IF ANY. THIS IS USED FOR ENCRYPTION AT REST WHERE IN THE 'STORAGE_ENCRYPTION_KEY' IS STORED
      // THE KEY
      final String encryptionKey = iProperties != null ?
          (String) iProperties
              .get(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey().toLowerCase(configuration.getLocaleInstance())) :
          null;
      if (encryptionKey != null)
        configuration.getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, encryptionKey);
      else
        configuration.getContextConfiguration()
            .setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getValue());

      componentsFactory = new OCurrentStorageComponentsFactory(configuration);
      try {
        performanceStatisticManager.registerMBean(name, id);
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for profiler cannot be registered.");
      }

      initWalAndDiskCache();

      atomicOperationsManager = new OAtomicOperationsManager(this);
      try {
        atomicOperationsManager.registerMBean();
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for atomic operations manager cannot be registered", e);
      }

      preCreateSteps();

      status = STATUS.OPEN;

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      doAddCluster(OMetadataDefault.CLUSTER_INTERNAL_NAME, null);

      configuration.create();

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      doAddCluster(OMetadataDefault.CLUSTER_INDEX_NAME, null);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      doAddCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = doAddCluster(CLUSTER_DEFAULT_NAME, null);

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
      throw OException.wrapException(new OStorageException("Error on creation of storage '" + name + "'"), e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public boolean isClosed() {
    stateLock.acquireReadLock();
    try {
      return super.isClosed();
    } finally {
      stateLock.releaseReadLock();
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

  @Override
  public void close(final boolean force, boolean onDelete) {
    doClose(force, onDelete);
  }

  public void delete() {
    final long timer = Orient.instance().getProfiler().startChrono();

    stateLock.acquireWriteLock();
    try {
      try {
        // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
        close(true, true);

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
        throw OException.wrapException(new OStorageException("Cannot delete database '" + name + "'"), e);
      }
    } finally {
      stateLock.releaseWriteLock();
      Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");

    }
  }

  public boolean check(final boolean verbose, final OCommandOutputListener listener) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
      try {
        checkOpeness();
        final long start = System.currentTimeMillis();

        OPageDataVerificationError[] pageErrors = writeCache.checkStoredPages(verbose ? listener : null);

        listener.onMessage(
            "Check of storage completed in " + (System.currentTimeMillis() - start) + "ms. " + (pageErrors.length > 0 ?
                pageErrors.length + " with errors." :
                " without errors."));

        return pageErrors.length == 0;
      } finally {
        atomicOperationsManager.releaseAtomicOperations(lockId);
      }
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
      return doAddCluster(clusterName, parameters);

    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error in creation of new cluster '" + clusterName), e);
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
        throw new OConfigurationException(
            "Requested cluster ID [" + requestedId + "] is occupied by cluster with name [" + clusters.get(requestedId).getName()
                + "]");
      }

      makeStorageDirty();
      return addClusterInternal(clusterName, requestedId, parameters);

    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error in creation of new cluster '" + clusterName + "'"), e);
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
        throw new IllegalArgumentException(
            "Cluster id '" + clusterId + "' is outside the of range of configured clusters (0-" + (clusters.size() - 1)
                + ") in database '" + name + "'");

      final OCluster cluster = clusters.get(clusterId);
      if (cluster == null)
        return false;

      if (iTruncate)
        cluster.truncate();
      cluster.delete();

      makeStorageDirty();
      clusterMap.remove(cluster.getName().toLowerCase(configuration.getLocaleInstance()));
      clusters.set(clusterId, null);

      // UPDATE CONFIGURATION
      configuration.dropCluster(clusterId);

      return true;
    } catch (Exception e) {
      throw OException.wrapException(new OStorageException("Error while removing cluster '" + clusterId + "'"), e);

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
        throw new IllegalArgumentException(
            "Cluster id '" + clusterId + "' is outside the of range of configured clusters (0-" + (clusters.size() - 1)
                + ") in database '" + name + "'");

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

      clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), newCluster);
      clusters.set(clusterId, newCluster);

      // UPDATE CONFIGURATION
      makeStorageDirty();
      configuration.setClusterStatus(clusterId, iStatus);

      makeFullCheckpoint();
      return true;
    } catch (Exception e) {
      throw OException.wrapException(new OStorageException("Error while removing cluster '" + clusterId + "'"), e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public OReadCache getReadCache() {
    return readCache;
  }

  public OWriteCache getWriteCache() {
    return writeCache;
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
      checkOpeness();

      final OCluster cluster = clusters.get(clusterId);
      if (cluster == null)
        return 0;

      if (countTombstones)
        return cluster.getEntries();

      return cluster.getEntries() - cluster.getTombstonesCount();
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

      return clusters.get(iClusterId) != null ?
          new long[] { clusters.get(iClusterId).getFirstPosition(), clusters.get(iClusterId).getLastPosition() } :
          OCommonConst.EMPTY_LONG_ARRAY;

    } catch (IOException ioe) {
      throw OException.wrapException(new OStorageException("Cannot retrieve information about data range"), ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public OLogSequenceNumber getLSN() {
    if (writeAheadLog == null)
      return null;

    return writeAheadLog.end();
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  /**
   * This method finds all the records which were updated starting from (but not including) current LSN and write result in provided
   * output stream. In output stream will be included all thw records which were updated/deleted/created since passed in LSN till
   * the current moment.
   * <p>
   * Deleted records are written in output stream first, then created/updated records. All records are sorted by record id.
   * <p>
   * Data format:
   * <ol>
   * <li>Amount of records (single entry) - 8 bytes</li>
   * <li>Record's cluster id - 4 bytes</li>
   * <li>Record's cluster position - 8 bytes</li>
   * <li>Delete flag, 1 if record is deleted - 1 byte</li>
   * <li>Record version , only if record is not deleted - 4 bytes</li>
   * <li>Record type, only if record is not deleted - 1 byte</li>
   * <li>Length of binary presentation of record, only if record is not deleted - 4 bytes</li>
   * <li>Binary presentation of the record, only if record is not deleted - length of content is provided in above entity</li>
   * </ol>
   *
   * @param lsn                LSN from which we should find changed records
   * @param stream             Stream which will contain found records
   * @param excludedClusterIds Array of cluster ids to exclude from the export
   * @return Last LSN processed during examination of changed records, or <code>null</code> if it was impossible to find changed
   * records: write ahead log is absent, record with start LSN was not found in WAL, etc.
   * @see OGlobalConfiguration#STORAGE_TRACK_CHANGED_RECORDS_IN_WAL
   */
  public OLogSequenceNumber recordsChangedAfterLSN(final OLogSequenceNumber lsn, final OutputStream stream,
      final Set<String> excludedClusterIds) {
    if (!OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getValueAsBoolean())
      throw new IllegalStateException(
          "Cannot find records which were changed starting from provided LSN because tracking of rids of changed records in WAL is switched off, "
              + "to switch it on please set property " + OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getKey()
              + " to the true value, please note that only records"
              + " which are stored after this property was set will be retrieved");

    stateLock.acquireReadLock();
    try {
      if (writeAheadLog == null) {
        return null;
      }

      // we iterate till the last record is contained in wal at the moment when we call this method
      final OLogSequenceNumber endLsn = writeAheadLog.end();

      if (endLsn == null || lsn.compareTo(endLsn) >= 0) {
        return null;
      }

      // container of rids of changed records
      final SortedSet<ORID> sortedRids = new TreeSet<ORID>();

      final OLogSequenceNumber startLsn = writeAheadLog.next(lsn);

      writeAheadLog.preventCutTill(startLsn);
      try {
        // start record is absent there is nothing that we can do
        OWALRecord walRecord = writeAheadLog.read(startLsn);
        if (walRecord == null) {
          OLogManager.instance().info(this, "Cannot find requested LSN=%s for database sync operation", lsn);
          return null;
        }

        OLogSequenceNumber currentLsn = startLsn;

        // all information about changed records is contained in atomic operation metadata
        while (currentLsn != null && endLsn.compareTo(currentLsn) >= 0) {
          walRecord = writeAheadLog.read(currentLsn);

          if (walRecord instanceof OFileCreatedWALRecord)
            throw new ODatabaseException(
                "Cannot execute delta-sync because a new file has been added. Filename: " + ((OFileCreatedWALRecord) walRecord)
                    .getFileName() + "(id=" + ((OFileCreatedWALRecord) walRecord).getFileId() + ")");

          if (walRecord instanceof OFileDeletedWALRecord)
            throw new ODatabaseException(
                "Cannot execute delta-sync because a file has been deleted. File id: " + ((OFileDeletedWALRecord) walRecord)
                    .getFileId());

          if (walRecord instanceof OAtomicUnitEndRecord) {
            final OAtomicUnitEndRecord atomicUnitEndRecord = (OAtomicUnitEndRecord) walRecord;
            if (atomicUnitEndRecord.getAtomicOperationMetadata().containsKey(ORecordOperationMetadata.RID_METADATA_KEY)) {
              final ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) atomicUnitEndRecord
                  .getAtomicOperationMetadata().get(ORecordOperationMetadata.RID_METADATA_KEY);
              final Set<ORID> rids = recordOperationMetadata.getValue();
              for (ORID rid : rids) {
                if (!excludedClusterIds.contains(getPhysicalClusterNameById(rid.getClusterId())))
                  sortedRids.add(rid);
              }
            }
          }

          currentLsn = writeAheadLog.next(currentLsn);
        }
      } finally {
        writeAheadLog.preventCutTill(null);
      }

      OLogManager.instance().info(this, "Exporting records after LSN=%s. Found %d records", lsn, sortedRids.size());

      // records may be deleted after we flag them as existing and as result rule of sorting of records
      // (deleted records go first will be broken), so we prohibit any modifications till we do not complete method execution
      final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
      try {
        final DataOutputStream dataOutputStream = new DataOutputStream(stream);
        try {

          dataOutputStream.writeLong(sortedRids.size());

          Iterator<ORID> ridIterator = sortedRids.iterator();
          while (ridIterator.hasNext()) {
            final ORID rid = ridIterator.next();
            final OCluster cluster = clusters.get(rid.getClusterId());

            // we do not need to load record only check it's presence
            if (cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition())) == null) {
              dataOutputStream.writeInt(rid.getClusterId());
              dataOutputStream.writeLong(rid.getClusterPosition());
              dataOutputStream.write(1);

              OLogManager.instance().debug(this, "Exporting deleted record %s", rid);

              // delete to avoid duplication
              ridIterator.remove();
            }
          }

          ridIterator = sortedRids.iterator();
          while (ridIterator.hasNext()) {
            final ORID rid = ridIterator.next();
            final OCluster cluster = clusters.get(rid.getClusterId());

            dataOutputStream.writeInt(rid.getClusterId());
            dataOutputStream.writeLong(rid.getClusterPosition());

            if (cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition())) == null) {
              dataOutputStream.writeBoolean(true);
              OLogManager.instance().debug(this, "Exporting deleted record %s", rid);
            } else {
              final ORawBuffer rawBuffer = cluster.readRecord(rid.getClusterPosition());
              assert rawBuffer != null;

              dataOutputStream.writeBoolean(false);
              dataOutputStream.writeInt(rawBuffer.version);
              dataOutputStream.write(rawBuffer.recordType);
              dataOutputStream.writeInt(rawBuffer.buffer.length);
              dataOutputStream.write(rawBuffer.buffer);

              OLogManager.instance()
                  .debug(this, "Exporting modified record rid=%s type=%d size=%d v=%d - buffer size=%d", rid, rawBuffer.recordType,
                      rawBuffer.buffer.length, rawBuffer.version, dataOutputStream.size());
            }
          }
        } finally {
          dataOutputStream.close();
        }
      } finally {
        atomicOperationsManager.releaseAtomicOperations(lockId);
      }

      return endLsn;
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error of reading of records changed after LSN " + lsn), e);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    checkOpeness();

    long tot = 0;

    stateLock.acquireReadLock();
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
      stateLock.releaseReadLock();
    }

  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId rid, final byte[] content, final int recordVersion,
      final byte recordType, final int mode, final ORecordCallback<Long> callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final OPhysicalPosition ppos = new OPhysicalPosition(recordType);
    final OCluster cluster = getClusterById(rid.clusterId);

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos, null);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer, "db.*.createRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos, null);
    } finally {
      stateLock.releaseReadLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer, "db.*.createRecord");

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
      checkOpeness();

      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
      if (ppos == null)
        return null;

      return new ORecordMetadata(rid, ppos.recordVersion);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
    } finally {
      stateLock.releaseReadLock();
    }

    return null;
  }

  public void scanCluster(final String iClusterName, final boolean iAscendingOrder, long iFrom, long iTo,
      final OCallable<Boolean, ORecord> iCallback) throws IOException {
    checkOpeness();
    final OCluster cluster = getClusterByName(iClusterName);

    if (cluster instanceof OOfflineCluster)
      return;

    if (!(cluster instanceof OPaginatedCluster))
      throw new IllegalArgumentException("Cluster '" + iClusterName + "' (type=" + cluster.getClass() + ") does not support scan");

    final long scanBatchSize = OGlobalConfiguration.QUERY_SCAN_BATCH_SIZE.getValueAsLong();

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        if (scanBatchSize > 0) {
          long lastPos;
          if (iAscendingOrder) {
            do {
              lastPos = ((OPaginatedCluster) cluster).scan(true, iFrom, iTo, scanBatchSize, iCallback);
              iFrom = lastPos;
            } while (lastPos > 0);
          } else {
            do {
              lastPos = ((OPaginatedCluster) cluster).scan(false, iFrom, iTo, scanBatchSize, iCallback);
              iTo = lastPos;
            } while (lastPos > 0);
          }
        } else
          ((OPaginatedCluster) cluster).scan(iAscendingOrder, iFrom, iTo, 0, iCallback);

        return;
      } finally {
        Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
      }
    }

    if (scanBatchSize > 0) {
      long lastPos;
      if (iAscendingOrder) {
        do {
          lastPos = scanClusterInLock(true, (OPaginatedCluster) cluster, iFrom, iTo, scanBatchSize, iCallback);
          iFrom = lastPos;
        } while (lastPos > 0);
      } else {
        do {
          lastPos = scanClusterInLock(false, (OPaginatedCluster) cluster, iFrom, iTo, scanBatchSize, iCallback);
          iTo = lastPos;
        } while (lastPos > 0);
      }
    } else
      scanClusterInLock(iAscendingOrder, (OPaginatedCluster) cluster, iFrom, iTo, 0, iCallback);
  }

  protected long scanClusterInLock(final boolean iAscendingOrder, final OPaginatedCluster cluster, final long iFrom, final long iTo,
      final long iLimit, final OCallable<Boolean, ORecord> iCallback) throws IOException {
    stateLock.acquireReadLock();
    try {
      return cluster.scan(iAscendingOrder, iFrom, iTo, iLimit, iCallback);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback) {
    checkOpeness();
    final OCluster cluster;
    try {
      cluster = getClusterById(iRid.clusterId);
    } catch (IllegalArgumentException e) {
      throw OException.wrapException(new ORecordNotFoundException(iRid), e);
    }

    return new OStorageOperationResult<ORawBuffer>(readRecord(cluster, iRid));
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecordIfNotLatest(getClusterById(rid.clusterId), rid, recordVersion));
  }

  @Override
  public OStorageOperationResult<Integer> updateRecord(final ORecordId rid, final boolean updateContent, final byte[] content,
      final int version, final byte recordType, final int mode, final ORecordCallback<Integer> callback) {
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
    stateLock.acquireReadLock();
    try {
      // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
      final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
      try {
        checkOpeness();

        // UPDATE IT
        return doUpdateRecord(rid, updateContent, content, version, recordType, callback, cluster);
      } finally {
        lock.unlock();
      }
    } finally {
      stateLock.releaseReadLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer, "db.*.updateRecord");
    }
  }

  @Override
  public OStorageOperationResult<Integer> recyclePosition(final ORecordId rid, final byte[] content, final int version,
      final byte recordType) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final OCluster cluster = getClusterById(rid.clusterId);
    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doRecycleRecord(rid, content, version, cluster, recordType);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_RECYCLE_RECORD, "Recycled a record position in database", timer, "db.*.recyclePosition");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
      final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
      try {
        checkOpeness();

        // RECYCLING IT
        return doRecycleRecord(rid, content, version, cluster, recordType);
      } finally {
        lock.unlock();
      }
    } finally {
      stateLock.releaseReadLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_RECYCLE_RECORD, "Recycled a record to database", timer, "db.*.recyclePosition");
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
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId rid, final int version, final int mode,
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
    stateLock.acquireReadLock();

    try {
      checkOpeness();
      return doDeleteRecord(rid, version, cluster);
    } finally {
      stateLock.releaseReadLock();
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
        return doHideMethod(rid, cluster);
      } finally {
        Orient.instance().getProfiler()
            .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
      try {
        checkOpeness();

        return doHideMethod(rid, cluster);
      } finally {
        lock.unlock();
      }
    } finally {
      stateLock.releaseReadLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");

    }
  }

  public OPerformanceStatisticManager getPerformanceStatisticManager() {
    return performanceStatisticManager;
  }

  /**
   * Starts to gather information about storage performance for current thread. Details which performance characteristics are
   * gathered can be found at {@link OSessionStoragePerformanceStatistic}.
   *
   * @see #completeGatheringPerformanceStatisticForCurrentThread()
   */
  public void startGatheringPerformanceStatisticForCurrentThread() {
    performanceStatisticManager.startThreadMonitoring();
  }

  /**
   * Completes gathering performance characteristics for current thread initiated by call of
   * {@link #startGatheringPerformanceStatisticForCurrentThread()}
   *
   * @return Performance statistic gathered after call of {@link #startGatheringPerformanceStatisticForCurrentThread()} or
   * <code>null</code> if profiling of storage was not started.
   */
  public OSessionStoragePerformanceStatistic completeGatheringPerformanceStatisticForCurrentThread() {
    return performanceStatisticManager.stopThreadMonitoring();
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    stateLock.acquireReadLock();
    try {
      if (iExclusiveLock) {
        return super.callInLock(iCallable, true);
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

      final OCluster segment = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
      if (segment != null)
        return segment.getId();

      return -1;
    } finally {
      stateLock.releaseReadLock();
    }

  }

  public List<ORecordOperation> commit(final OTransaction clientTx, Runnable callback) {
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();

    final ODatabaseDocumentInternal databaseRecord = (ODatabaseDocumentInternal) clientTx.getDatabase();
    final Map<String, OIndexInternal<?>> indexesToCommit = getChangedIndexes(clientTx,
        databaseRecord.getMetadata().getIndexManager());

    ((OMetadataInternal) databaseRecord.getMetadata()).makeThreadLocalSchemaSnapshot();

    final Iterable<ORecordOperation> entries = (Iterable<ORecordOperation>) clientTx.getAllRecordEntries();

    for (ORecordOperation txEntry : entries) {
      if (txEntry.type == ORecordOperation.CREATED || txEntry.type == ORecordOperation.UPDATED) {
        final ORecord record = txEntry.getRecord();
        if (record instanceof ODocument)
          ((ODocument) record).validate();
      }
    }

    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    stateLock.acquireReadLock();
    try {
      try {
        dataLock.acquireExclusiveLock();
        try {

          checkOpeness();

          if (writeAheadLog == null && clientTx.isUsingLog())
            throw new OStorageException("WAL mode is not active. Transactions are not supported in given mode");

          makeStorageDirty();
          startStorageTx(clientTx);

          Map<ORecordOperation, OPhysicalPosition> positions = new IdentityHashMap<ORecordOperation, OPhysicalPosition>();
          for (ORecordOperation txEntry : entries) {
            ORecord rec = txEntry.getRecord();

            if (txEntry.type == ORecordOperation.CREATED && rec.isDirty()) {
              ORecordId rid = (ORecordId) rec.getIdentity().copy();
              ORecordId oldRID = rid.copy();
              int clusterId = rid.clusterId;
              if (rid.clusterId == ORID.CLUSTER_ID_INVALID && rec instanceof ODocument
                  && ODocumentInternal.getImmutableSchemaClass(((ODocument) rec)) != null) {
                // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS
                final OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) rec));
                clusterId = schemaClass.getClusterForNewInstance((ODocument) rec);
              }

              final OCluster cluster = getClusterById(clusterId);
              OPhysicalPosition ppos = cluster.allocatePosition(ORecordInternal.getRecordType(rec));
              positions.put(txEntry, ppos);
              rid.clusterId = cluster.getId();

              if (rid.clusterPosition > -1 && rid.clusterPosition != ppos.clusterPosition)
                throw new OTransactionException(
                    "New record allocated #" + rid.clusterId + ":" + ppos.clusterPosition + " but the expected was " + rid);

              rid.clusterPosition = ppos.clusterPosition;

              clientTx.updateIdentityAfterCommit(oldRID, rid);
            }
          }

          for (ORecordOperation txEntry : entries) {
            commitEntry(txEntry, positions.get(txEntry));
            result.add(txEntry);
          }

          commitIndexes(clientTx, indexesToCommit);

          endStorageTx();

          OTransactionAbstract.updateCacheFromEntries(clientTx, entries, true);

        } catch (IOException ioe) {
          makeRollback(clientTx, ioe);
        } catch (RuntimeException e) {
          makeRollback(clientTx, e);
        } finally {
          transaction.set(null);
          dataLock.releaseExclusiveLock();
        }
      } finally {
        ((OMetadataInternal) databaseRecord.getMetadata()).clearThreadLocalSchemaSnapshot();
      }
    } finally {
      stateLock.releaseReadLock();
    }

    return result;
  }

  private void commitIndexes(OTransaction clientTx, final Map<String, OIndexInternal<?>> indexesToCommit) {
    assert clientTx instanceof OTransactionOptimistic;

    for (OIndexInternal<?> indexInternal : indexesToCommit.values())
      indexInternal.preCommit();

    Map<String, OTransactionIndexChanges> indexChanges = ((OTransactionOptimistic) clientTx).getIndexEntries();

    for (Map.Entry<String, OTransactionIndexChanges> indexEntry : indexChanges.entrySet()) {
      final OIndexInternal<?> index = indexesToCommit.get(indexEntry.getKey()).getInternal();

      if (index == null) {
        OLogManager.instance().error(this, "Index with name '" + indexEntry.getKey() + "' was not found.");
        throw new OIndexException("Index with name '" + indexEntry.getKey() + "' was not found.");
      } else
        index.addTxOperation(indexEntry.getValue());
    }

    try {
      for (OIndexInternal<?> indexInternal : indexesToCommit.values())
        indexInternal.commit();
    } finally {
      for (OIndexInternal<?> indexInternal : indexesToCommit.values())
        indexInternal.postCommit();
    }
  }

  private Map<String, OIndexInternal<?>> getChangedIndexes(OTransaction clientTx, OIndexManager manager) {
    assert clientTx instanceof OTransactionOptimistic;

    Map<String, OTransactionIndexChanges> indexChanges = ((OTransactionOptimistic) clientTx).getIndexEntries();

    final Map<String, OIndexInternal<?>> indexesToCommit = new HashMap<String, OIndexInternal<?>>();

    for (Map.Entry<String, OTransactionIndexChanges> indexChange : indexChanges.entrySet()) {
      final OIndex<?> idx = manager.getIndex(indexChange.getKey());
      if (idx == null)
        throw new OTransactionException("Cannot find index '" + indexChange.getKey() + "' while committing transaction");

      final OIndexInternal<?> index = idx.getInternal();
      indexesToCommit.put(index.getName(), index.getInternal());
    }
    return indexesToCommit;
  }

  @Override
  public OUncompletedCommit<List<ORecordOperation>> initiateCommit(OTransaction clientTx, Runnable callback) {
    boolean success = false;
    checkOpeness();
    checkLowDiskSpaceAndFullCheckpointRequests();
    final ODatabaseDocumentInternal databaseRecord = (ODatabaseDocumentInternal) clientTx.getDatabase();

    final Map<String, OIndexInternal<?>> indexesToCommit = getChangedIndexes(clientTx,
        databaseRecord.getMetadata().getIndexManager());

    ((OMetadataInternal) databaseRecord.getMetadata()).makeThreadLocalSchemaSnapshot();
    final Iterable<ORecordOperation> entries = (Iterable<ORecordOperation>) clientTx.getAllRecordEntries();

    for (ORecordOperation txEntry : entries) {
      if (txEntry.type == ORecordOperation.CREATED || txEntry.type == ORecordOperation.UPDATED) {
        final ORecord record = txEntry.getRecord();
        if (record instanceof ODocument)
          ((ODocument) record).validate();
      }
    }

    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    stateLock.acquireReadLock();
    try {
      try {
        dataLock.acquireExclusiveLock();
        try {
          checkOpeness();

          if (writeAheadLog == null && clientTx.isUsingLog())
            throw new OStorageException("WAL mode is not active. Transactions are not supported in given mode");

          makeStorageDirty();
          startStorageTx(clientTx);

          Map<ORecordOperation, OPhysicalPosition> positions = new IdentityHashMap<ORecordOperation, OPhysicalPosition>();
          for (ORecordOperation txEntry : entries) {
            ORecord rec = txEntry.getRecord();

            if (txEntry.type == ORecordOperation.CREATED && rec.isDirty()) {
              ORecordId rid = (ORecordId) rec.getIdentity().copy();
              ORecordId oldRID = rid.copy();
              int clusterId = rid.clusterId;
              if (rid.clusterId == ORID.CLUSTER_ID_INVALID && rec instanceof ODocument
                  && ODocumentInternal.getImmutableSchemaClass(((ODocument) rec)) != null) {
                // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS
                final OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) rec));
                clusterId = schemaClass.getClusterForNewInstance((ODocument) rec);
              }

              final OCluster cluster = getClusterById(clusterId);
              OPhysicalPosition ppos = cluster.allocatePosition(ORecordInternal.getRecordType(rec));
              positions.put(txEntry, ppos);
              rid.clusterId = cluster.getId();

              if (rid.clusterPosition > -1 && rid.clusterPosition != ppos.clusterPosition)
                throw new OTransactionException(
                    "New record allocated #" + rid.clusterId + ":" + ppos.clusterPosition + " but the expected was " + rid);

              rid.clusterPosition = ppos.clusterPosition;

              clientTx.updateIdentityAfterCommit(oldRID, rid);
            }
          }

          for (ORecordOperation txEntry : entries) {
            commitEntry(txEntry, positions.get(txEntry));
            result.add(txEntry);
          }

          commitIndexes(clientTx, indexesToCommit);

          final OUncompletedCommit<List<ORecordOperation>> uncompletedCommit = new UncompletedCommit(clientTx, entries, result,
              atomicOperationsManager.initiateCommit());
          success = true;
          return uncompletedCommit;
        } catch (IOException ioe) {
          makeRollback(clientTx, ioe);
        } catch (RuntimeException e) {
          makeRollback(clientTx, e);
        } finally {
          dataLock.releaseExclusiveLock();
        }
      } finally {
        if (!success)
          ((OMetadataInternal) databaseRecord.getMetadata()).clearThreadLocalSchemaSnapshot();
      }
    } finally {
      stateLock.releaseReadLock();
    }

    // not reachable
    assert false;
    return null;
  }

  public int loadIndexEngine(String name) {
    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final OIndexEngine engine = indexEngineNameMap.get(name.toLowerCase(configuration.getLocaleInstance()));
      if (engine == null)
        return -1;

      final int indexId = indexEngines.indexOf(engine);
      assert indexId >= 0;

      return indexId;
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public int loadExternalIndexEngine(String engineName, String algorithm, String indexType, OIndexDefinition indexDefinition,
      OBinarySerializer valueSerializer, boolean isAutomatic, Boolean durableInNonTxMode, int version,
      Map<String, String> engineProperties) {
    checkOpeness();

    stateLock.acquireWriteLock();
    try {
      checkOpeness();

      checkLowDiskSpaceAndFullCheckpointRequests();

      // this method introduced for binary compatibility only
      if (configuration.binaryFormatVersion > 15)
        return -1;

      final String originalName = engineName;
      engineName = engineName.toLowerCase(configuration.getLocaleInstance());

      if (indexEngineNameMap.containsKey(engineName))
        throw new OIndexException("Index with name " + engineName + " already exists");

      makeStorageDirty();

      final OBinarySerializer keySerializer = determineKeySerializer(indexDefinition);
      final int keySize = determineKeySize(indexDefinition);
      final OType[] keyTypes = indexDefinition != null ? indexDefinition.getTypes() : null;
      final boolean nullValuesSupport = indexDefinition != null && !indexDefinition.isNullValuesIgnored();

      final OStorageConfiguration.IndexEngineData engineData = new OStorageConfiguration.IndexEngineData(originalName, algorithm,
          indexType, durableInNonTxMode, version, valueSerializer.getId(), keySerializer.getId(), isAutomatic, keyTypes,
          nullValuesSupport, keySize, engineProperties);

      final OIndexEngine engine = OIndexes
          .createIndexEngine(originalName, algorithm, indexType, durableInNonTxMode, this, version, engineProperties, null);
      engine.load(originalName, valueSerializer, isAutomatic, keySerializer, keyTypes, nullValuesSupport, keySize,
          engineData.getEngineProperties());

      indexEngineNameMap.put(engineName, engine);
      indexEngines.add(engine);
      configuration.addIndexEngine(engineName, engineData);

      return indexEngines.size() - 1;
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public int addIndexEngine(String engineName, final String algorithm, final String indexType,
      final OIndexDefinition indexDefinition, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final Boolean durableInNonTxMode, final int version, final Map<String, String> engineProperties,
      final Set<String> clustersToIndex, final ODocument metadata) {
    checkOpeness();

    stateLock.acquireWriteLock();
    try {
      checkOpeness();

      checkLowDiskSpaceAndFullCheckpointRequests();

      final String originalName = engineName;
      engineName = engineName.toLowerCase(configuration.getLocaleInstance());

      if (indexEngineNameMap.containsKey(engineName))
        throw new OIndexException("Index with name " + engineName + " already exists");

      makeStorageDirty();

      final OBinarySerializer keySerializer = determineKeySerializer(indexDefinition);
      final int keySize = determineKeySize(indexDefinition);
      final OType[] keyTypes = indexDefinition != null ? indexDefinition.getTypes() : null;
      final boolean nullValuesSupport = indexDefinition != null && !indexDefinition.isNullValuesIgnored();
      final byte serializerId;

      if (valueSerializer != null)
        serializerId = valueSerializer.getId();
      else
        serializerId = -1;

      final OIndexEngine engine = OIndexes
          .createIndexEngine(originalName, algorithm, indexType, durableInNonTxMode, this, version, engineProperties, metadata);

      engine.create(valueSerializer, isAutomatic, keyTypes, nullValuesSupport, keySerializer, keySize, clustersToIndex,
          engineProperties, metadata);

      indexEngineNameMap.put(engineName, engine);

      indexEngines.add(engine);

      final OStorageConfiguration.IndexEngineData engineData = new OStorageConfiguration.IndexEngineData(originalName, algorithm,
          indexType, durableInNonTxMode, version, serializerId, keySerializer.getId(), isAutomatic, keyTypes, nullValuesSupport,
          keySize, engineProperties);

      configuration.addIndexEngine(engineName, engineData);

      return indexEngines.size() - 1;
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  private int determineKeySize(OIndexDefinition indexDefinition) {
    if (indexDefinition == null || indexDefinition instanceof ORuntimeKeyIndexDefinition)
      return 1;
    else
      return indexDefinition.getTypes().length;
  }

  private OBinarySerializer determineKeySerializer(OIndexDefinition indexDefinition) {
    final OBinarySerializer keySerializer;
    if (indexDefinition != null) {
      if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
        keySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();
      } else {
        if (indexDefinition.getTypes().length > 1) {
          keySerializer = OCompositeKeySerializer.INSTANCE;
        } else {
          OCurrentStorageComponentsFactory currentStorageComponentsFactory = componentsFactory;
          if (currentStorageComponentsFactory != null)
            keySerializer = currentStorageComponentsFactory.binarySerializerFactory
                .getObjectSerializer(indexDefinition.getTypes()[0]);
          else
            throw new IllegalStateException("Cannot load binary serializer, storage is not porperly initialized");
        }
      }
    } else {
      keySerializer = new OSimpleKeySerializer();
    }
    return keySerializer;
  }

  public void deleteIndexEngine(int indexId) {
    checkOpeness();

    stateLock.acquireWriteLock();
    try {
      checkOpeness();

      checkLowDiskSpaceAndFullCheckpointRequests();

      checkIndexId(indexId);

      makeStorageDirty();
      final OIndexEngine engine = indexEngines.get(indexId);

      indexEngines.set(indexId, null);

      engine.delete();

      final String engineName = engine.getName().toLowerCase(configuration.getLocaleInstance());
      indexEngineNameMap.remove(engineName);
      configuration.deleteIndexEngine(engineName);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error on index deletion"), e);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  private void checkIndexId(int indexId) {
    if (indexId < 0 || indexId >= indexEngines.size())
      throw new OIndexException("Engine with id " + indexId + " is not registered inside of storage");
  }

  public boolean indexContainsKey(int indexId, Object key) {
    if (transaction.get() != null)
      return doIndexContainsKey(indexId, key);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      return doIndexContainsKey(indexId, key);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private boolean doIndexContainsKey(int indexId, Object key) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.contains(key);
  }

  public boolean removeKeyFromIndex(int indexId, Object key) {
    if (transaction.get() != null) {
      return doRemoveKeyFromIndex(indexId, key);
    }

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      checkLowDiskSpaceAndFullCheckpointRequests();

      return doRemoveKeyFromIndex(indexId, key);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private boolean doRemoveKeyFromIndex(int indexId, Object key) {
    try {
      checkIndexId(indexId);

      makeStorageDirty();
      final OIndexEngine engine = indexEngines.get(indexId);

      return engine.remove(key);
    } catch (IOException e) {
      throw new OStorageException("Error during removal of entry with key " + key + " from index ");
    }
  }

  public void clearIndex(int indexId) {
    if (transaction.get() != null) {
      doClearIndex(indexId);
      return;
    }

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      checkLowDiskSpaceAndFullCheckpointRequests();

      doClearIndex(indexId);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private void doClearIndex(int indexId) {
    try {
      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);

      makeStorageDirty();
      engine.clear();
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during clearing of index"), e);
    }

  }

  public Object getIndexValue(int indexId, Object key) {
    if (transaction.get() != null)
      return doGetIndexValue(indexId, key);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexValue(indexId, key);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private Object doGetIndexValue(int indexId, Object key) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.get(key);
  }

  public OIndexEngine getIndexEngine(int indexId) {
    checkIndexId(indexId);
    return indexEngines.get(indexId);
  }

  public void updateIndexEntry(int indexId, Object key, Callable<Object> valueCreator) {
    if (transaction.get() != null) {
      doUpdateIndexEntry(indexId, key, valueCreator);
      return;
    }

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      checkLowDiskSpaceAndFullCheckpointRequests();

      doUpdateIndexEntry(indexId, key, valueCreator);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public <T> T callIndexEngine(boolean atomicOperation, boolean readOperation, int indexId, OIndexEngineCallback<T> callback) {
    if (transaction.get() != null)
      return doCallIndexEngine(atomicOperation, readOperation, indexId, callback);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      return doCallIndexEngine(atomicOperation, readOperation, indexId, callback);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private <T> T doCallIndexEngine(boolean atomicOperation, boolean readOperation, int indexId, OIndexEngineCallback<T> callback) {
    checkIndexId(indexId);
    try {
      if (atomicOperation)
        atomicOperationsManager.startAtomicOperation((String) null, true);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
    }

    try {

      if (!readOperation)
        makeStorageDirty();

      final OIndexEngine engine = indexEngines.get(indexId);
      T result = callback.callEngine(engine);

      if (atomicOperation)
        atomicOperationsManager.endAtomicOperation(false, null);

      return result;
    } catch (Exception e) {
      try {
        if (atomicOperation)
          atomicOperationsManager.endAtomicOperation(true, e);

        throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Error during operation rollback"), ioe);
      }
    }

  }

  private void doUpdateIndexEntry(int indexId, Object key, Callable<Object> valueCreator) {
    try {
      atomicOperationsManager.startAtomicOperation((String) null, true);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
    }

    try {

      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      final Object value = valueCreator.call();
      if (value == null)
        engine.remove(key);
      else
        engine.put(key, value);

      atomicOperationsManager.endAtomicOperation(false, null);
    } catch (Exception e) {
      try {
        atomicOperationsManager.endAtomicOperation(true, e);
        throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Error during operation rollback"), ioe);
      }
    }
  }

  public void putIndexValue(int indexId, Object key, Object value) {
    if (transaction.get() != null) {
      doPutIndexValue(indexId, key, value);
      return;
    }

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      checkLowDiskSpaceAndFullCheckpointRequests();

      doPutIndexValue(indexId, key, value);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private void doPutIndexValue(int indexId, Object key, Object value) {
    try {
      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      engine.put(key, value);
    } catch (IOException e) {
      throw new OStorageException("Cannot put key " + key + " value " + value + " entry to the index");
    }
  }

  public Object getIndexFirstKey(int indexId) {
    if (transaction.get() != null)
      return doGetIndexFirstKey(indexId);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexFirstKey(indexId);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private Object doGetIndexFirstKey(int indexId) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.getFirstKey();
  }

  public Object getIndexLastKey(int indexId) {
    if (transaction.get() != null)
      return doGetIndexFirstKey(indexId);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexLastKey(indexId);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private Object doGetIndexLastKey(int indexId) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.getLastKey();
  }

  public OIndexCursor iterateIndexEntriesBetween(int indexId, Object rangeFrom, boolean fromInclusive, Object rangeTo,
      boolean toInclusive, boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer) {
    if (transaction.get() != null)
      return doIterateIndexEntriesBetween(indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doIterateIndexEntriesBetween(indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OIndexCursor doIterateIndexEntriesBetween(int indexId, Object rangeFrom, boolean fromInclusive, Object rangeTo,
      boolean toInclusive, boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateIndexEntriesMajor(int indexId, Object fromKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) {
    if (transaction.get() != null)
      return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OIndexCursor doIterateIndexEntriesMajor(int indexId, Object fromKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateIndexEntriesMinor(int indexId, final Object toKey, final boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) {

    if (transaction.get() != null)
      return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OIndexCursor doIterateIndexEntriesMinor(int indexId, Object toKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor getIndexCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer) {
    if (transaction.get() != null)
      return doGetIndexCursor(indexId, valuesTransformer);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexCursor(indexId, valuesTransformer);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OIndexCursor doGetIndexCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.cursor(valuesTransformer);
  }

  public OIndexCursor getIndexDescCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer) {
    if (transaction.get() != null)
      return doGetIndexDescCursor(indexId, valuesTransformer);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexDescCursor(indexId, valuesTransformer);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OIndexCursor doGetIndexDescCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.descCursor(valuesTransformer);
  }

  public OIndexKeyCursor getIndexKeyCursor(int indexId) {
    if (transaction.get() != null)
      return doGetIndexKeyCursor(indexId);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexKeyCursor(indexId);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OIndexKeyCursor doGetIndexKeyCursor(int indexId) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.keyCursor();
  }

  public long getIndexSize(int indexId, OIndexEngine.ValuesTransformer transformer) {
    if (transaction.get() != null)
      return doGetIndexSize(indexId, transformer);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doGetIndexSize(indexId, transformer);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private long doGetIndexSize(int indexId, OIndexEngine.ValuesTransformer transformer) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.size(transformer);
  }

  public boolean hasIndexRangeQuerySupport(int indexId) {
    if (transaction.get() != null)
      return doHasRangeQuerySupport(indexId);

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();
      return doHasRangeQuerySupport(indexId);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private boolean doHasRangeQuerySupport(int indexId) {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.hasRangeQuerySupport();
  }

  private void makeRollback(OTransaction clientTx, Exception e) {
    // WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
    OLogManager.instance()
        .debug(this, "Error during transaction commit, transaction will be rolled back (tx-id=%d)", e, clientTx.getId());
    rollback(clientTx);
    if (e instanceof OException)
      throw ((OException) e);
    else
      throw OException.wrapException(new OStorageException("Error during transaction commit"), e);

  }

  public void rollback(final OTransaction clientTx) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
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
        throw OException.wrapException(new OStorageException("Error during transaction rollback"), e);
      } finally {
        transaction.set(null);
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return ppos != null && !ORecordVersionHelper.isTombstone(ppos.recordVersion);
  }

  public void synch() {
    checkOpeness();

    stateLock.acquireReadLock();
    try {
      final long timer = Orient.instance().getProfiler().startChrono();
      final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
      try {
        checkOpeness();

        for (OIndexEngine indexEngine : indexEngines)
          try {
            if (indexEngine != null)
              indexEngine.flush();
          } catch (Throwable t) {
            OLogManager.instance()
                .error(this, "Error while flushing index via index engine of class %s.", t, indexEngine.getClass().getSimpleName());
          }

        if (writeAheadLog != null) {
          makeFullCheckpoint();
          return;
        }

        writeCache.flush();

        if (configuration != null)
          configuration.synch();

        clearStorageDirty();
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error on synch storage '" + name + "'"), e);

      } finally {
        atomicOperationsManager.releaseAtomicOperations(lockId);
        Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
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
      final OCluster cluster = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));

      if (cluster == null)
        throw new OStorageException("Cluster " + clusterName + " does not exist in database '" + name + "'");
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
      throw OException.wrapException(new OStorageException("Cannot calculate records size"), ioe);
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
    clusterMap.put(newName.toLowerCase(configuration.getLocaleInstance()),
        clusterMap.remove(oldName.toLowerCase(configuration.getLocaleInstance())));
  }

  @Override
  public boolean cleanOutRecord(final ORecordId recordId, final int recordVersion, final int iMode,
      final ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, iMode, callback).getResult();
  }

  public void freeze(final boolean throwException) {
    checkOpeness();
    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final long freezeId;

      if (throwException)
        freezeId = atomicOperationsManager
            .freezeAtomicOperations(OModificationOperationProhibitedException.class, "Modification requests are prohibited");
      else
        freezeId = atomicOperationsManager.freezeAtomicOperations(null, null);

      final List<OFreezableStorageComponent> frozenIndexes = new ArrayList<OFreezableStorageComponent>(indexEngines.size());
      try {
        for (OIndexEngine indexEngine : indexEngines)
          if (indexEngine != null && indexEngine instanceof OFreezableStorageComponent) {
            ((OFreezableStorageComponent) indexEngine).freeze(false);
            frozenIndexes.add((OFreezableStorageComponent) indexEngine);
          }
      } catch (Exception e) {
        // RELEASE ALL THE FROZEN INDEXES
        for (OFreezableStorageComponent indexEngine : frozenIndexes)
          indexEngine.release();

        throw OException.wrapException(new OStorageException("Error on freeze of storage '" + name + "'"), e);
      }

      synch();
      try {
        unlock();

        if (configuration != null)
          configuration.setSoftlyClosed(true);

      } catch (IOException e) {
        atomicOperationsManager.releaseAtomicOperations(freezeId);
        try {
          lock();
        } catch (IOException ignored) {
        }
        throw OException.wrapException(new OStorageException("Error on freeze of storage '" + name + "'"), e);
      }
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void release() {
    try {
      lock();

      for (OIndexEngine indexEngine : indexEngines)
        if (indexEngine != null && indexEngine instanceof OFreezableStorageComponent)
          ((OFreezableStorageComponent) indexEngine).release();

      if (configuration != null)
        configuration.setSoftlyClosed(false);

    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error on release of storage '" + name + "'"), e);
    }

    atomicOperationsManager.releaseAtomicOperations(-1);
  }

  @Override
  public boolean isRemote() {
    return false;
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
    while (true) {
      try {
        final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

        // COPY THE CONTEXT FROM THE REQUEST
        executor.setContext(iCommand.getContext());

        executor.setProgressListener(iCommand.getProgressListener());
        executor.parse(iCommand);

        return executeCommand(iCommand, executor);
      } catch (ORetryQueryException e) {

        if (iCommand instanceof OQueryAbstract) {
          final OQueryAbstract query = (OQueryAbstract) iCommand;
          query.reset();
        }

        continue;
      }
    }
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

      boolean foundInCache = false;
      Object result = null;
      if (iCommand.isCacheableResult() && executor.isCacheable() && iCommand.getParameters() == null) {
        // TRY WITH COMMAND CACHE
        result = db.getMetadata().getCommandCache().get(db.getUser(), iCommand.getText(), iCommand.getLimit());

        if (result != null) {
          foundInCache = true;

          if (iCommand.getResultListener() != null) {
            // INVOKE THE LISTENER IF ANY
            if (result instanceof Collection) {
              for (Object o : (Collection) result)
                iCommand.getResultListener().result(o);
            } else
              iCommand.getResultListener().result(result);

            // RESET THE RESULT TO AVOID TO SEND IT TWICE
            result = null;
          }
        }
      }

      if (!foundInCache) {
        // EXECUTE THE COMMAND
        result = executor.execute(iCommand.getParameters());

        if (result != null && iCommand.isCacheableResult() && executor.isCacheable() && (iCommand.getParameters() == null
            || iCommand.getParameters().isEmpty()))
          // CACHE THE COMMAND RESULT
          db.getMetadata().getCommandCache()
              .put(db.getUser(), iCommand.getText(), result, iCommand.getLimit(), executor.getInvolvedClusters(),
                  System.currentTimeMillis() - beginTime);
      }

      // CALL AFTER COMMAND
      for (ODatabaseListener oDatabaseListener : listeners) {
        oDatabaseListener.onAfterCommand(iCommand, executor, result);
      }

      return result;

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException("Error on execution of command: " + iCommand), e);

    } finally {
      if (Orient.instance().getProfiler().isRecording()) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
        if (db != null) {
          final OSecurityUser user = db.getUser();
          final String userString = user != null ? user.toString() : null;
          Orient.instance().getProfiler()
              .stopChrono("db." + ODatabaseRecordThreadLocal.INSTANCE.get().getName() + ".command." + iCommand.toString(),
                  "Command executed against the database", beginTime, "db.*.command.*", null, userString);
        }
      }
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return new OPhysicalPosition[0];

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final OCluster cluster = getClusterById(currentClusterId);
      return cluster.higherPositions(physicalPosition);
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return new OPhysicalPosition[0];

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final OCluster cluster = getClusterById(clusterId);
      return cluster.ceilingPositions(physicalPosition);
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
    } finally {
      stateLock.releaseReadLock();
    }

  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    if (currentClusterId == -1)
      return new OPhysicalPosition[0];

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final OCluster cluster = getClusterById(currentClusterId);

      return cluster.lowerPositions(physicalPosition);
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    if (clusterId == -1)
      return new OPhysicalPosition[0];

    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final OCluster cluster = getClusterById(clusterId);

      return cluster.floorPositions(physicalPosition);
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void acquireWriteLock(final ORID rid) {
    assert !dataLock.assertSharedLockHold() && !dataLock
        .assertExclusiveLockHold() : " a record lock should not be taken inside a storage lock";
    lockManager.acquireLock(rid, OComparableLockManager.LOCK.EXCLUSIVE, RECORD_LOCK_TIMEOUT);
  }

  public void releaseWriteLock(final ORID rid) {
    assert !dataLock.assertSharedLockHold() && !dataLock
        .assertExclusiveLockHold() : " a record lock should not be released inside a storage lock";
    lockManager.releaseLock(this, rid, OComparableLockManager.LOCK.EXCLUSIVE);
  }

  public void acquireReadLock(final ORID rid) {
    lockManager.acquireLock(rid, OComparableLockManager.LOCK.SHARED, RECORD_LOCK_TIMEOUT);
  }

  public void releaseReadLock(final ORID rid) {
    assert !dataLock.assertSharedLockHold() && !dataLock
        .assertExclusiveLockHold() : " a record lock should not be released inside a storage lock";
    lockManager.releaseLock(this, rid, OComparableLockManager.LOCK.SHARED);
  }

  public ORecordConflictStrategy getConflictStrategy() {
    return recordConflictStrategy;
  }

  public void setConflictStrategy(final ORecordConflictStrategy conflictResolver) {
    this.recordConflictStrategy = conflictResolver;
  }

  protected abstract OLogSequenceNumber copyWALToIncrementalBackup(ZipOutputStream zipOutputStream, long startSegment)
      throws IOException;

  protected abstract boolean isWriteAllowedDuringIncrementalBackup();

  public OStorageRecoverListener getRecoverListener() {
    return recoverListener;
  }

  public void registerRecoverListener(final OStorageRecoverListener recoverListener) {
    this.recoverListener = recoverListener;
  }

  public void unregisterRecoverListener(final OStorageRecoverListener recoverListener) {
    if (this.recoverListener == recoverListener)
      this.recoverListener = null;
  }

  protected abstract File createWalTempDirectory();

  protected abstract void addFileToDirectory(String name, InputStream stream, File directory) throws IOException;

  protected abstract OWriteAheadLog createWalFromIBUFiles(File directory) throws IOException;

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  protected void checkOpeness() {
    if (status != STATUS.OPEN)
      throw new OStorageException("Storage " + name + " is not opened.");
  }

  protected void makeFullCheckpoint() throws IOException {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    if (statistic != null)
      statistic.startFullCheckpointTimer();
    try {
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
        throw OException.wrapException(new OStorageException("Error during checkpoint creation for storage " + name), ioe);
      }

      fullCheckpointCount++;
    } finally {
      if (statistic != null)
        statistic.stopFullCheckpointTimer();
    }
  }

  public long getFullCheckpointCount() {
    return fullCheckpointCount;
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

  private ORawBuffer readRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    checkOpeness();

    if (!rid.isPersistent())
      throw new ORecordNotFoundException(rid,
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    if (transaction.get() != null) {
      final long timer = Orient.instance().getProfiler().startChrono();
      try {
        return doReadRecordIfNotLatest(cluster, rid, recordVersion);
      } finally {
        Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
      }
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      ORawBuffer buff;
      checkOpeness();

      buff = doReadRecordIfNotLatest(cluster, rid, recordVersion);
      return buff;
    } finally {
      stateLock.releaseReadLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
    }
  }

  private ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid) {
    checkOpeness();

    if (!rid.isPersistent())
      throw new ORecordNotFoundException(rid,
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

    final long timer = Orient.instance().getProfiler().startChrono();
    stateLock.acquireReadLock();
    try {
      ORawBuffer buff;
      checkOpeness();

      buff = doReadRecord(clusterSegment, rid);
      return buff;
    } finally {
      stateLock.releaseReadLock();
      Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
    }
  }

  /**
   * Returns the requested records. The returned order could be different than the requested.
   *
   * @param iRids Set of rids to load in one shot
   */
  public Collection<OPair<ORecordId, ORawBuffer>> readRecords(final Collection<ORecordId> iRids) {
    checkOpeness();

    final List<OPair<ORecordId, ORawBuffer>> records = new ArrayList<OPair<ORecordId, ORawBuffer>>();

    if (iRids == null || iRids.isEmpty())
      return records;

    if (transaction.get() != null) {
      for (ORecordId rid : iRids) {
        if (!rid.isPersistent())
          throw new ORecordNotFoundException(rid,
              "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

        records.add(new OPair<ORecordId, ORawBuffer>(rid, doReadRecord(getClusterById(rid.clusterId), rid)));
      }
      return records;
    }

    // CREATE GROUP OF RIDS PER CLUSTER TO REDUCE LOCKS
    final Map<Integer, List<ORecordId>> ridsPerCluster = getRidsGroupedByCluster(iRids);

    stateLock.acquireReadLock();
    try {

      for (Map.Entry<Integer, List<ORecordId>> entry : ridsPerCluster.entrySet()) {
        final int clusterId = entry.getKey();
        final OCluster clusterSegment = getClusterById(clusterId);

        for (ORecordId rid : entry.getValue()) {
          records.add(new OPair<ORecordId, ORawBuffer>(rid, doReadRecord(clusterSegment, rid)));
        }
      }
    } finally {
      stateLock.releaseReadLock();
    }

    return records;
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
      atomicOperationsManager.startAtomicOperation((String) null, true);
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

  private void recoverIfNeeded() throws Exception {
    if (isDirty()) {
      OLogManager.instance().warn(this, "Storage '" + name + "' was not closed properly. Will try to recover from write ahead log");
      try {
        wereDataRestoredAfterOpen = restoreFromWAL() != null;

        if (recoverListener != null)
          recoverListener.onStorageRecover();

      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during storage data restore", e);
        throw e;
      }

      OLogManager.instance().info(this, "Storage data recover was completed");
    }
  }

  private OStorageOperationResult<OPhysicalPosition> doCreateRecord(final ORecordId rid, final byte[] content, int recordVersion,
      final byte recordType, final ORecordCallback<Long> callback, final OCluster cluster, OPhysicalPosition ppos,
      final OPhysicalPosition allocated) {
    if (content == null)
      throw new IllegalArgumentException("Record is null");

    try {
      if (recordVersion > -1)
        recordVersion++;
      else
        recordVersion = 0;

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, true);
      try {
        ppos = cluster.createRecord(content, recordVersion, recordType, allocated);
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

      throw OException.wrapException(new OStorageException("Error during record deletion"), ioe);
    }
  }

  private OStorageOperationResult<Integer> doUpdateRecord(final ORecordId rid, final boolean updateContent, byte[] content,
      final int version, final byte recordType, final ORecordCallback<Integer> callback, final OCluster cluster) {

    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
      if (!checkForRecordValidity(ppos)) {
        final int recordVersion = -1;
        if (callback != null)
          callback.call(rid, recordVersion);

        return new OStorageOperationResult<Integer>(recordVersion);
      }

      boolean contentModified = false;
      if (updateContent) {
        final AtomicInteger recVersion = new AtomicInteger(version);
        final int oldVersion = ppos.recordVersion;
        final AtomicInteger dbVersion = new AtomicInteger(ppos.recordVersion);

        final byte[] newContent = checkAndIncrementVersion(cluster, rid, recVersion, dbVersion, content, recordType);

        ppos.recordVersion = dbVersion.get();

        // REMOVED BECAUSE DISTRIBUTED COULD UNDO AN OPERATION RESTORING A LOWER VERSION
        // assert ppos.recordVersion >= oldVersion;

        if (newContent != null) {
          contentModified = true;
          content = newContent;
        }
      }

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, true);
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

        final int recordVersion = -1;
        if (callback != null)
          callback.call(rid, recordVersion);

        return new OStorageOperationResult<Integer>(recordVersion);
      }

      if (callback != null)
        callback.call(rid, ppos.recordVersion);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Updated record %s v.%s size=%d", rid, ppos.recordVersion, content.length);

      if (contentModified)
        return new OStorageOperationResult<Integer>(ppos.recordVersion, content, false);
      else
        return new OStorageOperationResult<Integer>(ppos.recordVersion);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on updating record " + rid + " (cluster: " + cluster + ")", ioe);

      final int recordVersion = -1;
      if (callback != null)
        callback.call(rid, recordVersion);

      return new OStorageOperationResult<Integer>(recordVersion);
    }
  }

  private OStorageOperationResult<Integer> doRecycleRecord(final ORecordId rid, byte[] content, final int version,
      final OCluster cluster, final byte recordType) {

    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
      if (!checkForRecordValidity(ppos)) {
        final int recordVersion = -1;
        return new OStorageOperationResult<Integer>(recordVersion);
      }

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, true);
      try {
        cluster.recycleRecord(rid.clusterPosition, content, version, recordType);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);

        OLogManager.instance().error(this, "Error on recycling record " + rid + " (cluster: " + cluster + ")", e);

        final int recordVersion = -1;

        return new OStorageOperationResult<Integer>(recordVersion);
      }

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Recycled record %s v.%s size=%d", rid, version, content.length);

      return new OStorageOperationResult<Integer>(version, content, false);

    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on recycling record " + rid + " (cluster: " + cluster + ")", ioe);

      final int recordVersion = -1;

      return new OStorageOperationResult<Integer>(recordVersion);
    }
  }

  private OStorageOperationResult<Boolean> doDeleteRecord(ORecordId rid, final int version, OCluster cluster) {
    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));

      if (ppos == null)
        // ALREADY DELETED
        return new OStorageOperationResult<Boolean>(false);

      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (version > -1 && ppos.recordVersion != version)
        if (OFastConcurrentModificationException.enabled())
          throw OFastConcurrentModificationException.instance();
        else
          throw new OConcurrentModificationException(rid, ppos.recordVersion, version, ORecordOperation.DELETED);

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, true);
      try {
        cluster.deleteRecord(ppos.clusterPosition);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
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
      throw OException.wrapException(new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")"), ioe);
    }
  }

  private OStorageOperationResult<Boolean> doHideMethod(ORecordId rid, OCluster cluster) {
    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));

      if (ppos == null)
        // ALREADY HIDDEN
        return new OStorageOperationResult<Boolean>(false);

      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, true);
      try {
        cluster.hideRecord(ppos.clusterPosition);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);

        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);
        OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", e);

        return new OStorageOperationResult<Boolean>(false);
      }

      return new OStorageOperationResult<Boolean>(true);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw OException.wrapException(new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")"), ioe);
    }
  }

  private ORawBuffer doReadRecord(final OCluster clusterSegment, final ORecordId rid) {
    try {
      final ORawBuffer buff = clusterSegment.readRecord(rid.clusterPosition);

      if (buff != null && OLogManager.instance().isDebugEnabled())
        OLogManager.instance()
            .debug(this, "Read record %s v.%s size=%d bytes", rid, buff.version, buff.buffer != null ? buff.buffer.length : 0);

      return buff;
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private ORawBuffer doReadRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    try {
      return cluster.readRecordIfVersionIsNotLatest(rid.clusterPosition, recordVersion);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private void addDefaultClusters() throws IOException {
    final String storageCompression = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);

    final String storageEncryption = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);

    final String encryptionKey = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

    final String stgConflictStrategy = getConflictStrategy().getName();

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), OMetadataDefault.CLUSTER_INTERNAL_NAME, null,
            true, 20, 4, storageCompression, storageEncryption, encryptionKey, stgConflictStrategy,
            OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), OMetadataDefault.CLUSTER_INDEX_NAME, null, false,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            storageCompression, storageEncryption, encryptionKey, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null,
            false, 1, 1, storageCompression, storageEncryption, encryptionKey, stgConflictStrategy,
            OStorageClusterConfiguration.STATUS.ONLINE));

    defaultClusterId = createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(configuration, clusters.size(), CLUSTER_DEFAULT_NAME, null, true,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            storageCompression, storageEncryption, encryptionKey, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration config) throws IOException {
    OCluster cluster = clusterMap.get(config.getName().toLowerCase(configuration.getLocaleInstance()));

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
   * @param cluster OCluster implementation
   * @return The id (physical position into the array) of the new cluster just created. First is 0.
   * @throws IOException
   */
  private int registerCluster(final OCluster cluster) throws IOException {
    final int id;

    if (cluster != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (clusterMap.containsKey(cluster.getName().toLowerCase(configuration.getLocaleInstance())))
        throw new OConfigurationException(
            "Cannot add cluster '" + cluster.getName() + "' because it is already registered in database '" + name + "'");
      // CREATE AND ADD THE NEW REF SEGMENT
      clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), cluster);
      id = cluster.getId();
    } else {
      id = clusters.size();
    }

    setCluster(id, cluster);

    return id;
  }

  private int doAddCluster(String clusterName, Object[] parameters) throws IOException {
    // FIND THE FIRST AVAILABLE CLUSTER ID
    int clusterPos = clusters.size();
    for (int i = 0; i < clusters.size(); ++i) {
      if (clusters.get(i) == null) {
        clusterPos = i;
        break;
      }
    }

    return addClusterInternal(clusterName, clusterPos, parameters);
  }

  private int addClusterInternal(String clusterName, int clusterPos, Object... parameters) throws IOException {

    final OCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase(configuration.getLocaleInstance());

      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(clusterName, configuration.version, this);
      cluster.configure(this, clusterPos, clusterName, parameters);
    } else {
      cluster = null;
    }

    final int createdClusterId = registerCluster(cluster);

    if (cluster != null) {
      if (!cluster.exists()) {
        cluster.create(-1);
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

      readCache.storeCacheState(writeCache);

      if (!onDelete)
        makeFullCheckpoint();

      preCloseSteps();

      sbTreeCollectionManager.close();

      closeClusters(onDelete);
      closeIndexes(onDelete);

      if (configuration != null)
        if (onDelete)
          configuration.delete();
        else
          configuration.close();

      super.close(force, onDelete);

      if (writeCache != null)
        writeCache.removeLowDiskSpaceListener(this);

      if (writeAheadLog != null)
        writeAheadLog.removeFullCheckpointListener(this);

      if (readCache != null)
        if (!onDelete)
          readCache.closeStorage(writeCache);
        else
          readCache.deleteStorage(writeCache);

      if (writeAheadLog != null) {
        writeAheadLog.close();
        if (onDelete)
          writeAheadLog.delete();
      }

      try {
        performanceStatisticManager.unregisterMBean(name, id);
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for write cache cannot be unregistered", e);
      }

      postCloseSteps(onDelete);

      if (atomicOperationsManager != null)
        try {
          atomicOperationsManager.unregisterMBean();
        } catch (Exception e) {
          OLogManager.instance().error(this, "MBean for atomic opeations manager cannot be unregistered", e);
        }

      status = STATUS.CLOSED;
    } catch (IOException e) {
      final String message = "Error on closing of storage '" + name;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new OStorageException(message), e);

    } finally {
      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
      stateLock.releaseWriteLock();
    }
  }

  protected void closeClusters(boolean onDelete) throws IOException {
    for (OCluster cluster : clusters)
      if (cluster != null)
        cluster.close(!onDelete);

    clusters.clear();
    clusterMap.clear();
  }

  protected void closeIndexes(boolean onDelete) {
    for (OIndexEngine engine : indexEngines) {
      if (engine != null) {
        if (onDelete)
          engine.delete();
        else
          engine.close();
      }
    }

    indexEngines.clear();
    indexEngineNameMap.clear();
  }

  @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
  private byte[] checkAndIncrementVersion(final OCluster iCluster, final ORecordId rid, final AtomicInteger version,
      final AtomicInteger iDatabaseVersion, final byte[] iRecordContent, final byte iRecordType) {

    final int v = version.get();

    switch (v) {
    // DOCUMENT UPDATE, NO VERSION CONTROL
    case -1:
      iDatabaseVersion.incrementAndGet();
      break;

    // DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION UPDATE
    case -2:
      break;

    default:
      // MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (v < -2) {
        // OVERWRITE VERSION: THIS IS USED IN CASE OF FIX OF RECORDS IN DISTRIBUTED MODE
        version.set(ORecordVersionHelper.clearRollbackMode(v));
        iDatabaseVersion.set(version.get());

      } else if (v != iDatabaseVersion.get()) {
        final ORecordConflictStrategy strategy =
            iCluster.getRecordConflictStrategy() != null ? iCluster.getRecordConflictStrategy() : recordConflictStrategy;
        return strategy.onUpdate(this, iRecordType, rid, v, iRecordContent, iDatabaseVersion);
      } else
        // OK, INCREMENT DB VERSION
        iDatabaseVersion.incrementAndGet();
    }

    return null;
  }

  private void commitEntry(final ORecordOperation txEntry, final OPhysicalPosition allocated) throws IOException {

    final ORecord rec = txEntry.getRecord();
    if (txEntry.type != ORecordOperation.DELETED && !rec.isDirty())
      // NO OPERATION
      return;

    ORecordId rid = (ORecordId) rec.getIdentity();

    if (txEntry.type == ORecordOperation.UPDATED && rid.isNew())
      // OVERWRITE OPERATION AS CREATE
      txEntry.type = ORecordOperation.CREATED;

    ORecordSerializationContext.pushContext();
    try {
      final OCluster cluster = getClusterById(rid.clusterId);

      if (cluster.getName().equals(OMetadataDefault.CLUSTER_INDEX_NAME) || cluster.getName()
          .equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME))
        // AVOID TO COMMIT INDEX STUFF
        return;

      if (rec instanceof OTxListener)
        ((OTxListener) rec).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

      switch (txEntry.type) {
      case ORecordOperation.LOADED:
        break;

      case ORecordOperation.CREATED: {
        final byte[] stream = rec.toStream();
        if (allocated != null) {
          final OPhysicalPosition ppos;
          final byte recordType = ORecordInternal.getRecordType(rec);
          ppos = doCreateRecord(rid, stream, rec.getVersion(), recordType, null, cluster, new OPhysicalPosition(recordType),
              allocated).getResult();

          ORecordInternal.setVersion(rec, ppos.recordVersion);
        } else {
          // USE -2 AS VERSION TO AVOID INCREMENTING THE VERSION
          ORecordInternal.setVersion(rec,
              updateRecord(rid, ORecordInternal.isContentChanged(rec), stream, -2, ORecordInternal.getRecordType(rec), -1, null)
                  .getResult());
        }
        txEntry.getRecord().fromStream(stream);
        break;
      }

      case ORecordOperation.UPDATED: {
        final byte[] stream = rec.toStream();

        OStorageOperationResult<Integer> updateRes = doUpdateRecord(rid, ORecordInternal.isContentChanged(rec), stream,
            rec.getVersion(), ORecordInternal.getRecordType(rec), null, cluster);
        ORecordInternal.setVersion(rec, updateRes.getResult());
        if (updateRes.getModifiedRecordContent() != null) {
          ORecordInternal.fill(rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);
        } else
          txEntry.getRecord().fromStream(stream);

        break;
      }

      case ORecordOperation.DELETED: {
        deleteRecord(rid, rec.getVersion(), -1, null);
        break;
      }

      default:
        throw new OStorageException("Unknown record operation " + txEntry.type);
      }
    } finally {
      ORecordSerializationContext.pullContext();
    }

    // RESET TRACKING
    if (rec instanceof ODocument && ((ODocument) rec).isTrackingChanges()) {
      ODocumentInternal.clearTrackData(((ODocument) rec));
    }

    ORecordInternal.unsetDirty(rec);

    if (rec instanceof OTxListener)
      ((OTxListener) rec).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId < 0 || iClusterId > clusters.size() - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  private OLogSequenceNumber restoreFromWAL() throws IOException {
    if (writeAheadLog == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead logging is switched off.");
      return null;
    }

    if (writeAheadLog.begin() == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead log is empty.");
      return null;
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

  @Override
  public String incrementalBackup(String backupDirectory) {
    throw new IllegalStateException("Incremental backup is supported only in enterprise version");
  }

  @Override
  public void restoreFromIncrementalBackup(String filePath) {
    throw new IllegalStateException("Incremental backup is supported only in enterprise version");
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

  private OLogSequenceNumber restoreFromCheckPoint(OAbstractCheckPointStartRecord checkPointRecord) throws IOException {
    if (checkPointRecord instanceof OFuzzyCheckpointStartRecord) {
      return restoreFromFuzzyCheckPoint((OFuzzyCheckpointStartRecord) checkPointRecord);
    }

    if (checkPointRecord instanceof OFullCheckpointStartRecord) {
      return restoreFromFullCheckPoint((OFullCheckpointStartRecord) checkPointRecord);
    }

    throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());
  }

  private OLogSequenceNumber restoreFromFullCheckPoint(OFullCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from full checkpoint is started. Restore is performed from LSN %s",
        checkPointRecord.getLsn());

    final OLogSequenceNumber lsn = writeAheadLog.next(checkPointRecord.getLsn());
    return restoreFrom(lsn, writeAheadLog);
  }

  private OLogSequenceNumber restoreFromFuzzyCheckPoint(OFuzzyCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from FUZZY checkpoint is started.");
    OLogSequenceNumber flushedLsn = checkPointRecord.getFlushedLsn();

    if (flushedLsn.compareTo(writeAheadLog.begin()) < 0)
      flushedLsn = writeAheadLog.begin();

    return restoreFrom(flushedLsn, writeAheadLog);
  }

  private OLogSequenceNumber restoreFromBegging() throws IOException {
    OLogManager.instance().info(this, "Data restore procedure is started.");
    OLogSequenceNumber lsn = writeAheadLog.begin();

    return restoreFrom(lsn, writeAheadLog);
  }

  protected OLogSequenceNumber restoreFrom(OLogSequenceNumber lsn, OWriteAheadLog writeAheadLog) throws IOException {
    OLogSequenceNumber logSequenceNumber = null;
    OModifiableBoolean atLeastOnePageUpdate = new OModifiableBoolean();

    long recordsProcessed = 0;

    final int reportInterval = OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();
    final Map<OOperationUnitId, List<OWALRecord>> operationUnits = new HashMap<OOperationUnitId, List<OWALRecord>>();

    try {
      while (lsn != null) {
        logSequenceNumber = lsn;

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
          OLogManager.instance().warn(this, "Record %s will be skipped during data restore", walRecord);

        recordsProcessed++;

        if (reportInterval > 0 && recordsProcessed % reportInterval == 0) {
          OLogManager.instance().info(this, "%d operations were processed, current LSN is %s last LSN is %s", recordsProcessed, lsn,
              writeAheadLog.end());

        }

        lsn = writeAheadLog.next(lsn);
      }
    } catch (OWALPageBrokenException e) {
      OLogManager.instance()
          .error(this, "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.");
    } catch (RuntimeException e) {
      OLogManager.instance().error(this,
          "Data restore was paused because of exception. The rest of changes will be rolled back and WAL files will be backed up."
              + " Please report issue about this exception to bug tracker and provide WAL files which are backed up in 'wal_backup' directory.");
      backUpWAL(e);
    }

    if (atLeastOnePageUpdate.getValue())
      return logSequenceNumber;

    return null;
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

      final PrintWriter metadataFileWriter = new PrintWriter(new OutputStreamWriter(archiveZipOutputStream, "UTF-8"));
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
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error during WAL backup", ioe);
    }

  }

  private void archiveEntry(ZipOutputStream archiveZipOutputStream, String walSegment) throws IOException {
    final File walFile = new File(walSegment);
    final ZipEntry walZipEntry = new ZipEntry(walFile.getName());
    archiveZipOutputStream.putNextEntry(walZipEntry);
    try {
      final FileInputStream walInputStream = new FileInputStream(walFile);
      try {
        final BufferedInputStream walBufferedInputStream = new BufferedInputStream(walInputStream);
        try {
          final byte[] buffer = new byte[1024];
          int readBytes = 0;

          while ((readBytes = walBufferedInputStream.read(buffer)) > -1) {
            archiveZipOutputStream.write(buffer, 0, readBytes);
          }
        } finally {
          walBufferedInputStream.close();
        }
      } finally {
        walInputStream.close();
      }
    } finally {
      archiveZipOutputStream.closeEntry();
    }
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

        long fileId = updatePageRecord.getFileId();

        final long pageIndex = updatePageRecord.getPageIndex();
        fileId = readCache.openFile(fileId, writeCache);

        OCacheEntry cacheEntry = readCache.load(fileId, pageIndex, true, writeCache, 1);
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
        OLogManager.instance()
            .error(this, "Invalid WAL record type was passed %s. Given record will be skipped.", walRecord.getClass());

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

  private static class ORIDOLockManager extends OComparableLockManager<ORID> {
    public ORIDOLockManager(final int amountOfCachedInstances) {
      super(true, -1);
    }

    @Override
    protected ORID getImmutableResourceId(ORID iResourceId) {
      return new ORecordId(iResourceId);
    }
  }

  protected Map<Integer, List<ORecordId>> getRidsGroupedByCluster(final Collection<ORecordId> iRids) {
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

  private class UncompletedCommit implements OUncompletedCommit<List<ORecordOperation>> {
    private final OTransaction                         clientTx;
    private final Iterable<ORecordOperation>           entries;
    private final List<ORecordOperation>               result;
    private final OUncompletedCommit<OAtomicOperation> nestedCommit;

    public UncompletedCommit(OTransaction clientTx, Iterable<ORecordOperation> entries, List<ORecordOperation> result,
        OUncompletedCommit<OAtomicOperation> nestedCommit) {
      this.clientTx = clientTx;
      this.entries = entries;
      this.result = result;
      this.nestedCommit = nestedCommit;
    }

    @Override
    public List<ORecordOperation> complete() {
      checkOpeness();

      stateLock.acquireReadLock();
      try {
        try {
          try {
            checkOpeness();

            nestedCommit.complete();

            OTransactionAbstract.updateCacheFromEntries(clientTx, entries, true);
          } catch (RuntimeException e) {
            makeRollback(clientTx, e);
          } finally {
            transaction.set(null);
          }
        } finally {
          ((OMetadataInternal) clientTx.getDatabase().getMetadata()).clearThreadLocalSchemaSnapshot();
        }
      } finally {
        stateLock.releaseReadLock();
      }

      return result;
    }

    @Override
    public void rollback() {
      checkOpeness();
      stateLock.acquireReadLock();
      try {
        try {
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

            nestedCommit.rollback();
            assert atomicOperationsManager.getCurrentOperation() == null;

            OTransactionAbstract.updateCacheFromEntries(clientTx, clientTx.getAllRecordEntries(), false);
          } catch (IOException e) {
            throw OException.wrapException(new OStorageException("Error during transaction rollback"), e);
          } finally {
            transaction.set(null);
          }
        } finally {
          ((OMetadataInternal) clientTx.getDatabase().getMetadata()).clearThreadLocalSchemaSnapshot();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    }

    private void makeRollback(OTransaction clientTx, Exception e) {
      OLogManager.instance()
          .debug(this, "Error during transaction commit completion, transaction will be rolled back (tx-id=%d)", e,
              clientTx.getId());
      rollback();
      if (e instanceof OException)
        throw ((OException) e);
      else
        throw OException.wrapException(new OStorageException("Error during transaction commit completion"), e);
    }
  }

}
