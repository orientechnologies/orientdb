/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.lock.*;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.AtomicLongOProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.*;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDeleter;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
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
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerAbstract;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 28.03.13
 */
public abstract class OAbstractPaginatedStorage extends OStorageAbstract
    implements OLowDiskSpaceListener, OCheckpointRequestListener, OIdentifiableStorage, OBackgroundExceptionListener,
    OFreezableStorageComponent, OPageIsBrokenListener {
  private static final int RECORD_LOCK_TIMEOUT         = OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger();
  private static final int WAL_RESTORE_REPORT_INTERVAL = 30 * 1000; // milliseconds

  private static final Comparator<ORecordOperation> COMMIT_RECORD_OPERATION_COMPARATOR = Comparator
      .comparing(o -> o.getRecord().getIdentity());

  @SuppressWarnings("CanBeFinal")
  private static volatile DataOutputStream journaledStream = null;

  static {
    // initialize journaled tx streaming if enabled by configuration

    final Integer journaledPort = OGlobalConfiguration.STORAGE_INTERNAL_JOURNALED_TX_STREAMING_PORT.getValue();
    if (journaledPort != null) {
      ServerSocket serverSocket;
      try {
        //noinspection resource,IOResourceOpenedButNotSafelyClosed
        serverSocket = new ServerSocket(journaledPort, 0, InetAddress.getLocalHost());
        serverSocket.setReuseAddress(true);
      } catch (IOException e) {
        serverSocket = null;
        OLogManager.instance().error(OAbstractPaginatedStorage.class, "unable to create journaled tx server socket", e);
      }

      if (serverSocket != null) {
        final ServerSocket finalServerSocket = serverSocket;
        final Thread serverThread = new Thread(() -> {
          OLogManager.instance()
              .info(OAbstractPaginatedStorage.class, "journaled tx streaming server is listening on localhost:" + journaledPort);
          try {
            @SuppressWarnings("resource")
            final Socket clientSocket = finalServerSocket.accept(); // accept single connection only and only once
            clientSocket.setSendBufferSize(4 * 1024 * 1024 /* 4MB */);
            journaledStream = new DataOutputStream(clientSocket.getOutputStream());
          } catch (IOException e) {
            journaledStream = null;
            OLogManager.instance().error(OAbstractPaginatedStorage.class, "unable to accept journaled tx client connection", e);
          }
        });
        serverThread.setDaemon(true);
        serverThread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
        serverThread.start();
      }
    }
  }

  private final OComparableLockManager<ORID> lockManager;

  /**
   * Lock is used to atomically update record versions.
   */
  private final OLockManager<ORID> recordVersionManager;

  private final Map<String, OCluster> clusterMap = new HashMap<>();
  private final List<OCluster>        clusters   = new ArrayList<>();

  private volatile ThreadLocal<OStorageTransaction> transaction;
  private final AtomicBoolean checkpointInProgress = new AtomicBoolean();
  private final AtomicBoolean walVacuumInProgress  = new AtomicBoolean();

  /**
   * Error which happened inside of storage or during data processing related to this storage.
   */
  private final AtomicReference<Error> jvmError = new AtomicReference<>();

  @SuppressWarnings("WeakerAccess")
  protected final OSBTreeCollectionManagerShared sbTreeCollectionManager;

  private final OPerformanceStatisticManager performanceStatisticManager = new OPerformanceStatisticManager(this,
      OGlobalConfiguration.STORAGE_PROFILER_SNAPSHOT_INTERVAL.getValueAsInteger() * 1000000L,
      OGlobalConfiguration.STORAGE_PROFILER_CLEANUP_INTERVAL.getValueAsInteger() * 1000000L);

  protected volatile OWriteAheadLog          writeAheadLog;
  private            OStorageRecoverListener recoverListener;

  protected volatile OReadCache  readCache;
  protected volatile OWriteCache writeCache;

  private volatile ORecordConflictStrategy recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
      .getDefaultImplementation();

  private volatile int defaultClusterId = -1;
  @SuppressWarnings("WeakerAccess")
  protected volatile OAtomicOperationsManager atomicOperationsManager;
  private volatile boolean                  wereNonTxOperationsPerformedInPreviousOpen = false;
  private volatile OLowDiskSpaceInformation lowDiskSpace                               = null;

  /**
   * Set of pages which were detected as broken and need to be repaired.
   */
  private final Set<OPair<String, Long>> brokenPages = Collections.newSetFromMap(new ConcurrentHashMap<>());

  protected volatile OScheduledThreadPoolExecutorWithLogging fuzzyCheckpointExecutor;

  private volatile Throwable dataFlushException = null;

  private final int id;

  private final Map<String, OIndexEngine> indexEngineNameMap        = new HashMap<>();
  private final List<OIndexEngine>        indexEngines              = new ArrayList<>();
  private       boolean                   wereDataRestoredAfterOpen = false;

  private final LongAdder fullCheckpointCount = new LongAdder();

  private final AtomicLong recordCreated = new AtomicLong(0);
  private final AtomicLong recordUpdated = new AtomicLong(0);
  private final AtomicLong recordRead    = new AtomicLong(0);
  private final AtomicLong recordDeleted = new AtomicLong(0);

  private final AtomicLong recordScanned  = new AtomicLong(0);
  private final AtomicLong recordRecycled = new AtomicLong(0);
  private final AtomicLong recordConflict = new AtomicLong(0);
  private final AtomicLong txBegun        = new AtomicLong(0);
  private final AtomicLong txCommit       = new AtomicLong(0);
  private final AtomicLong txRollback     = new AtomicLong(0);

  public OAbstractPaginatedStorage(String name, String filePath, String mode, int id) {
    super(name, filePath, mode);

    this.id = id;
    lockManager = new ORIDOLockManager();
    recordVersionManager = new OPartitionedLockManager<>();

    registerProfilerHooks();
    sbTreeCollectionManager = new OSBTreeCollectionManagerShared(this);
  }

  @Override
  public void open(final String iUserName, final String iUserPassword, final OContextConfiguration contextConfiguration) {
    open(contextConfiguration);
  }

  public void open(final OContextConfiguration contextConfiguration) {
    try {
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

        fuzzyCheckpointExecutor = new OScheduledThreadPoolExecutorWithLogging(1, new FuzzyCheckpointThreadFactory());
        fuzzyCheckpointExecutor.setMaximumPoolSize(1);

        transaction = new ThreadLocal<>();
        getConfiguration().load(contextConfiguration);

        final String cs = getConfiguration().getConflictStrategy();
        if (cs != null) {
          // SET THE CONFLICT STORAGE STRATEGY FROM THE LOADED CONFIGURATION
          setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(cs));
        }

        componentsFactory = new OCurrentStorageComponentsFactory(getConfiguration());

        preOpenSteps();

        try {
          performanceStatisticManager.registerMBean(name, id);
        } catch (Exception e) {
          OLogManager.instance().error(this, "MBean for profiler cannot be registered.", e);
        }

        initWalAndDiskCache(contextConfiguration);

        atomicOperationsManager = new OAtomicOperationsManager(this);
        try {
          atomicOperationsManager.registerMBean();
        } catch (Exception e) {
          OLogManager.instance().error(this, "MBean for atomic operations manager cannot be registered", e);
        }

        recoverIfNeeded();

        openClusters();
        openIndexes();

        status = STATUS.OPEN;

        readCache.loadCacheState(writeCache);

      } catch (OStorageException e) {
        throw e;
      } catch (Exception e) {
        for (OCluster c : clusters) {
          try {
            if (c != null)
              c.close(false);
          } catch (IOException e1) {
            OLogManager.instance().error(this, "Cannot close cluster after exception on open", e1);
          }
        }

        try {
          status = STATUS.OPEN;
          close(true, false);
        } catch (RuntimeException re) {
          OLogManager.instance().error(this, "Error during storage close", re);
        }

        status = STATUS.CLOSED;

        throw OException.wrapException(new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    OLogManager.instance()
        .infoNoDb(this, "Storage '%s' is opened under OrientDB distribution : %s", getURL(), OConstants.getVersion());
  }

  /**
   * That is internal method which is called once we encounter any error inside of JVM. In such case we need to restart JVM to avoid
   * any data corruption. Till JVM is not restarted storage will be put in read-only state.
   */
  public void handleJVMError(Error e) {
    jvmError.compareAndSet(null, e);
  }

  /**
   * This method is called by distributed storage during initialization to indicate that database is used in distributed cluster
   * configuration
   */
  public void underDistributedStorage() {
    sbTreeCollectionManager.prohibitAccess();
  }

  /**
   * @inheritDoc
   */
  @Override
  public String getCreatedAtVersion() {
    return getConfiguration().getCreatedAtVersion();
  }

  @SuppressWarnings("WeakerAccess")
  protected void openIndexes() {
    OCurrentStorageComponentsFactory cf = componentsFactory;
    if (cf == null)
      throw new OStorageException("Storage '" + name + "' is not properly initialized");

    final Set<String> indexNames = getConfiguration().indexEngines();
    for (String indexName : indexNames) {
      final OStorageConfigurationImpl.IndexEngineData engineData = getConfiguration().getIndexEngine(indexName);
      final OIndexEngine engine = OIndexes
          .createIndexEngine(engineData.getName(), engineData.getAlgorithm(), engineData.getIndexType(),
              engineData.getDurableInNonTxMode(), this, engineData.getVersion(), engineData.getEngineProperties(), null);

      try {
        engine.load(engineData.getName(), cf.binarySerializerFactory.getObjectSerializer(engineData.getValueSerializerId()),
            engineData.isAutomatic(), cf.binarySerializerFactory.getObjectSerializer(engineData.getKeySerializedId()),
            engineData.getKeyTypes(), engineData.isNullValuesSupport(), engineData.getKeySize(), engineData.getEngineProperties());

        indexEngineNameMap.put(engineData.getName(), engine);
        indexEngines.add(engine);
      } catch (RuntimeException e) {
        OLogManager.instance()
            .error(this, "Index '" + engineData.getName() + "' cannot be created and will be removed from configuration", e);

        engine.deleteWithoutLoad(engineData.getName());
      }
    }
  }

  @SuppressWarnings("WeakerAccess")
  protected void openClusters() throws IOException {
    // OPEN BASIC SEGMENTS
    int pos;
    addDefaultClusters();

    // REGISTER CLUSTER
    for (int i = 0; i < getConfiguration().clusters.size(); ++i) {
      final OStorageClusterConfiguration clusterConfig = getConfiguration().clusters.get(i);

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
              + "): file not found. It will be excluded from current database '" + getName() + "'.", e);

          clusterMap.remove(clusters.get(i).getName().toLowerCase(configuration.getLocaleInstance()));

          setCluster(i, null);
        }
      } else {
        setCluster(i, null);
      }
    }
  }

  @SuppressWarnings("unused")
  public void open(final OToken iToken, final OContextConfiguration configuration) {
    open(iToken.getUserName(), "", configuration);
  }

  @Override
  public void create(OContextConfiguration contextConfiguration) throws IOException {
    try {
      stateLock.acquireWriteLock();
      try {

        if (status != STATUS.CLOSED)
          throw new OStorageExistsException("Cannot create new storage '" + getURL() + "' because it is not closed");

        if (exists())
          throw new OStorageExistsException("Cannot create new storage '" + getURL() + "' because it already exists");

        fuzzyCheckpointExecutor = new OScheduledThreadPoolExecutorWithLogging(1, new FuzzyCheckpointThreadFactory());
        fuzzyCheckpointExecutor.setMaximumPoolSize(1);

        getConfiguration().initConfiguration(contextConfiguration);
        componentsFactory = new OCurrentStorageComponentsFactory(getConfiguration());
        try {
          performanceStatisticManager.registerMBean(name, id);
        } catch (Exception e) {
          OLogManager.instance().error(this, "MBean for profiler cannot be registered.", e);
        }
        transaction = new ThreadLocal<>();
        initWalAndDiskCache(contextConfiguration);

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
        getConfiguration().create();
        getConfiguration().setCreationVersion(OConstants.getVersion());

        // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
        // INDEXING
        doAddCluster(OMetadataDefault.CLUSTER_INDEX_NAME, null);

        // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
        // INDEXING
        doAddCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null);

        // ADD THE DEFAULT CLUSTER
        defaultClusterId = doAddCluster(CLUSTER_DEFAULT_NAME, null);

        if (jvmError.get() == null) {
          clearStorageDirty();
        }

        if (contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE))
          makeFullCheckpoint();

        postCreateSteps();

      } catch (InterruptedException e) {
        throw OException.wrapException(new OStorageException("Storage creation was interrupted"), e);
      } catch (OStorageException e) {
        close();
        throw e;
      } catch (IOException e) {
        close();
        throw OException.wrapException(new OStorageException("Error on creation of storage '" + name + "'"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    OLogManager.instance()
        .infoNoDb(this, "Storage '%s' is created under OrientDB distribution : %s", getURL(), OConstants.getVersion());

  }

  @Override
  public boolean isClosed() {
    try {
      stateLock.acquireReadLock();
      try {
        return super.isClosed();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void close(final boolean force, boolean onDelete) {
    try {
      doClose(force, onDelete);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void delete() {
    try {
      final long timer = Orient.instance().getProfiler().startChrono();

      stateLock.acquireWriteLock();
      try {
        try {
          // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
          close(true, true);

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
        //noinspection ResultOfMethodCallIgnored
        Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public boolean check(final boolean verbose, final OCommandOutputListener listener) {
    try {
      listener.onMessage("Check of storage is started...");

      checkOpenness();
      stateLock.acquireReadLock();
      try {
        final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
        try {
          checkOpenness();
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
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int addCluster(String clusterName, final Object... parameters) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        makeStorageDirty();
        return doAddCluster(clusterName, parameters);

      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error in creation of new cluster '" + clusterName), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int addCluster(String clusterName, int requestedId, Object... parameters) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();
      stateLock.acquireWriteLock();
      try {
        checkOpenness();

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
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public boolean dropCluster(final int clusterId, final boolean iTruncate) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {

        checkOpenness();
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
        getConfiguration().dropCluster(clusterId);

        return true;
      } catch (Exception e) {
        throw OException.wrapException(new OStorageException("Error while removing cluster '" + clusterId + "'"), e);

      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int getId() {
    return id;
  }

  public boolean setClusterStatus(final int clusterId, final OStorageClusterConfiguration.STATUS iStatus) {
    try {
      checkOpenness();
      stateLock.acquireWriteLock();
      try {
        checkOpenness();
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

          newCluster = OPaginatedClusterFactory.INSTANCE.createCluster(cluster.getName(), getConfiguration().version, this);
          newCluster.configure(this, clusterId, cluster.getName());
          newCluster.open();
        }

        clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), newCluster);
        clusters.set(clusterId, newCluster);

        // UPDATE CONFIGURATION
        makeStorageDirty();
        getConfiguration().setClusterStatus(clusterId, iStatus);

        makeFullCheckpoint();
        return true;
      } catch (Exception e) {
        throw OException.wrapException(new OStorageException("Error while removing cluster '" + clusterId + "'"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
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

  @Override
  public long count(final int iClusterId) {
    return count(iClusterId, false);
  }

  @Override
  public long count(int clusterId, boolean countTombstones) {
    try {
      if (clusterId == -1)
        throw new OStorageException("Cluster Id " + clusterId + " is invalid in database '" + name + "'");

      // COUNT PHYSICAL CLUSTER IF ANY
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null)
          return 0;

        if (countTombstones)
          return cluster.getEntries();

        return cluster.getEntries() - cluster.getTombstonesCount();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long[] getClusterDataRange(final int iClusterId) {
    try {
      if (iClusterId == -1)
        return new long[] { ORID.CLUSTER_POS_INVALID, ORID.CLUSTER_POS_INVALID };

      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return clusters.get(iClusterId) != null ?
            new long[] { clusters.get(iClusterId).getFirstPosition(), clusters.get(iClusterId).getLastPosition() } :
            OCommonConst.EMPTY_LONG_ARRAY;

      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Cannot retrieve information about data range"), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public OLogSequenceNumber getLSN() {
    try {
      if (writeAheadLog == null)
        return null;

      return writeAheadLog.end();
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public void onException(Throwable e) {
    dataFlushException = e;
  }

  /**
   * This method finds all the records which were updated starting from (but not including) current LSN and write result in provided
   * output stream. In output stream will be included all thw records which were updated/deleted/created since passed in LSN till
   * the current moment.
   * Deleted records are written in output stream first, then created/updated records. All records are sorted by record id.
   * Data format: <ol> <li>Amount of records (single entry) - 8 bytes</li> <li>Record's cluster id - 4 bytes</li> <li>Record's
   * cluster position - 8 bytes</li> <li>Delete flag, 1 if record is deleted - 1 byte</li> <li>Record version , only if record is
   * not deleted - 4 bytes</li> <li>Record type, only if record is not deleted - 1 byte</li> <li>Length of binary presentation of
   * record, only if record is not deleted - 4 bytes</li> <li>Binary presentation of the record, only if record is not deleted -
   * length of content is provided in above entity</li> </ol>
   *
   * @param lsn    LSN from which we should find changed records
   * @param stream Stream which will contain found records
   *
   * @return Last LSN processed during examination of changed records, or <code>null</code> if it was impossible to find changed
   * records: write ahead log is absent, record with start LSN was not found in WAL, etc.
   *
   * @see OGlobalConfiguration#STORAGE_TRACK_CHANGED_RECORDS_IN_WAL
   */
  public OLogSequenceNumber recordsChangedAfterLSN(final OLogSequenceNumber lsn, final OutputStream stream,
      final OCommandOutputListener outputListener) {
    try {
      if (!getConfiguration().getContextConfiguration()
          .getValueAsBoolean(OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL))
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

        if (endLsn == null || lsn.compareTo(endLsn) > 0) {
          OLogManager.instance()
              .warn(this, "Cannot find requested LSN=%s for database sync operation. Last available LSN is %s", lsn, endLsn);
          return null;
        }

        if (lsn.equals(endLsn)) {
          // nothing has changed
          return endLsn;
        }

        // container of rids of changed records
        final SortedSet<ORID> sortedRids = new TreeSet<>();

        OLogSequenceNumber startLsn = writeAheadLog.next(lsn);
        if (startLsn == null) {
          OLogManager.instance()
              .info(this, "Cannot find requested LSN=%s for database sync operation (last available LSN is %s)", lsn, endLsn);
          return null;
        }

        final OLogSequenceNumber freezeLsn = startLsn;

        writeAheadLog.addCutTillLimit(freezeLsn);
        try {
          startLsn = writeAheadLog.next(lsn);
          if (startLsn == null) {
            OLogManager.instance()
                .info(this, "Cannot find requested LSN=%s for database sync operation (last available LSN is %s)", lsn, endLsn);
            return null;
          }

          // start record is absent there is nothing that we can do
          OWALRecord walRecord = writeAheadLog.read(startLsn);
          if (walRecord == null) {
            OLogManager.instance()
                .info(this, "Cannot find requested LSN=%s for database sync operation (record in WAL is absent)", lsn);
            return null;
          }

          OLogSequenceNumber currentLsn = startLsn;

          // all information about changed records is contained in atomic operation metadata
          long read = 0;
          while (currentLsn != null && endLsn.compareTo(currentLsn) >= 0) {
            walRecord = writeAheadLog.read(currentLsn);

            if (walRecord instanceof OFileCreatedWALRecord)
              throw new ODatabaseException(
                  "Cannot execute delta-sync because a new file has been added. Filename: '" + ((OFileCreatedWALRecord) walRecord)
                      .getFileName() + "' (id=" + ((OFileCreatedWALRecord) walRecord).getFileId() + ")");

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
                sortedRids.addAll(rids);
              }
            }

            currentLsn = writeAheadLog.next(currentLsn);

            read++;

            if (outputListener != null)
              outputListener.onMessage("read " + read + " records from WAL and collected " + sortedRids.size() + " records");
          }
        } finally {
          writeAheadLog.removeCutTillLimit(freezeLsn);
        }

        final int totalRecords = sortedRids.size();
        OLogManager.instance().info(this, "Exporting records after LSN=%s. Found %d records", lsn, totalRecords);

        // records may be deleted after we flag them as existing and as result rule of sorting of records
        // (deleted records go first will be broken), so we prohibit any modifications till we do not complete method execution
        final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
        try {
          try (DataOutputStream dataOutputStream = new DataOutputStream(stream)) {

            dataOutputStream.writeLong(sortedRids.size());

            long exportedRecord = 1;
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

                if (outputListener != null)
                  outputListener.onMessage("exporting record " + exportedRecord + "/" + totalRecords);

                // delete to avoid duplication
                ridIterator.remove();
                exportedRecord++;
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
                final ORawBuffer rawBuffer = cluster.readRecord(rid.getClusterPosition(), false);
                assert rawBuffer != null;

                dataOutputStream.writeBoolean(false);
                dataOutputStream.writeInt(rawBuffer.version);
                dataOutputStream.write(rawBuffer.recordType);
                dataOutputStream.writeInt(rawBuffer.buffer.length);
                dataOutputStream.write(rawBuffer.buffer);

                OLogManager.instance().debug(this, "Exporting modified record rid=%s type=%d size=%d v=%d - buffer size=%d", rid,
                    rawBuffer.recordType, rawBuffer.buffer.length, rawBuffer.version, dataOutputStream.size());
              }

              if (outputListener != null)
                outputListener.onMessage("exporting record " + exportedRecord + "/" + totalRecords);

              exportedRecord++;
            }
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
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * This method finds all the records changed in the last X transactions.
   *
   * @param maxEntries Maximum number of entries to check back from last log.
   *
   * @return A set of record ids of the changed records
   *
   * @see OGlobalConfiguration#STORAGE_TRACK_CHANGED_RECORDS_IN_WAL
   */
  public Set<ORecordId> recordsChangedRecently(final int maxEntries) {
    final SortedSet<ORecordId> result = new TreeSet<>();

    try {
      if (!OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getValueAsBoolean())
        throw new IllegalStateException(
            "Cannot find records which were changed starting from provided LSN because tracking of rids of changed records in WAL is switched off, "
                + "to switch it on please set property " + OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getKey()
                + " to the true value, please note that only records"
                + " which are stored after this property was set will be retrieved");

      stateLock.acquireReadLock();
      try {
        if (writeAheadLog == null) {
          OLogManager.instance().warn(this, "No WAL found for database '%s'", name);
          return null;
        }

        OLogSequenceNumber startLsn = writeAheadLog.begin();
        if (startLsn == null) {
          OLogManager.instance().warn(this, "The WAL is empty for database '%s'", name);
          return result;
        }

        final OLogSequenceNumber freezeLSN = startLsn;
        writeAheadLog.addCutTillLimit(freezeLSN);
        try {
          //reread because log may be already truncated
          startLsn = writeAheadLog.begin();
          if (startLsn == null) {
            OLogManager.instance().warn(this, "The WAL is empty for database '%s'", name);
            return result;
          }

          final OLogSequenceNumber endLsn = writeAheadLog.end();
          if (endLsn == null) {
            OLogManager.instance().warn(this, "The WAL is empty for database '%s'", name);
            return result;
          }

          OWALRecord walRecord = writeAheadLog.read(startLsn);
          if (walRecord == null) {
            OLogManager.instance()
                .info(this, "Cannot find requested LSN=%s for database sync operation (record in WAL is absent)", startLsn);
            return null;
          }

          OLogSequenceNumber currentLsn = startLsn;

          // KEEP LAST MAX-ENTRIES TRANSACTIONS' LSN
          final List<OLogSequenceNumber> lastTx = new LinkedList<>();
          while (currentLsn != null && endLsn.compareTo(currentLsn) >= 0) {
            walRecord = writeAheadLog.read(currentLsn);

            if (walRecord instanceof OAtomicUnitEndRecord) {
              if (lastTx.size() >= maxEntries)
                lastTx.remove(0);
              lastTx.add(currentLsn);
            }

            currentLsn = writeAheadLog.next(currentLsn);
          }

          // COLLECT ALL THE MODIFIED RECORDS
          for (OLogSequenceNumber lsn : lastTx) {
            walRecord = writeAheadLog.read(lsn);

            final OAtomicUnitEndRecord atomicUnitEndRecord = (OAtomicUnitEndRecord) walRecord;

            if (atomicUnitEndRecord.getAtomicOperationMetadata().containsKey(ORecordOperationMetadata.RID_METADATA_KEY)) {
              final ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) atomicUnitEndRecord
                  .getAtomicOperationMetadata().get(ORecordOperationMetadata.RID_METADATA_KEY);
              final Set<ORID> rids = recordOperationMetadata.getValue();
              for (ORID rid : rids) {
                result.add((ORecordId) rid);
              }
            }
          }

          OLogManager.instance().info(this, "Found %d records changed in last %d operations", result.size(), lastTx.size());

          return result;
        } finally {
          writeAheadLog.removeCutTillLimit(freezeLSN);
        }

      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error on reading last changed records"), e);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    try {
      checkOpenness();

      long tot = 0;

      stateLock.acquireReadLock();
      try {
        checkOpenness();

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
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId rid, final byte[] content, final int recordVersion,
      final byte recordType, final int mode, final ORecordCallback<Long> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      final OPhysicalPosition ppos = new OPhysicalPosition(recordType);
      final OCluster cluster = getClusterById(rid.getClusterId());

      if (transaction.get() != null) {
        return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos, null);
      }

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doCreateRecord(rid, content, recordVersion, recordType, callback, cluster, ppos, null);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    try {
      if (rid.isNew())
        throw new OStorageException("Passed record with id " + rid + " is new and cannot be stored.");

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        final OCluster cluster = getClusterById(rid.getClusterId());
        checkOpenness();

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
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public boolean isDeleted(ORID rid) {
    try {
      if (rid.isNew())
        throw new OStorageException("Passed record with id " + rid + " is new and cannot be stored.");

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        final OCluster cluster = getClusterById(rid.getClusterId());
        checkOpenness();

        return cluster.isDeleted(new OPhysicalPosition(rid.getClusterPosition()));

      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        stateLock.releaseReadLock();
      }

      return false;
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public Iterator<OClusterBrowsePage> browseCluster(int clusterId) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final int finalClusterId;
        if (clusterId == ORID.CLUSTER_ID_INVALID)
          // GET THE DEFAULT CLUSTER
          finalClusterId = defaultClusterId;
        else
          finalClusterId = clusterId;
        return new Iterator<OClusterBrowsePage>() {
          private OClusterBrowsePage page = null;
          private long lastPos = -1;

          @Override
          public boolean hasNext() {
            if (page == null) {
              page = nextPage(finalClusterId, lastPos);
              if (page != null)
                lastPos = page.getLastPosition();
            }
            return page != null;
          }

          @Override
          public OClusterBrowsePage next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            OClusterBrowsePage curPage = page;
            page = null;
            return curPage;
          }
        };
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OClusterBrowsePage nextPage(int clusterId, long lastPosition) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(clusterId);
        OPhysicalPosition[] nextPositions = cluster.higherPositions(new OPhysicalPosition(lastPosition));
        if (nextPositions.length > 0) {
          long newLastPosition = nextPositions[nextPositions.length - 1].clusterPosition;
          List<OClusterBrowseEntry> nexv = new ArrayList<>();
          for (OPhysicalPosition pos : nextPositions) {
            final ORawBuffer buff = cluster.readRecord(pos.clusterPosition, false);
            nexv.add(new OClusterBrowseEntry(pos.clusterPosition, buff));
          }
          return new OClusterBrowsePage(nexv, newLastPosition);
        } else {
          return null;
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OCluster doGetAndCheckCluster(int clusterId) {
    checkClusterSegmentIndexRange(clusterId);

    final OCluster cluster = clusters.get(clusterId);
    if (cluster == null)
      throw new IllegalArgumentException("Cluster " + clusterId + " is null");
    return cluster;
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      boolean prefetchRecords, ORecordCallback<ORawBuffer> iCallback) {
    try {
      checkOpenness();
      final OCluster cluster;
      try {
        cluster = getClusterById(iRid.getClusterId());
      } catch (IllegalArgumentException e) {
        throw OException.wrapException(new ORecordNotFoundException(iRid), e);
      }

      return new OStorageOperationResult<>(readRecord(cluster, iRid, prefetchRecords));
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    try {
      checkOpenness();
      return new OStorageOperationResult<>(readRecordIfNotLatest(getClusterById(rid.getClusterId()), rid, recordVersion));
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public OStorageOperationResult<Integer> updateRecord(final ORecordId rid, final boolean updateContent, final byte[] content,
      final int version, final byte recordType, final int mode, final ORecordCallback<Integer> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      final OCluster cluster = getClusterById(rid.getClusterId());

      if (transaction.get() != null) {
        return doUpdateRecord(rid, updateContent, content, version, recordType, callback, cluster);
      }

      stateLock.acquireReadLock();
      try {
        // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpenness();

          // UPDATE IT
          return doUpdateRecord(rid, updateContent, content, version, recordType, callback, cluster);
        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OStorageOperationResult<Integer> recyclePosition(final ORecordId rid, final byte[] content, final int version,
      final byte recordType) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      final OCluster cluster = getClusterById(rid.getClusterId());
      if (transaction.get() != null) {
        return doRecycleRecord(rid, content, version, cluster, recordType);
      }

      stateLock.acquireReadLock();
      try {
        // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpenness();

          // RECYCLING IT
          return doRecycleRecord(rid, content, version, cluster, recordType);

        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
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
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      final OCluster cluster = getClusterById(rid.getClusterId());

      if (transaction.get() != null) {
        return doDeleteRecord(rid, version, cluster);
      }

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doDeleteRecord(rid, version, cluster);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(final ORecordId rid, final int mode, ORecordCallback<Boolean> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      final OCluster cluster = getClusterById(rid.getClusterId());

      if (transaction.get() != null) {
        return doHideMethod(rid, cluster);
      }

      stateLock.acquireReadLock();
      try {
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpenness();

          return doHideMethod(rid, cluster);
        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
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
    try {
      performanceStatisticManager.startThreadMonitoring();
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Completes gathering performance characteristics for current thread initiated by call of {@link
   * #startGatheringPerformanceStatisticForCurrentThread()}
   *
   * @return Performance statistic gathered after call of {@link #startGatheringPerformanceStatisticForCurrentThread()} or
   * <code>null</code> if profiling of storage was not started.
   */
  public OSessionStoragePerformanceStatistic completeGatheringPerformanceStatisticForCurrentThread() {
    try {
      return performanceStatisticManager.stopThreadMonitoring();
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    try {
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
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public Set<String> getClusterNames() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return new HashSet<>(clusterMap.keySet());
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int getClusterIdByName(final String clusterName) {
    try {
      checkOpenness();

      if (clusterName == null)
        throw new IllegalArgumentException("Cluster name is null");

      if (clusterName.length() == 0)
        throw new IllegalArgumentException("Cluster name is empty");

      // if (Character.isDigit(clusterName.charAt(0)))
      // return Integer.parseInt(clusterName);

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        // SEARCH IT BETWEEN PHYSICAL CLUSTERS

        final OCluster segment = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (segment != null)
          return segment.getId();

        return -1;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Scan the given transaction for new record and allocate a record id for them, the relative record id is inserted inside the
   * transaction for future use.
   *
   * @param clientTx the transaction of witch allocate rids
   */
  public void preallocateRids(final OTransactionInternal clientTx) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      @SuppressWarnings("unchecked")
      final Iterable<ORecordOperation> entries = clientTx.getRecordOperations();
      final TreeMap<Integer, OCluster> clustersToLock = new TreeMap<>();

      final Set<ORecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

      for (ORecordOperation txEntry : entries) {

        if (txEntry.type == ORecordOperation.CREATED) {
          newRecords.add(txEntry);
          int clusterId = txEntry.getRID().getClusterId();
          clustersToLock.put(clusterId, getClusterById(clusterId));
        }
      }
      stateLock.acquireReadLock();
      try {

        checkOpenness();

        makeStorageDirty();
        atomicOperationsManager.startAtomicOperation((String) null, true);
        try {
          lockClusters(clustersToLock);

          for (ORecordOperation txEntry : newRecords) {
            ORecord rec = txEntry.getRecord();
            if (!rec.getIdentity().isPersistent()) {
              if (rec.isDirty()) {
                //This allocate a position for a new record
                ORecordId rid = (ORecordId) rec.getIdentity().copy();
                ORecordId oldRID = rid.copy();
                final OCluster cluster = getClusterById(rid.getClusterId());
                OPhysicalPosition ppos = cluster.allocatePosition(ORecordInternal.getRecordType(rec));
                rid.setClusterPosition(ppos.clusterPosition);
                clientTx.updateIdentityAfterCommit(oldRID, rid);
              }
            } else {
              //This allocate position starting from a valid rid, used in distributed for allocate the same position on other nodes
              ORecordId rid = (ORecordId) rec.getIdentity();
              final OCluster cluster = getClusterById(rid.getClusterId());
              OPhysicalPosition ppos = cluster.allocatePosition(ORecordInternal.getRecordType(rec));
              if (ppos.clusterPosition != rid.getClusterPosition()) {
                throw new OConcurrentCreateException(rid, new ORecordId(rid.getClusterId(), ppos.clusterPosition));
              }
            }
          }
          atomicOperationsManager.endAtomicOperation(false, null);
        } catch (RuntimeException e) {
          atomicOperationsManager.endAtomicOperation(true, e);
          throw e;
        }

      } catch (IOException | RuntimeException ioe) {
        throw OException.wrapException(new OStorageException("Could not preallocate RIDs"), ioe);
      } finally {
        stateLock.releaseReadLock();
      }

    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Traditional commit that support already temporary rid and already assigned rids
   *
   * @param clientTx the transaction to commit
   *
   * @return The list of operations applied by the transaction
   */
  @Override
  public List<ORecordOperation> commit(final OTransactionInternal clientTx) {
    return commit(clientTx, false);
  }

  /**
   * Commit a transaction where the rid where pre-allocated in a previous phase
   *
   * @param clientTx the pre-allocated transaction to commit
   *
   * @return The list of operations applied by the transaction
   */
  @SuppressWarnings("UnusedReturnValue")
  public List<ORecordOperation> commitPreAllocated(final OTransactionInternal clientTx) {
    return commit(clientTx, true);
  }

  /**
   * The commit operation can be run in 3 different conditions, embedded commit, pre-allocated commit, other node commit.
   * <bold>Embedded commit</bold> is the basic commit where the operation is run in embedded or server side, the transaction arrive
   * with invalid rids that get allocated and committed.
   * <bold>pre-allocated commit</bold> is the commit that happen after an preAllocateRids call is done, this is usually run by the
   * coordinator of a tx in distributed.
   * <bold>other node commit</bold> is the commit that happen when a node execute a transaction of another node where all the rids
   * are already allocated in the other node.
   *
   * @param transaction the transaction to commit
   * @param allocated   true if the operation is pre-allocated commit
   *
   * @return The list of operations applied by the transaction
   */
  private List<ORecordOperation> commit(final OTransactionInternal transaction, boolean allocated) {
    // XXX: At this moment, there are two implementations of the commit method. One for regular client transactions and one for
    // implicit micro-transactions. The implementations are quite identical, but operate on slightly different data. If you change
    // this method don't forget to change its counterpart:
    //
    //  OAbstractPaginatedStorage.commit(com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction)

    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      txBegun.incrementAndGet();

      final ODatabaseDocumentInternal database = transaction.getDatabase();
      final OIndexManager indexManager = database.getMetadata().getIndexManager();
      final TreeMap<String, OTransactionIndexChanges> indexOperations = getSortedIndexOperations(transaction);

      database.getMetadata().makeThreadLocalSchemaSnapshot();

      final Collection<ORecordOperation> recordOperations = transaction.getRecordOperations();
      final TreeMap<Integer, OCluster> clustersToLock = new TreeMap<>();
      final Map<ORecordOperation, Integer> clusterOverrides = new IdentityHashMap<>();

      final Set<ORecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

      for (ORecordOperation recordOperation : recordOperations) {
        if (recordOperation.type == ORecordOperation.CREATED || recordOperation.type == ORecordOperation.UPDATED) {
          final ORecord record = recordOperation.getRecord();
          if (record instanceof ODocument)
            ((ODocument) record).validate();
        }

        if (recordOperation.type == ORecordOperation.UPDATED || recordOperation.type == ORecordOperation.DELETED) {
          final int clusterId = recordOperation.getRecord().getIdentity().getClusterId();
          clustersToLock.put(clusterId, getClusterById(clusterId));
        } else if (recordOperation.type == ORecordOperation.CREATED) {
          newRecords.add(recordOperation);

          final ORecord record = recordOperation.getRecord();
          final ORID rid = record.getIdentity();

          int clusterId = rid.getClusterId();

          if (record.isDirty() && clusterId == ORID.CLUSTER_ID_INVALID && record instanceof ODocument) {
            // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS

            final OImmutableClass class_ = ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
            if (class_ != null) {
              clusterId = class_.getClusterForNewInstance((ODocument) record);
              clusterOverrides.put(recordOperation, clusterId);
            }
          }

          clustersToLock.put(clusterId, getClusterById(clusterId));
        }
      }

      final List<ORecordOperation> result = new ArrayList<>();
      stateLock.acquireReadLock();
      try {
        try {
          try {

            checkOpenness();

            makeStorageDirty();
            startStorageTx(transaction);

            lockClusters(clustersToLock);

            checkReadOnlyConditions();

            Map<ORecordOperation, OPhysicalPosition> positions = new IdentityHashMap<>();
            for (ORecordOperation recordOperation : newRecords) {
              ORecord rec = recordOperation.getRecord();

              if (allocated) {
                if (rec.getIdentity().isPersistent()) {
                  positions.put(recordOperation, new OPhysicalPosition(rec.getIdentity().getClusterPosition()));
                } else {
                  throw new OStorageException("Impossible to commit a transaction with not valid rid in pre-allocated commit");
                }
              } else if (rec.isDirty() && !rec.getIdentity().isPersistent()) {
                ORecordId rid = (ORecordId) rec.getIdentity().copy();
                ORecordId oldRID = rid.copy();

                final Integer clusterOverride = clusterOverrides.get(recordOperation);
                final int clusterId = clusterOverride == null ? rid.getClusterId() : clusterOverride;

                final OCluster cluster = getClusterById(clusterId);
                OPhysicalPosition physicalPosition = cluster.allocatePosition(ORecordInternal.getRecordType(rec));
                rid.setClusterId(cluster.getId());

                if (rid.getClusterPosition() > -1) {
                  // CREATE EMPTY RECORDS UNTIL THE POSITION IS REACHED. THIS IS THE CASE WHEN A SERVER IS OUT OF SYNC
                  // BECAUSE A TRANSACTION HAS BEEN ROLLED BACK BEFORE TO SEND THE REMOTE CREATES. SO THE OWNER NODE DELETED
                  // RECORD HAVING A HIGHER CLUSTER POSITION
                  while (rid.getClusterPosition() > physicalPosition.clusterPosition) {
                    physicalPosition = cluster.allocatePosition(ORecordInternal.getRecordType(rec));
                  }

                  if (rid.getClusterPosition() != physicalPosition.clusterPosition)
                    throw new OConcurrentCreateException(rid, new ORecordId(rid.getClusterId(), physicalPosition.clusterPosition));
                }
                positions.put(recordOperation, physicalPosition);

                rid.setClusterPosition(physicalPosition.clusterPosition);

                transaction.updateIdentityAfterCommit(oldRID, rid);
              }
            }

            lockRidBags(clustersToLock, indexOperations, indexManager);

            checkReadOnlyConditions();

            for (ORecordOperation recordOperation : recordOperations) {
              commitEntry(recordOperation, positions.get(recordOperation), database.getSerializer());
              result.add(recordOperation);
            }

            lockIndexes(indexOperations);

            checkReadOnlyConditions();

            commitIndexes(indexOperations);

            final OLogSequenceNumber lsn = endStorageTx();
            final DataOutputStream journaledStream = OAbstractPaginatedStorage.journaledStream;
            if (journaledStream != null) { // send event to journaled tx stream if the streaming is on
              final int txId = transaction.getClientTransactionId();
              if (lsn == null || writeAheadLog == null) // if tx is not journaled
                try {
                  journaledStream.writeInt(txId);
                } catch (IOException e) {
                  OLogManager.instance().error(this, "unable to write tx id into journaled stream", e);
                }
              else
                writeAheadLog.addEventAt(lsn, () -> {
                  try {
                    journaledStream.writeInt(txId);
                  } catch (IOException e) {
                    OLogManager.instance().error(this, "unable to write tx id into journaled stream", e);
                  }
                });
            }

            OTransactionAbstract.updateCacheFromEntries(transaction.getDatabase(), recordOperations, true);
            txCommit.incrementAndGet();

          } catch (IOException | RuntimeException e) {
            makeRollback(transaction, e);
          } finally {
            this.transaction.set(null);
          }
        } finally {
          database.getMetadata().clearThreadLocalSchemaSnapshot();
        }
      } finally {
        stateLock.releaseReadLock();
      }

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance()
            .debug(this, "%d Committed transaction %d on database '%s' (result=%s)", Thread.currentThread().getId(),
                transaction.getId(), database.getName(), result);

      return result;
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      handleJVMError(ee);
      atomicOperationsManager.alarmClearOfAtomicOperation();
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void commitIndexes(final Map<String, OTransactionIndexChanges> indexesToCommit) {
    final Map<OIndex, OIndexAbstract.IndexTxSnapshot> snapshots = new IdentityHashMap<>();

    for (OTransactionIndexChanges changes : indexesToCommit.values()) {
      final OIndexInternal<?> index = changes.getAssociatedIndex();
      final OIndexAbstract.IndexTxSnapshot snapshot = new OIndexAbstract.IndexTxSnapshot();
      snapshots.put(index, snapshot);

      index.preCommit(snapshot);
    }

    for (OTransactionIndexChanges changes : indexesToCommit.values()) {
      final OIndexInternal<?> index = changes.getAssociatedIndex();
      final OIndexAbstract.IndexTxSnapshot snapshot = snapshots.get(index);

      index.addTxOperation(snapshot, changes);
    }

    try {
      for (OTransactionIndexChanges changes : indexesToCommit.values()) {
        final OIndexInternal<?> index = changes.getAssociatedIndex();
        final OIndexAbstract.IndexTxSnapshot snapshot = snapshots.get(index);

        index.commit(snapshot);
      }
    } finally {
      for (OTransactionIndexChanges changes : indexesToCommit.values()) {
        final OIndexInternal<?> index = changes.getAssociatedIndex();
        final OIndexAbstract.IndexTxSnapshot snapshot = snapshots.get(index);

        index.postCommit(snapshot);
      }
    }
  }

  private TreeMap<String, OTransactionIndexChanges> getSortedIndexOperations(OTransactionInternal clientTx) {
    return new TreeMap<>(clientTx.getIndexOperations());
  }

  public int loadIndexEngine(String name) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OIndexEngine engine = indexEngineNameMap.get(name);
        if (engine == null)
          return -1;

        final int indexId = indexEngines.indexOf(engine);
        assert indexId >= 0;

        return indexId;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public int loadExternalIndexEngine(String engineName, String algorithm, String indexType, OIndexDefinition indexDefinition,
      OBinarySerializer valueSerializer, boolean isAutomatic, Boolean durableInNonTxMode, int version,
      Map<String, String> engineProperties) {
    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        // this method introduced for binary compatibility only
        if (getConfiguration().binaryFormatVersion > 15)
          return -1;

        if (indexEngineNameMap.containsKey(engineName))
          throw new OIndexException("Index with name " + engineName + " already exists");

        makeStorageDirty();

        final OBinarySerializer keySerializer = determineKeySerializer(indexDefinition);
        final int keySize = determineKeySize(indexDefinition);
        final OType[] keyTypes = indexDefinition != null ? indexDefinition.getTypes() : null;
        final boolean nullValuesSupport = indexDefinition != null && !indexDefinition.isNullValuesIgnored();

        final OStorageConfigurationImpl.IndexEngineData engineData = new OStorageConfigurationImpl.IndexEngineData(engineName,
            algorithm, indexType, durableInNonTxMode, version, valueSerializer.getId(), keySerializer.getId(), isAutomatic,
            keyTypes, nullValuesSupport, keySize, engineProperties);

        final OIndexEngine engine = OIndexes
            .createIndexEngine(engineName, algorithm, indexType, durableInNonTxMode, this, version, engineProperties, null);
        engine.load(engineName, valueSerializer, isAutomatic, keySerializer, keyTypes, nullValuesSupport, keySize,
            engineData.getEngineProperties());

        indexEngineNameMap.put(engineName, engine);
        indexEngines.add(engine);
        getConfiguration().addIndexEngine(engineName, engineData);

        return indexEngines.size() - 1;
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public int addIndexEngine(String engineName, final String algorithm, final String indexType,
      final OIndexDefinition indexDefinition, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final Boolean durableInNonTxMode, final int version, final Map<String, String> engineProperties,
      final Set<String> clustersToIndex, final ODocument metadata) {
    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        if (indexEngineNameMap.containsKey(engineName)) {
          // OLD INDEX FILE ARE PRESENT: THIS IS THE CASE OF PARTIAL/BROKEN INDEX
          OLogManager.instance().warn(this, "Index with name '%s' already exists, removing it and re-create the index", engineName);
          final OIndexEngine engine = indexEngineNameMap.remove(engineName);
          if (engine != null) {
            indexEngines.remove(engine);
            getConfiguration().deleteIndexEngine(engineName);
            engine.delete();
          }
        }

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
            .createIndexEngine(engineName, algorithm, indexType, durableInNonTxMode, this, version, engineProperties, metadata);

        engine.create(valueSerializer, isAutomatic, keyTypes, nullValuesSupport, keySerializer, keySize, clustersToIndex,
            engineProperties, metadata);

        indexEngineNameMap.put(engineName, engine);

        indexEngines.add(engine);

        final OStorageConfigurationImpl.IndexEngineData engineData = new OStorageConfigurationImpl.IndexEngineData(engineName,
            algorithm, indexType, durableInNonTxMode, version, serializerId, keySerializer.getId(), isAutomatic, keyTypes,
            nullValuesSupport, keySize, engineProperties);

        getConfiguration().addIndexEngine(engineName, engineData);

        return indexEngines.size() - 1;
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
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
            throw new IllegalStateException("Cannot load binary serializer, storage is not properly initialized");
        }
      }
    } else {
      keySerializer = new OSimpleKeySerializer();
    }
    return keySerializer;
  }

  public void deleteIndexEngine(int indexId) throws OInvalidIndexEngineIdException {
    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        checkIndexId(indexId);

        makeStorageDirty();
        final OIndexEngine engine = indexEngines.get(indexId);

        indexEngines.set(indexId, null);

        engine.delete();

        final String engineName = engine.getName();
        indexEngineNameMap.remove(engineName);
        getConfiguration().deleteIndexEngine(engineName);
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error on index deletion"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void checkIndexId(int indexId) throws OInvalidIndexEngineIdException {
    if (indexId < 0 || indexId >= indexEngines.size() || indexEngines.get(indexId) == null)
      throw new OInvalidIndexEngineIdException("Engine with id " + indexId + " is not registered inside of storage");
  }

  public boolean indexContainsKey(int indexId, Object key) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doIndexContainsKey(indexId, key);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return doIndexContainsKey(indexId, key);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doIndexContainsKey(int indexId, Object key) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.contains(key);
  }

  public boolean removeKeyFromIndex(int indexId, Object key) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null) {
        return doRemoveKeyFromIndex(indexId, key);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        return doRemoveKeyFromIndex(indexId, key);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doRemoveKeyFromIndex(int indexId, Object key) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      makeStorageDirty();
      final OIndexEngine engine = indexEngines.get(indexId);

      return engine.remove(key);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during removal of entry with key " + key + " from index "), e);
    }
  }

  public void clearIndex(int indexId) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null) {
        doClearIndex(indexId);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        doClearIndex(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doClearIndex(int indexId) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);

      makeStorageDirty();
      engine.clear();
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during clearing of index"), e);
    }

  }

  public Object getIndexValue(int indexId, Object key) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexValue(indexId, key);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexValue(indexId, key);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private Object doGetIndexValue(int indexId, Object key) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.get(key);
  }

  public OIndexEngine getIndexEngine(int indexId) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);
      return indexEngines.get(indexId);
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void updateIndexEntry(int indexId, Object key, OIndexKeyUpdater<Object> valueCreator) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null) {
        doUpdateIndexEntry(indexId, key, valueCreator);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        doUpdateIndexEntry(indexId, key, valueCreator);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public <T> T callIndexEngine(boolean atomicOperation, boolean readOperation, int indexId, OIndexEngineCallback<T> callback)
      throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doCallIndexEngine(atomicOperation, readOperation, indexId, callback);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        return doCallIndexEngine(atomicOperation, readOperation, indexId, callback);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private <T> T doCallIndexEngine(boolean atomicOperation, boolean readOperation, int indexId, OIndexEngineCallback<T> callback)
      throws OInvalidIndexEngineIdException {
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

  private void doUpdateIndexEntry(int indexId, Object key, OIndexKeyUpdater<Object> valueCreator) throws OInvalidIndexEngineIdException {
    try {
      atomicOperationsManager.startAtomicOperation((String) null, true);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
    }

    try {

      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      engine.update(key,valueCreator);
      /*
      final Object value = valueCreator.call();
      if (value == null)
        engine.remove(key);
      else
        engine.put(key, value);
        */

      atomicOperationsManager.endAtomicOperation(false, null);
    } catch (OInvalidIndexEngineIdException e) {
      try {
        atomicOperationsManager.endAtomicOperation(true, e);
      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Error during operation rollback"), ioe);
      }

      throw e;
    } catch (Exception e) {
      try {
        atomicOperationsManager.endAtomicOperation(true, e);
        throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Error during operation rollback"), ioe);
      }
    }
  }

  public void putIndexValue(int indexId, Object key, Object value) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null) {
        doPutIndexValue(indexId, key, value);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        doPutIndexValue(indexId, key, value);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doPutIndexValue(int indexId, Object key, Object value) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      engine.put(key, value);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key " + key + " value " + value + " entry to the index"), e);
    }
  }

  /**
   * Puts the given value under the given key into this storage for the index with the given index id. Validates the operation using
   * the provided validator.
   *
   * @param indexId   the index id of the index to put the value into.
   * @param key       the key to put the value under.
   * @param value     the value to put.
   * @param validator the operation validator.
   *
   * @return {@code true} if the validator allowed the put, {@code false} otherwise.
   *
   * @see OIndexEngine.Validator#validate(Object, Object, Object)
   */
  @SuppressWarnings("UnusedReturnValue")
  public boolean validatedPutIndexValue(int indexId, Object key, OIdentifiable value,
      OIndexEngine.Validator<Object, OIdentifiable> validator) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doValidatedPutIndexValue(indexId, key, value, validator);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        return doValidatedPutIndexValue(indexId, key, value, validator);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doValidatedPutIndexValue(int indexId, Object key, OIdentifiable value,
      OIndexEngine.Validator<Object, OIdentifiable> validator) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      return engine.validatedPut(key, value, validator);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key " + key + " value " + value + " entry to the index"), e);
    }
  }

  public Object getIndexFirstKey(int indexId) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexFirstKey(indexId);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexFirstKey(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private Object doGetIndexFirstKey(int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.getFirstKey();
  }

  public Object getIndexLastKey(int indexId) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexFirstKey(indexId);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexLastKey(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private Object doGetIndexLastKey(int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.getLastKey();
  }

  public OIndexCursor iterateIndexEntriesBetween(int indexId, Object rangeFrom, boolean fromInclusive, Object rangeTo,
      boolean toInclusive, boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doIterateIndexEntriesBetween(indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doIterateIndexEntriesBetween(indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doIterateIndexEntriesBetween(int indexId, Object rangeFrom, boolean fromInclusive, Object rangeTo,
      boolean toInclusive, boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateIndexEntriesMajor(int indexId, Object fromKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doIterateIndexEntriesMajor(int indexId, Object fromKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateIndexEntriesMinor(int indexId, final Object toKey, final boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doIterateIndexEntriesMinor(int indexId, Object toKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor getIndexCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexCursor(indexId, valuesTransformer);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexCursor(indexId, valuesTransformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doGetIndexCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.cursor(valuesTransformer);
  }

  public OIndexCursor getIndexDescCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexDescCursor(indexId, valuesTransformer);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexDescCursor(indexId, valuesTransformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doGetIndexDescCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.descCursor(valuesTransformer);
  }

  public OIndexKeyCursor getIndexKeyCursor(int indexId) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexKeyCursor(indexId);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexKeyCursor(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexKeyCursor doGetIndexKeyCursor(int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.keyCursor();
  }

  public long getIndexSize(int indexId, OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doGetIndexSize(indexId, transformer);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexSize(indexId, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private long doGetIndexSize(int indexId, OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.size(transformer);
  }

  public boolean hasIndexRangeQuerySupport(int indexId) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null)
        return doHasRangeQuerySupport(indexId);

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doHasRangeQuerySupport(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doHasRangeQuerySupport(int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OIndexEngine engine = indexEngines.get(indexId);

    return engine.hasRangeQuerySupport();
  }

  private void makeRollback(OTransactionInternal clientTx, Exception e) {
    // WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
    OLogManager.instance()
        .debug(this, "Error during transaction commit, transaction will be rolled back (tx-id=%d)", e, clientTx.getId());
    rollback(clientTx);
    if (e instanceof RuntimeException)
      throw ((RuntimeException) e);
    else
      throw OException.wrapException(new OStorageException("Error during transaction commit"), e);

  }

  private void makeRollback(OMicroTransaction microTransaction, Exception e) {
    // WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
    OLogManager.instance().debug(this, "Error during micro-transaction commit, micro-transaction will be rolled back (tx-id=%d)", e,
        microTransaction.getId());

    try {
      rollback(microTransaction);
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Exception during transaction rollback, `%08X`", ex, System.identityHashCode(ex));
    }

    if (e instanceof RuntimeException)
      throw ((RuntimeException) e);
    else
      throw OException.wrapException(new OStorageException("Error during micro-transaction commit"), e);

  }

  @Override
  public void rollback(final OTransactionInternal clientTx) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        try {
          checkOpenness();

          if (transaction.get() == null)
            return;

          if (transaction.get().getClientTx().getId() != clientTx.getId())
            throw new OStorageException(
                "Passed in and active transaction are different transactions. Passed in transaction cannot be rolled back.");

          makeStorageDirty();
          rollbackStorageTx();

          OTransactionAbstract.updateCacheFromEntries(clientTx.getDatabase(), clientTx.getRecordOperations(), false);

          txRollback.incrementAndGet();

        } catch (IOException e) {
          throw OException.wrapException(new OStorageException("Error during transaction rollback"), e);
        } finally {
          transaction.set(null);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Rollbacks the given micro-transaction.
   *
   * @param microTransaction the micro-transaction to rollback.
   */
  public void rollback(OMicroTransaction microTransaction) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        try {
          checkOpenness();

          if (transaction.get() == null)
            return;

          if (transaction.get().getMicroTransaction().getId() != microTransaction.getId())
            throw new OStorageException(
                "Passed in and active micro-transaction are different micro-transactions. Passed in micro-transaction cannot be "
                    + "rolled back.");

          makeStorageDirty();
          rollbackStorageTx();

          microTransaction.updateRecordCacheAfterRollback();

          txRollback.incrementAndGet();

        } catch (IOException e) {
          throw OException.wrapException(new OStorageException("Error during micro-transaction rollback"), e);
        } finally {
          transaction.set(null);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    try {
      return ppos != null && !ORecordVersionHelper.isTombstone(ppos.recordVersion);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void synch() {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        final long timer = Orient.instance().getProfiler().startChrono();
        final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
        try {
          checkOpenness();
          if (jvmError.get() == null) {
            for (OIndexEngine indexEngine : indexEngines)
              try {
                if (indexEngine != null)
                  indexEngine.flush();
              } catch (Throwable t) {
                OLogManager.instance().error(this, "Error while flushing index via index engine of class %s.", t,
                    indexEngine.getClass().getSimpleName());
              }

            if (writeAheadLog != null) {
              makeFullCheckpoint();
              return;
            }

            writeCache.flush();

            if (configuration != null)
              getConfiguration().synch();

            clearStorageDirty();
          } else {
            OLogManager.instance().errorNoDb(this, "Sync can not be performed because of JVM error on storage", null);
          }

        } catch (IOException e) {
          throw OException.wrapException(new OStorageException("Error on synch storage '" + name + "'"), e);

        } finally {
          atomicOperationsManager.releaseAtomicOperations(lockId);
          //noinspection ResultOfMethodCallIgnored
          Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public String getPhysicalClusterNameById(final int iClusterId) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (iClusterId < 0 || iClusterId >= clusters.size())
          return null;

        return clusters.get(iClusterId) != null ? clusters.get(iClusterId).getName() : null;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  @Override
  public void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  @Override
  public OCluster getClusterById(int iClusterId) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (iClusterId == ORID.CLUSTER_ID_INVALID)
          // GET THE DEFAULT CLUSTER
          iClusterId = defaultClusterId;

        final OCluster cluster = doGetAndCheckCluster(iClusterId);

        return cluster;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OCluster getClusterByName(final String clusterName) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        final OCluster cluster = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));

        if (cluster == null)
          throw new OStorageException("Cluster " + clusterName + " does not exist in database '" + name + "'");
        return cluster;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long getSize() {
    try {
      try {
        long size = 0;

        stateLock.acquireReadLock();
        try {
          for (OCluster c : clusters)
            if (c != null)
              size += c.getRecordsSize();
        } finally {
          stateLock.releaseReadLock();
        }

        return size;
      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Cannot calculate records size"), ioe);
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int getClusters() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return clusterMap.size();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public Set<OCluster> getClusterInstances() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();
        final Set<OCluster> result = new HashSet<>();

        // ADD ALL THE CLUSTERS
        for (OCluster c : clusters)
          if (c != null)
            result.add(c);

        return result;

      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Method that completes the cluster rename operation. <strong>IT WILL NOT RENAME A CLUSTER, IT JUST CHANGES THE NAME IN THE
   * INTERNAL MAPPING</strong>
   */
  public void renameCluster(final String oldName, final String newName) {
    try {
      clusterMap.put(newName.toLowerCase(configuration.getLocaleInstance()),
          clusterMap.remove(oldName.toLowerCase(configuration.getLocaleInstance())));
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public boolean cleanOutRecord(final ORecordId recordId, final int recordVersion, final int iMode,
      final ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, iMode, callback).getResult();
  }

  @Override
  public boolean isFrozen() {
    try {
      return atomicOperationsManager.isFrozen();
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void freeze(final boolean throwException) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (throwException)
          atomicOperationsManager
              .freezeAtomicOperations(OModificationOperationProhibitedException.class, "Modification requests are prohibited");
        else
          atomicOperationsManager.freezeAtomicOperations(null, null);

        final List<OFreezableStorageComponent> frozenIndexes = new ArrayList<>(indexEngines.size());
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
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void release() {
    try {
      for (OIndexEngine indexEngine : indexEngines)
        if (indexEngine != null && indexEngine instanceof OFreezableStorageComponent)
          ((OFreezableStorageComponent) indexEngine).release();

      atomicOperationsManager.releaseAtomicOperations(-1);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
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

  @Override
  public void reload() {
    try {
      close();
      open(null, null, null);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @SuppressWarnings("unused")
  public String getMode() {
    return mode;
  }

  @Override
  public void lowDiskSpace(OLowDiskSpaceInformation information) {
    lowDiskSpace = information;
  }

  /**
   * @inheritDoc
   */
  @Override
  public void pageIsBroken(String fileName, long pageIndex) {
    brokenPages.add(new OPair<>(fileName, pageIndex));
  }

  @Override
  public void requestCheckpoint() {
    try {
      if (!walVacuumInProgress.get() && walVacuumInProgress.compareAndSet(false, true)) {
        fuzzyCheckpointExecutor.submit(new WALVacuum());
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Executes the command request and return the result back.
   */
  @Override
  public Object command(final OCommandRequestText iCommand) {
    try {
      while (true) {
        try {
          final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

          // COPY THE CONTEXT FROM THE REQUEST
          executor.setContext(iCommand.getContext());

          executor.setProgressListener(iCommand.getProgressListener());
          executor.parse(iCommand);

          return executeCommand(iCommand, executor);
        } catch (ORetryQueryException ignore) {

          if (iCommand instanceof OQueryAbstract) {
            final OQueryAbstract query = (OQueryAbstract) iCommand;
            query.reset();
          }

        }
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @SuppressWarnings("WeakerAccess")
  public Object executeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor) {
    try {
      if (iCommand.isIdempotent() && !executor.isIdempotent())
        throw new OCommandExecutionException("Cannot execute non idempotent command");

      long beginTime = Orient.instance().getProfiler().startChrono();

      try {

        ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();

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
          Map<Object, Object> params = iCommand.getParameters();
          result = executor.execute(params);

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
          final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
          if (db != null) {
            final OSecurityUser user = db.getUser();
            final String userString = user != null ? user.toString() : null;
            //noinspection ResultOfMethodCallIgnored
            Orient.instance().getProfiler()
                .stopChrono("db." + ODatabaseRecordThreadLocal.instance().get().getName() + ".command." + iCommand.toString(),
                    "Command executed against the database", beginTime, "db.*.command.*", null, userString);
          }
        }
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(currentClusterId);
        return cluster.higherPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(clusterId);
        return cluster.ceilingPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(currentClusterId);

        return cluster.lowerPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(clusterId);

        return cluster.floorPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void acquireWriteLock(final ORID rid) {
    try {
      lockManager.acquireLock(rid, OComparableLockManager.LOCK.EXCLUSIVE, RECORD_LOCK_TIMEOUT);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void releaseWriteLock(final ORID rid) {
    try {
      lockManager.releaseLock(this, rid, OComparableLockManager.LOCK.EXCLUSIVE);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void acquireReadLock(final ORID rid) {
    try {
      lockManager.acquireLock(rid, OComparableLockManager.LOCK.SHARED, RECORD_LOCK_TIMEOUT);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void releaseReadLock(final ORID rid) {
    try {
      lockManager.releaseLock(this, rid, OComparableLockManager.LOCK.SHARED);
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return recordConflictStrategy;
  }

  @Override
  public void setConflictStrategy(final ORecordConflictStrategy conflictResolver) {
    this.recordConflictStrategy = conflictResolver;
  }

  @SuppressWarnings("unused")
  public AtomicLong getRecordScanned() {
    return recordScanned;
  }

  @SuppressWarnings("unused")
  protected abstract OLogSequenceNumber copyWALToIncrementalBackup(ZipOutputStream zipOutputStream, long startSegment)
      throws IOException;

  @SuppressWarnings("unused")
  protected abstract boolean isWriteAllowedDuringIncrementalBackup();

  @SuppressWarnings("unused")
  public OStorageRecoverListener getRecoverListener() {
    return recoverListener;
  }

  public void registerRecoverListener(final OStorageRecoverListener recoverListener) {
    this.recoverListener = recoverListener;
  }

  @SuppressWarnings("unused")
  public void unregisterRecoverListener(final OStorageRecoverListener recoverListener) {
    if (this.recoverListener == recoverListener)
      this.recoverListener = null;
  }

  @SuppressWarnings("unused")
  protected abstract File createWalTempDirectory();

  @SuppressWarnings("unused")
  protected abstract void addFileToDirectory(String name, InputStream stream, File directory) throws IOException;

  @SuppressWarnings("unused")
  protected abstract OWriteAheadLog createWalFromIBUFiles(File directory) throws IOException;

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  @SuppressWarnings("WeakerAccess")
  protected void checkOpenness() {
    if (status != STATUS.OPEN)
      throw new OStorageException("Storage " + name + " is not opened.");
  }

  protected void makeFuzzyCheckpoint() {
    if (writeAheadLog == null)
      return;

    //check every 1 ms.
    while (!stateLock.tryAcquireReadLock(1_000_000)) {
      if (status != STATUS.OPEN)
        return;
    }

    try {
      if (status != STATUS.OPEN || writeAheadLog == null)
        return;

      final OLogSequenceNumber endLSN = writeAheadLog.end();

      final OLogSequenceNumber minLSN = writeCache.getMinimalNotFlushedLSN();
      final long fuzzySegment;

      if (minLSN != null) {
        fuzzySegment = minLSN.getSegment();
      } else {
        if (endLSN == null)
          return;

        fuzzySegment = endLSN.getSegment();
      }

      writeCache.makeFuzzyCheckpoint(fuzzySegment);

    } catch (IOException ioe) {
      throw OException.wrapException(new OIOException("Error during fuzzy checkpoint"), ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  protected void makeFullCheckpoint() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    if (statistic != null)
      statistic.startFullCheckpointTimer();
    try {
      if (writeAheadLog == null)
        return;

      try {
        writeAheadLog.flush();

        if (configuration != null)
          getConfiguration().synch();

        //so we will be able to cut almost all the log
        writeAheadLog.appendNewSegment();

        final OLogSequenceNumber lastLSN = writeAheadLog.logFullCheckpointStart();
        writeCache.flush();
        writeAheadLog.logFullCheckpointEnd();
        writeAheadLog.flush();

        writeAheadLog.cutTill(lastLSN);

        if (jvmError.get() == null) {
          clearStorageDirty();
        }

      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Error during checkpoint creation for storage " + name), ioe);
      }

      fullCheckpointCount.increment();
    } finally {
      if (statistic != null)
        statistic.stopFullCheckpointTimer();
    }
  }

  public long getFullCheckpointCount() {
    return fullCheckpointCount.sum();
  }

  protected void preOpenSteps() throws IOException {
  }

  @SuppressWarnings({ "WeakerAccess", "EmptyMethod" })
  protected void postCreateSteps() {
  }

  protected void preCreateSteps() throws IOException {
  }

  protected abstract void initWalAndDiskCache(OContextConfiguration contextConfiguration) throws IOException, InterruptedException;

  protected abstract void postCloseSteps(@SuppressWarnings("unused") boolean onDelete, boolean jvmError) throws IOException;

  @SuppressWarnings({ "EmptyMethod", "WeakerAccess" })
  protected void preCloseSteps() {
  }

  protected void postDeleteSteps() {
  }

  protected void makeStorageDirty() throws IOException {
  }

  protected void clearStorageDirty() throws IOException {
  }

  protected boolean isDirty() {
    return false;
  }

  private ORawBuffer readRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    checkOpenness();

    if (!rid.isPersistent())
      throw new ORecordNotFoundException(rid,
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    if (transaction.get() != null) {
      return doReadRecordIfNotLatest(cluster, rid, recordVersion);
    }

    stateLock.acquireReadLock();
    try {
      ORawBuffer buff;
      checkOpenness();

      buff = doReadRecordIfNotLatest(cluster, rid, recordVersion);
      return buff;
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid, boolean prefetchRecords) {
    checkOpenness();

    if (!rid.isPersistent())
      throw new ORecordNotFoundException(rid,
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    if (transaction.get() != null) {
      // Disabled this assert have no meaning anymore
      // assert iLockingStrategy.equals(LOCKING_STRATEGY.DEFAULT);
      return doReadRecord(clusterSegment, rid, prefetchRecords);
    }

    stateLock.acquireReadLock();
    try {
      checkOpenness();
      return doReadRecord(clusterSegment, rid, prefetchRecords);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private OLogSequenceNumber endStorageTx() throws IOException {
    final OLogSequenceNumber lsn = atomicOperationsManager.endAtomicOperation(false, null);
    assert atomicOperationsManager.getCurrentOperation() == null;
    return lsn;
  }

  private void startStorageTx(OTransactionInternal clientTx) throws IOException {
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

  private void startStorageTx(OMicroTransaction microTransaction) throws IOException {
    final OStorageTransaction storageTx = transaction.get();

    if (storageTx != null && storageTx.getMicroTransaction().getId() != microTransaction.getId())
      rollback(microTransaction);

    assert atomicOperationsManager.getCurrentOperation() == null;

    transaction.set(new OStorageTransaction(microTransaction));
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

        makeFullCheckpoint();
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
        rid.setClusterPosition(ppos.clusterPosition);

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

        throw ODatabaseException.wrapException(new OStorageException("Error during creation of record"), e);
      }

      if (callback != null)
        callback.call(rid, ppos.clusterPosition);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Created record %s v.%s size=%d bytes", rid, recordVersion, content.length);

      recordCreated.incrementAndGet();

      return new OStorageOperationResult<>(ppos);
    } catch (IOException ioe) {
      throw OException.wrapException(
          new OStorageException("Error during record deletion in cluster " + (cluster != null ? cluster.getName() : "")), ioe);
    }
  }

  private OStorageOperationResult<Integer> doUpdateRecord(final ORecordId rid, final boolean updateContent, byte[] content,
      final int version, final byte recordType, final ORecordCallback<Integer> callback, final OCluster cluster) {

    Orient.instance().getProfiler().startChrono();
    try {

      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
      if (!checkForRecordValidity(ppos)) {
        final int recordVersion = -1;
        if (callback != null)
          callback.call(rid, recordVersion);

        return new OStorageOperationResult<>(recordVersion);
      }

      boolean contentModified = false;
      if (updateContent) {
        final AtomicInteger recVersion = new AtomicInteger(version);
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
          cluster.updateRecord(rid.getClusterPosition(), content, ppos.recordVersion, recordType);

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

        return new OStorageOperationResult<>(recordVersion);
      }

      //if we do not update content of the record we should keep version of the record the same
      //otherwise we would have issues when two records may have the same version but different content
      int newRecordVersion;
      if (updateContent) {
        newRecordVersion = ppos.recordVersion;
      } else {
        newRecordVersion = version;
      }

      if (callback != null)
        callback.call(rid, newRecordVersion);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Updated record %s v.%s size=%d", rid, newRecordVersion, content.length);

      recordUpdated.incrementAndGet();

      if (contentModified)
        return new OStorageOperationResult<>(newRecordVersion, content, false);
      else
        return new OStorageOperationResult<>(newRecordVersion);
    } catch (OConcurrentModificationException e) {
      recordConflict.incrementAndGet();
      throw e;
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Error on updating record " + rid + " (cluster: " + cluster.getName() + ")"), ioe);
    }

  }

  private OStorageOperationResult<Integer> doRecycleRecord(final ORecordId rid, byte[] content, final int version,
      final OCluster cluster, final byte recordType) {

    try {
      makeStorageDirty();
      atomicOperationsManager.startAtomicOperation((String) null, true);
      try {
        cluster.recycleRecord(rid.getClusterPosition(), content, version, recordType);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null)
          context.executeOperations(this);
        atomicOperationsManager.endAtomicOperation(false, null);
      } catch (RuntimeException e) {
        atomicOperationsManager.endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        atomicOperationsManager.endAtomicOperation(true, e);

        OLogManager.instance().error(this, "Error on recycling record " + rid + " (cluster: " + cluster + ")", e);

        throw OException
            .wrapException(new OStorageException("Error on recycling record " + rid + " (cluster: " + cluster + ")"), e);
      }

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Recycled record %s v.%s size=%d", rid, version, content.length);

      return new OStorageOperationResult<>(version, content, false);

    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on recycling record " + rid + " (cluster: " + cluster + ")", ioe);

      throw OException
          .wrapException(new OStorageException("Error on recycling record " + rid + " (cluster: " + cluster + ")"), ioe);
    }
  }

  private OStorageOperationResult<Boolean> doDeleteRecord(ORecordId rid, final int version, OCluster cluster) {
    Orient.instance().getProfiler().startChrono();
    try {

      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));

      if (ppos == null)
        // ALREADY DELETED
        return new OStorageOperationResult<>(false);

      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (version > -1 && ppos.recordVersion != version) {
        recordConflict.incrementAndGet();

        if (OFastConcurrentModificationException.enabled())
          throw OFastConcurrentModificationException.instance();
        else
          throw new OConcurrentModificationException(rid, ppos.recordVersion, version, ORecordOperation.DELETED);
      }

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
        return new OStorageOperationResult<>(false);
      }

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Deleted record %s v.%s", rid, version);

      recordDeleted.incrementAndGet();

      return new OStorageOperationResult<>(true);
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster.getName() + ")"), ioe);
    }
  }

  private OStorageOperationResult<Boolean> doHideMethod(ORecordId rid, OCluster cluster) {
    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));

      if (ppos == null)
        // ALREADY HIDDEN
        return new OStorageOperationResult<>(false);

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

        return new OStorageOperationResult<>(false);
      }

      return new OStorageOperationResult<>(true);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw OException.wrapException(new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")"), ioe);
    }
  }

  private ORawBuffer doReadRecord(final OCluster clusterSegment, final ORecordId rid, boolean prefetchRecords) {
    try {

      final ORawBuffer buff = clusterSegment.readRecord(rid.getClusterPosition(), prefetchRecords);

      if (buff != null && OLogManager.instance().isDebugEnabled())
        OLogManager.instance()
            .debug(this, "Read record %s v.%s size=%d bytes", rid, buff.version, buff.buffer != null ? buff.buffer.length : 0);

      recordRead.incrementAndGet();

      return buff;
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private ORawBuffer doReadRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    try {
      return cluster.readRecordIfVersionIsNotLatest(rid.getClusterPosition(), recordVersion);
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
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), OMetadataDefault.CLUSTER_INTERNAL_NAME, null,
            true, 20, 4, storageCompression, storageEncryption, encryptionKey, stgConflictStrategy,
            OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), OMetadataDefault.CLUSTER_INDEX_NAME, null,
            false, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, storageCompression, storageEncryption, encryptionKey,
            stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME,
            null, false, 1, 1, storageCompression, storageEncryption, encryptionKey, stgConflictStrategy,
            OStorageClusterConfiguration.STATUS.ONLINE));

    defaultClusterId = createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), CLUSTER_DEFAULT_NAME, null, true,
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
      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(config.getName(), getConfiguration().version, this);
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
   *
   * @return The id (physical position into the array) of the new cluster just created. First is 0.
   */
  private int registerCluster(final OCluster cluster) {
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

      cluster = OPaginatedClusterFactory.INSTANCE.createCluster(clusterName, getConfiguration().version, this);
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

    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance()
          .debug(this, "Created cluster '%s' in database '%s' with id %d. Clusters: %s", clusterName, url, createdClusterId,
              clusters);

    return createdClusterId;
  }

  private void doClose(boolean force, boolean onDelete) {
    if (!force && !onDelete)
      return;

    if (status == STATUS.CLOSED)
      return;

    final long timer = Orient.instance().getProfiler().startChrono();
    int fuzzyCheckpointWaitTimeout = getConfiguration().getContextConfiguration()
        .getValueAsInteger(OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_SHUTDOWN_TIMEOUT);

    ScheduledExecutorService executor = fuzzyCheckpointExecutor;
    stateLock.acquireWriteLock();
    try {
      if (status == STATUS.CLOSED)
        return;

      status = STATUS.CLOSING;

      if (jvmError.get() == null) {
        readCache.storeCacheState(writeCache);

        if (!onDelete && jvmError.get() == null)
          makeFullCheckpoint();

        preCloseSteps();

        sbTreeCollectionManager.close();

        // we close all files inside cache system so we only clear cluster metadata
        clusters.clear();
        clusterMap.clear();

        // we close all files inside cache system so we only clear index metadata and close non core indexes
        for (OIndexEngine engine : indexEngines) {
          if (engine != null && !(engine instanceof OSBTreeIndexEngine || engine instanceof OHashTableIndexEngine)) {
            if (onDelete)
              engine.delete();
            else
              engine.close();
          }
        }

        indexEngines.clear();
        indexEngineNameMap.clear();

        if (getConfiguration() != null)
          if (onDelete)
            getConfiguration().delete();
          else
            getConfiguration().close();

        super.close(force, onDelete);

        if (writeCache != null) {
          writeCache.removeLowDiskSpaceListener(this);
          writeCache.removeBackgroundExceptionListener(this);
          writeCache.removePageIsBrokenListener(this);
        }

        if (writeAheadLog != null) {
          writeAheadLog.removeFullCheckpointListener(this);
          writeAheadLog.removeLowDiskSpaceListener(this);
        }

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

        postCloseSteps(onDelete, jvmError.get() != null);

        if (atomicOperationsManager != null)
          try {
            atomicOperationsManager.unregisterMBean();
          } catch (Exception e) {
            OLogManager.instance().error(this, "MBean for atomic operations manager cannot be unregistered", e);
          }

        transaction = null;
        fuzzyCheckpointExecutor = null;
      } else {
        OLogManager.instance()
            .errorNoDb(this, "Because of JVM error happened inside of storage it can not be properly closed", null);
      }

      status = STATUS.CLOSED;
    } catch (IOException e) {
      final String message = "Error on closing of storage '" + name;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new OStorageException(message), e);

    } finally {
      //noinspection ResultOfMethodCallIgnored
      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
      stateLock.releaseWriteLock();
    }

    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(fuzzyCheckpointWaitTimeout, TimeUnit.SECONDS)) {
          throw new OStorageException("Can not able to terminate fuzzy checkpoint");
        }
      } catch (InterruptedException e) {
        throw OException.wrapException(new OInterruptedException("Thread was interrupted during fuzzy checkpoint termination"), e);
      }
    }
  }

  @SuppressWarnings("unused")
  protected void closeClusters(boolean onDelete) throws IOException {
    for (OCluster cluster : clusters)
      if (cluster != null)
        cluster.close(!onDelete);

    clusters.clear();
    clusterMap.clear();
  }

  @SuppressWarnings("unused")
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

  private void commitEntry(final ORecordOperation txEntry, final OPhysicalPosition allocated, ORecordSerializer serializer) {

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
      final OCluster cluster = getClusterById(rid.getClusterId());

      if (cluster.getName().equals(OMetadataDefault.CLUSTER_INDEX_NAME) || cluster.getName()
          .equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME))
        // AVOID TO COMMIT INDEX STUFF
        return;

      switch (txEntry.type) {
      case ORecordOperation.LOADED:
        break;

      case ORecordOperation.CREATED: {

        final byte[] stream = serializer.toStream(rec, false);
        if (allocated != null) {
          final OPhysicalPosition ppos;
          final byte recordType = ORecordInternal.getRecordType(rec);
          ppos = doCreateRecord(rid, stream, rec.getVersion(), recordType, null, cluster, new OPhysicalPosition(recordType),
              allocated).getResult();

          ORecordInternal.setVersion(rec, ppos.recordVersion);
        } else {
          // USE -2 AS VERSION TO AVOID INCREMENTING THE VERSION
          final OStorageOperationResult<Integer> updateRes = updateRecord(rid, ORecordInternal.isContentChanged(rec), stream, -2,
              ORecordInternal.getRecordType(rec), -1, null);
          ORecordInternal.setVersion(rec, updateRes.getResult());
          if (updateRes.getModifiedRecordContent() != null)
            ORecordInternal.fill(rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);
        }

        break;
      }

      case ORecordOperation.UPDATED: {
        final byte[] stream = serializer.toStream(rec, false);

        final OStorageOperationResult<Integer> updateRes = doUpdateRecord(rid, ORecordInternal.isContentChanged(rec), stream,
            rec.getVersion(), ORecordInternal.getRecordType(rec), null, cluster);
        ORecordInternal.setVersion(rec, updateRes.getResult());
        if (updateRes.getModifiedRecordContent() != null)
          ORecordInternal.fill(rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);

        break;
      }

      case ORecordOperation.DELETED: {
        if (rec instanceof ODocument)
          ORidBagDeleter.deleteAllRidBags((ODocument) rec);
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
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId < 0 || iClusterId > clusters.size() - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  private OLogSequenceNumber restoreFromWAL() throws IOException {
    if (writeAheadLog == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead logging is switched off.", null);
      return null;
    }

    if (writeAheadLog.begin() == null) {
      OLogManager.instance().error(this, "Restore is not possible because write ahead log is empty.", null);
      return null;
    }

    OLogManager.instance().info(this, "Looking for last checkpoint...");

    final OLogSequenceNumber end = writeAheadLog.end();
    if (end == null) {
      OLogManager.instance().errorNoDb(this, "WAL is empty, there is nothing not restore", null);
      return null;
    }

    writeAheadLog.addCutTillLimit(end);
    try {
      OLogSequenceNumber lastCheckPoint;
      try {
        lastCheckPoint = writeAheadLog.getLastCheckpoint();
      } catch (OWALPageBrokenException ignore) {
        lastCheckPoint = null;
      }

      if (lastCheckPoint == null) {
        OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
        return restoreFromBeginning();
      }

      OWALRecord checkPointRecord;
      try {
        checkPointRecord = writeAheadLog.read(lastCheckPoint);
      } catch (OWALPageBrokenException ignore) {
        checkPointRecord = null;
      }

      if (checkPointRecord == null) {
        OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
        return restoreFromBeginning();
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
            return restoreFromBeginning();
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
            return restoreFromBeginning();
          }
        } else
          return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
      }

      throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());
    } finally {
      writeAheadLog.removeCutTillLimit(end);
    }
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
    } catch (OWALPageBrokenException ignore) {
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
    } catch (OWALPageBrokenException ignore) {
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
    OLogManager.instance().infoNoDb(this, "Data restore procedure from FUZZY checkpoint is started.");
    OLogSequenceNumber flushedLsn = checkPointRecord.getFlushedLsn();

    if (flushedLsn.compareTo(writeAheadLog.begin()) < 0) {
      OLogManager.instance().errorNoDb(this,
          "Fuzzy checkpoint points to removed part of the log, " + "will try to restore data from the rest of the WAL", null);
      flushedLsn = writeAheadLog.begin();
    }

    return restoreFrom(flushedLsn, writeAheadLog);
  }

  private OLogSequenceNumber restoreFromBeginning() throws IOException {
    OLogManager.instance().info(this, "Data restore procedure is started.");
    OLogSequenceNumber lsn = writeAheadLog.begin();

    return restoreFrom(lsn, writeAheadLog);
  }

  @SuppressWarnings("WeakerAccess")
  protected OLogSequenceNumber restoreFrom(OLogSequenceNumber lsn, OWriteAheadLog writeAheadLog) throws IOException {
    OLogSequenceNumber logSequenceNumber = null;
    OModifiableBoolean atLeastOnePageUpdate = new OModifiableBoolean();

    long recordsProcessed = 0;

    final int reportBatchSize = OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();
    final Map<OOperationUnitId, List<OWALRecord>> operationUnits = new HashMap<>();

    long lastReportTime = 0;

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
          List<OWALRecord> operationList = new ArrayList<>();

          assert !operationUnits.containsKey(((OAtomicUnitStartRecord) walRecord).getOperationUnitId());

          operationUnits.put(((OAtomicUnitStartRecord) walRecord).getOperationUnitId(), operationList);
          operationList.add(walRecord);
        } else if (walRecord instanceof OOperationUnitRecord) {
          OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) walRecord;

          List<OWALRecord> operationList = operationUnits.get(operationUnitRecord.getOperationUnitId());

          if (operationList == null || operationList.isEmpty()) {
            OLogManager.instance().errorNoDb(this, "'Start transaction' record is absent for atomic operation", null);

            if (operationList == null) {
              operationList = new ArrayList<>();
              operationUnits.put(operationUnitRecord.getOperationUnitId(), operationList);
            }
          }

          operationList.add(operationUnitRecord);
        } else if (walRecord instanceof ONonTxOperationPerformedWALRecord) {
          if (!wereNonTxOperationsPerformedInPreviousOpen) {
            OLogManager.instance().warnNoDb(this, "Non tx operation was used during data modification we will need index rebuild.");
            wereNonTxOperationsPerformedInPreviousOpen = true;
          }
        } else
          OLogManager.instance().warnNoDb(this, "Record %s will be skipped during data restore", walRecord);

        recordsProcessed++;

        final long currentTime = System.currentTimeMillis();
        if (reportBatchSize > 0 && recordsProcessed % reportBatchSize == 0
            || currentTime - lastReportTime > WAL_RESTORE_REPORT_INTERVAL) {
          OLogManager.instance()
              .infoNoDb(this, "%d operations were processed, current LSN is %s last LSN is %s", recordsProcessed, lsn,
                  writeAheadLog.end());
          lastReportTime = currentTime;
        }

        lsn = writeAheadLog.next(lsn);
      }

      OLogManager.instance()
          .infoNoDb(this, "There are %d unfinished atomic operations left, they will be rolled back", operationUnits.size());

      if (!operationUnits.isEmpty()) {
        for (List<OWALRecord> atomicOperation : operationUnits.values()) {
          revertAtomicUnit(atomicOperation, atLeastOnePageUpdate);
        }
      }
    } catch (OWALPageBrokenException e) {
      OLogManager.instance()
          .errorNoDb(this, "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.",
              e);
    } catch (RuntimeException e) {
      OLogManager.instance().errorNoDb(this,
          "Data restore was paused because of exception. The rest of changes will be rolled back and WAL files will be backed up."
              + " Please report issue about this exception to bug tracker and provide WAL files which are backed up in 'wal_backup' directory.",
          e);
      backUpWAL(e);
    }

    if (atLeastOnePageUpdate.getValue())
      return logSequenceNumber;

    return null;
  }

  private void backUpWAL(Exception e) {
    try {
      final File rootDir = new File(getConfiguration().getDirectory());
      final File backUpDir = new File(rootDir, "wal_backup");
      if (!backUpDir.exists()) {
        final boolean created = backUpDir.mkdir();
        if (!created) {
          OLogManager.instance().error(this, "Cannot create directory for backup files " + backUpDir.getAbsolutePath(), null);
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
        OLogManager.instance().error(this, "Cannot create backup file " + archiveFile.getAbsolutePath(), null);
        return;
      }

      try (final FileOutputStream archiveOutputStream = new FileOutputStream(archiveFile)) {
        try (final ZipOutputStream archiveZipOutputStream = new ZipOutputStream(new BufferedOutputStream(archiveOutputStream))) {

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

          archiveEntry(archiveZipOutputStream, ((ODiskWriteAheadLog) writeAheadLog).getWMRFile().toString());
        }
      }
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error during WAL backup", ioe);
    }
  }

  private void archiveEntry(ZipOutputStream archiveZipOutputStream, String walSegment) throws IOException {
    final File walFile = new File(walSegment);
    final ZipEntry walZipEntry = new ZipEntry(walFile.getName());
    archiveZipOutputStream.putNextEntry(walZipEntry);
    try {
      try (FileInputStream walInputStream = new FileInputStream(walFile)) {
        try (BufferedInputStream walBufferedInputStream = new BufferedInputStream(walInputStream)) {
          final byte[] buffer = new byte[1024];
          int readBytes;

          while ((readBytes = walBufferedInputStream.read(buffer)) > -1) {
            archiveZipOutputStream.write(buffer, 0, readBytes);
          }
        }
      }
    } finally {
      archiveZipOutputStream.closeEntry();
    }
  }

  private void revertAtomicUnit(List<OWALRecord> atomicUnit, OModifiableBoolean atLeastOnePageUpdate) throws IOException {
    final ListIterator<OWALRecord> recordsIterator = atomicUnit.listIterator(atomicUnit.size());

    while (recordsIterator.hasPrevious()) {
      final OWALRecord record = recordsIterator.previous();

      if (record instanceof OFileDeletedWALRecord) {
        OLogManager.instance().infoNoDb(this, "Deletion of file can not be rolled back");
      } else if (record instanceof OFileCreatedWALRecord) {
        final OFileCreatedWALRecord fileCreatedWALRecord = (OFileCreatedWALRecord) record;
        OLogManager.instance().infoNoDb(this, "File %s is going to be deleted", fileCreatedWALRecord.getFileName());

        if (writeCache.exists(fileCreatedWALRecord.getFileId())) {
          readCache.deleteFile(fileCreatedWALRecord.getFileId(), writeCache);
          OLogManager.instance().infoNoDb(this, "File %s was deleted", fileCreatedWALRecord.getFileName());
        } else {
          OLogManager.instance().infoNoDb(this, "File %d is absent and can not be deleted", fileCreatedWALRecord.getFileName());
        }
      } else if (record instanceof OUpdatePageRecord) {
        final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) record;

        long fileId = updatePageRecord.getFileId();
        if (!writeCache.exists(fileId)) {
          final String fileName = writeCache.restoreFileById(fileId);

          throw new OStorageException("File with id " + fileId + " and name " + fileName
              + " was deleted from storage, the rest of operations can not be restored");

        }

        final long pageIndex = updatePageRecord.getPageIndex();
        fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

        OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, true, writeCache, 1, false);
        if (cacheEntry == null) {
          //page may not exist because it was not flushed that is OK, we just go forward
          continue;
        }

        try {
          final ODurablePage durablePage = new ODurablePage(cacheEntry);
          durablePage.rollbackChanges(updatePageRecord.getChanges());
          durablePage.setLsn(updatePageRecord.getPrevLsn());
        } finally {
          readCache.releaseFromWrite(cacheEntry, writeCache);
        }

        atLeastOnePageUpdate.setValue(true);
      } else if (record instanceof OAtomicUnitStartRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else if (record instanceof OAtomicUnitEndRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else {
        OLogManager.instance()
            .errorNoDb(this, "Invalid WAL record type was passed %s. Given record will be skipped.", null, record.getClass());

        assert false : "Invalid WAL record type was passed " + record.getClass().getName();
      }
    }

  }

  @SuppressWarnings("WeakerAccess")
  protected void restoreAtomicUnit(List<OWALRecord> atomicUnit, OModifiableBoolean atLeastOnePageUpdate) throws IOException {
    assert atomicUnit.get(atomicUnit.size() - 1) instanceof OAtomicUnitEndRecord;

    for (OWALRecord walRecord : atomicUnit) {
      if (walRecord instanceof OFileDeletedWALRecord) {
        OFileDeletedWALRecord fileDeletedWALRecord = (OFileDeletedWALRecord) walRecord;
        if (writeCache.exists(fileDeletedWALRecord.getFileId()))
          readCache.deleteFile(fileDeletedWALRecord.getFileId(), writeCache);
      } else if (walRecord instanceof OFileCreatedWALRecord) {
        OFileCreatedWALRecord fileCreatedCreatedWALRecord = (OFileCreatedWALRecord) walRecord;

        if (!writeCache.exists(fileCreatedCreatedWALRecord.getFileName())) {
          readCache.addFile(fileCreatedCreatedWALRecord.getFileName(), fileCreatedCreatedWALRecord.getFileId(), writeCache);
        }
      } else if (walRecord instanceof OUpdatePageRecord) {
        final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) walRecord;

        long fileId = updatePageRecord.getFileId();
        if (!writeCache.exists(fileId)) {
          String fileName = writeCache.restoreFileById(fileId);

          if (fileName == null) {
            throw new OStorageException(
                "File with id " + fileId + " was deleted from storage, the rest of operations can not be restored");
          } else {
            OLogManager.instance().warn(this, "Previously deleted file with name " + fileName
                + " was deleted but new empty file was added to continue restore process");
          }
        }

        final long pageIndex = updatePageRecord.getPageIndex();
        fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

        OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, true, writeCache, 1, false);
        if (cacheEntry == null) {
          do {
            if (cacheEntry != null)
              readCache.releaseFromWrite(cacheEntry, writeCache);

            cacheEntry = readCache.allocateNewPage(fileId, writeCache, false);
          } while (cacheEntry.getPageIndex() != pageIndex);
        }

        try {
          ODurablePage durablePage = new ODurablePage(cacheEntry);
          durablePage.restoreChanges(updatePageRecord.getChanges());
          durablePage.setLsn(updatePageRecord.getLsn());
        } finally {
          readCache.releaseFromWrite(cacheEntry, writeCache);
        }

        atLeastOnePageUpdate.setValue(true);
      } else if (walRecord instanceof OAtomicUnitStartRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else {
        OLogManager.instance()
            .error(this, "Invalid WAL record type was passed %s. Given record will be skipped.", null, walRecord.getClass());

        assert false : "Invalid WAL record type was passed " + walRecord.getClass().getName();
      }
    }
  }

  /**
   * Method which is called before any data modification operation to check alarm conditions such as: <ol> <li>Low disk space</li>
   * <li>Exception during data flush in background threads</li> <li>Broken files</li> </ol>
   * If one of those conditions are satisfied data modification operation is aborted and storage is switched in "read only" mode.
   */
  private void checkLowDiskSpaceRequestsAndReadOnlyConditions() {
    if (transaction.get() != null)
      return;

    if (lowDiskSpace != null) {
      if (checkpointInProgress.compareAndSet(false, true)) {
        try {
          if (writeCache.checkLowDiskSpace()) {

            OLogManager.instance().error(this, "Not enough disk space, force sync will be called", null);
            synch();

            if (writeCache.checkLowDiskSpace()) {
              throw new OLowDiskSpaceException("Error occurred while executing a write operation to database '" + name
                  + "' due to limited free space on the disk (" + (lowDiskSpace.freeSpace / (1024 * 1024))
                  + " MB). The database is now working in read-only mode."
                  + " Please close the database (or stop OrientDB), make room on your hard drive and then reopen the database. "
                  + "The minimal required space is " + (lowDiskSpace.requiredSpace / (1024 * 1024)) + " MB. "
                  + "Required space is now set to " + getConfiguration().getContextConfiguration()
                  .getValueAsInteger(OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT)
                  + "MB (you can change it by setting parameter " + OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getKey()
                  + ") .");
            } else {
              lowDiskSpace = null;
            }
          } else
            lowDiskSpace = null;
        } catch (IOException e) {
          throw OException.wrapException(new OStorageException("Error during low disk space handling"), e);
        } finally {
          checkpointInProgress.set(false);
        }
      }
    }

    checkReadOnlyConditions();
  }

  public void checkReadOnlyConditions() {
    if (dataFlushException != null) {
      throw OException.wrapException(new OStorageException(
              "Error in data flush background thread, please restart database and send full stack trace inside of bug report"),
          dataFlushException);
    }

    if (!brokenPages.isEmpty()) {
      //order pages by file and index
      final Map<String, SortedSet<Long>> pagesByFile = new HashMap<>();

      for (OPair<String, Long> brokenPage : brokenPages) {
        final SortedSet<Long> sortedPages = pagesByFile.computeIfAbsent(brokenPage.key, (fileName) -> new TreeSet<>());
        sortedPages.add(brokenPage.value);
      }

      final StringBuilder brokenPagesList = new StringBuilder();
      brokenPagesList.append("[");

      for (String fileName : pagesByFile.keySet()) {
        brokenPagesList.append('\'').append(fileName).append("' :");

        final SortedSet<Long> pageIndexes = pagesByFile.get(fileName);
        final long lastPage = pageIndexes.last();

        for (Long pageIndex : pagesByFile.get(fileName)) {
          brokenPagesList.append(pageIndex);
          if (pageIndex != lastPage) {
            brokenPagesList.append(", ");
          }
        }

        brokenPagesList.append(";");
      }
      brokenPagesList.append("]");

      throw new OPageIsBrokenException("Following files and pages are detected to be broken " + brokenPagesList + ", storage is "
          + "switched to 'read only' mode. Any modification operations are prohibited. "
          + "To restore database and make it fully operational you may export and import database " + "to and from JSON.");

    }

    if (jvmError.get() != null) {
      throw new OJVMErrorException("JVM error '" + jvmError.get().getClass().getSimpleName() + " : " + jvmError.get().getMessage()
          + "' occurred during data processing, storage is switched to 'read-only' mode. "
          + "To prevent this exception please restart the JVM and check data consistency by calling of 'check database' "
          + "command from database console.");
    }

  }

  @SuppressWarnings("unused")
  public void setStorageConfigurationUpdateListener(OStorageConfigurationUpdateListener storageConfigurationUpdateListener) {
    this.getConfiguration().setConfigurationUpdateListener(storageConfigurationUpdateListener);
  }

  private static class ORIDOLockManager extends OComparableLockManager<ORID> {
    ORIDOLockManager() {
      super(true, -1);
    }

    @Override
    protected ORID getImmutableResourceId(ORID iResourceId) {
      return new ORecordId(iResourceId);
    }
  }

  @SuppressWarnings("unused")
  protected Map<Integer, List<ORecordId>> getRidsGroupedByCluster(final Collection<ORecordId> iRids) {
    final Map<Integer, List<ORecordId>> ridsPerCluster = new HashMap<>();
    for (ORecordId rid : iRids) {
      List<ORecordId> rids = ridsPerCluster.computeIfAbsent(rid.getClusterId(), k -> new ArrayList<>(iRids.size()));
      rids.add(rid);
    }
    return ridsPerCluster;
  }

  private void lockIndexes(final TreeMap<String, OTransactionIndexChanges> indexes) {
    for (OTransactionIndexChanges changes : indexes.values()) {
      assert changes.changesPerKey instanceof TreeMap;

      final OIndexInternal<?> index = changes.getAssociatedIndex();

      final List<Object> orderedIndexNames = new ArrayList<>(changes.changesPerKey.keySet());
      if (orderedIndexNames.size() > 1)
        orderedIndexNames.sort((o1, o2) -> {
          String i1 = index.getIndexNameByKey(o1);
          String i2 = index.getIndexNameByKey(o2);
          return i1.compareTo(i2);
        });

      boolean fullyLocked = false;
      for (Object key : orderedIndexNames)
        if (index.acquireAtomicExclusiveLock(key)) {
          fullyLocked = true;
          break;
        }
      if (!fullyLocked && !changes.nullKeyChanges.entries.isEmpty())
        index.acquireAtomicExclusiveLock(null);
    }
  }

  private void lockClusters(final TreeMap<Integer, OCluster> clustersToLock) {
    for (OCluster cluster : clustersToLock.values())
      cluster.acquireAtomicExclusiveLock();
  }

  private void lockRidBags(final TreeMap<Integer, OCluster> clusters, final TreeMap<String, OTransactionIndexChanges> indexes,
      OIndexManager manager) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

    for (Integer clusterId : clusters.keySet())
      atomicOperationsManager
          .acquireExclusiveLockTillOperationComplete(atomicOperation, OSBTreeCollectionManagerAbstract.generateLockName(clusterId));

    for (Map.Entry<String, OTransactionIndexChanges> entry : indexes.entrySet()) {
      final String indexName = entry.getKey();
      final OIndexInternal<?> index = entry.getValue().resolveAssociatedIndex(indexName, manager);

      if (!index.isUnique())
        atomicOperationsManager
            .acquireExclusiveLockTillOperationComplete(atomicOperation, OIndexRIDContainerSBTree.generateLockName(indexName));
    }
  }

  private void registerProfilerHooks() {
    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".createRecord", "Number of created records", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(recordCreated), "db.*.createRecord");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".readRecord", "Number of read records", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(recordRead), "db.*.readRecord");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".updateRecord", "Number of updated records", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(recordUpdated), "db.*.updateRecord");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".deleteRecord", "Number of deleted records", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(recordDeleted), "db.*.deleteRecord");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".scanRecord", "Number of read scanned", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(recordScanned), "db.*.scanRecord");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".recyclePosition", "Number of recycled records", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(recordRecycled), "db.*.recyclePosition");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".conflictRecord", "Number of conflicts during updating and deleting records",
            OProfiler.METRIC_TYPE.COUNTER, new AtomicLongOProfilerHookValue(recordConflict), "db.*.conflictRecord");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".txBegun", "Number of transactions begun", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(txBegun), "db.*.txBegun");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".txCommit", "Number of committed transactions", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(txCommit), "db.*.txCommit");

    Orient.instance().getProfiler()
        .registerHookValue("db." + this.name + ".txRollback", "Number of rolled back transactions", OProfiler.METRIC_TYPE.COUNTER,
            new AtomicLongOProfilerHookValue(txRollback), "db.*.txRollback");
  }

  protected RuntimeException logAndPrepareForRethrow(RuntimeException runtimeException) {
    if (!(runtimeException instanceof OHighLevelException || runtimeException instanceof ONeedRetryException))
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", runtimeException, System.identityHashCode(runtimeException),
              getURL(), OConstants.getVersion());
    return runtimeException;
  }

  protected Error logAndPrepareForRethrow(Error error) {
    return logAndPrepareForRethrow(error, true);
  }

  private Error logAndPrepareForRethrow(Error error, boolean putInReadOnlyMode) {
    if (!(error instanceof OHighLevelException))
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", error, System.identityHashCode(error), getURL(),
              OConstants.getVersion());

    if (putInReadOnlyMode) {
      handleJVMError(error);
    }

    return error;
  }

  protected RuntimeException logAndPrepareForRethrow(Throwable throwable) {
    if (!(throwable instanceof OHighLevelException || throwable instanceof ONeedRetryException))
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", throwable, System.identityHashCode(throwable), getURL(),
              OConstants.getVersion());
    return new RuntimeException(throwable);
  }

  private OInvalidIndexEngineIdException logAndPrepareForRethrow(OInvalidIndexEngineIdException exception) {
    OLogManager.instance()
        .errorStorage(this, "Exception `%08X` in storage `%s` : %s", exception, System.identityHashCode(exception), getURL(),
            OConstants.getVersion());
    return exception;
  }

  private static class FuzzyCheckpointThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    }
  }

  private final class WALVacuum implements Runnable {

    WALVacuum() {
    }

    @Override
    public void run() {
      stateLock.acquireReadLock();
      try {
        if (status == STATUS.CLOSED)
          return;

        final long[] nonActiveSegments = writeAheadLog.nonActiveSegments();
        if (nonActiveSegments.length == 0)
          return;

        long flushTillSegmentId;
        if (nonActiveSegments.length == 1) {
          flushTillSegmentId = writeAheadLog.activeSegment();
        } else {
          flushTillSegmentId = (nonActiveSegments[0] + nonActiveSegments[nonActiveSegments.length - 1]) / 2;
        }

        long minDirtySegment;
        do {
          writeCache.flushTillSegment(flushTillSegmentId);

          //we should take min lsn BEFORE min write cache LSN call
          //to avoid case when new data are changed before call
          OLogSequenceNumber endLSN = writeAheadLog.end();
          OLogSequenceNumber minDirtyLSN = writeCache.getMinimalNotFlushedLSN();

          if (minDirtyLSN == null) {
            minDirtySegment = endLSN.getSegment();
          } else {
            minDirtySegment = minDirtyLSN.getSegment();
          }
        } while (minDirtySegment < flushTillSegmentId);

        writeCache.makeFuzzyCheckpoint(minDirtySegment);

      } catch (Exception e) {
        dataFlushException = e;
        OLogManager.instance().error(this, "Error during flushing of data for fuzzy checkpoint", e);
      } finally {
        stateLock.releaseReadLock();
        walVacuumInProgress.set(false);
      }
    }
  }

  @Override
  public OStorageConfigurationImpl getConfiguration() {
    return (OStorageConfigurationImpl) configuration;
  }

}
