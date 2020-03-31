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
import com.orientechnologies.common.concur.lock.OComparableLockManager;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.AtomicLongOProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCallable;
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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDeleter;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.encryption.impl.ONothingEncryption;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.*;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeMultiValueIndexEngine;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeSingleValueIndexEngine;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingIndexEngine;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.cluster.OOfflineCluster;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsTable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OCASDiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OFuzzyCheckpointStartMetadataRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.*;
import com.orientechnologies.orient.core.tx.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.RID_BAG_SBTREEBONSAI_DELETE_DALAY;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 28.03.13
 */
public abstract class OAbstractPaginatedStorage extends OStorageAbstract
    implements OLowDiskSpaceListener, OCheckpointRequestListener, OIdentifiableStorage, OBackgroundExceptionListener,
    OFreezableStorageComponent, OPageIsBrokenListener {
  protected static final OScheduledThreadPoolExecutorWithLogging fuzzyCheckpointExecutor;
  private static final   OScheduledThreadPoolExecutorWithLogging storageProfilerExecutor;

  private static final int                          RECORD_LOCK_TIMEOUT                = OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT
      .getValueAsInteger();
  private static final int                          WAL_RESTORE_REPORT_INTERVAL        = 30 * 1000; // milliseconds
  private static final Comparator<ORecordOperation> COMMIT_RECORD_OPERATION_COMPARATOR = Comparator
      .comparing(o -> o.getRecord().getIdentity());

  private int txProfilerTrigger;
  private int txProfilerInterval;
  private int txProfilerStackTraceMapThreshold;

  static {
    fuzzyCheckpointExecutor = new OScheduledThreadPoolExecutorWithLogging(1, new FuzzyCheckpointThreadFactory());
    fuzzyCheckpointExecutor.setMaximumPoolSize(1);

    storageProfilerExecutor = new OScheduledThreadPoolExecutorWithLogging(0, new StorageProfilerThreadFactory());
  }

  @SuppressWarnings("WeakerAccess")
  protected final OSBTreeCollectionManagerShared sbTreeCollectionManager;
  private final   OComparableLockManager<ORID>   lockManager;
  /**
   * Lock is used to atomically update record versions.
   */
  private final   OLockManager<ORID>             recordVersionManager;
  private final   Map<String, OCluster>          clusterMap = new HashMap<>();
  private final   List<OCluster>                 clusters   = new ArrayList<>();

  private volatile ThreadLocal<OStorageTransaction> transaction;
  private final    AtomicBoolean                    checkpointInProgress = new AtomicBoolean();
  private final    AtomicBoolean                    walVacuumInProgress  = new AtomicBoolean();

  /**
   * Error which happened inside of storage or during data processing related to this storage.
   */
  private final AtomicReference<Error>       jvmError                    = new AtomicReference<>();
  private final OPerformanceStatisticManager performanceStatisticManager = new OPerformanceStatisticManager(this,
      OGlobalConfiguration.STORAGE_PROFILER_SNAPSHOT_INTERVAL.getValueAsInteger() * 1000000L,
      OGlobalConfiguration.STORAGE_PROFILER_CLEANUP_INTERVAL.getValueAsInteger() * 1000000L);

  protected volatile OWriteAheadLog          writeAheadLog;
  private            OStorageRecoverListener recoverListener;

  protected volatile OReadCache  readCache;
  protected volatile OWriteCache writeCache;

  private volatile ORecordConflictStrategy recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
      .getDefaultImplementation();

  private volatile   int                      defaultClusterId = -1;
  protected volatile OAtomicOperationsManager atomicOperationsManager;
  private volatile   boolean                  wereNonTxOperationsPerformedInPreviousOpen;
  private volatile   OLowDiskSpaceInformation lowDiskSpace;
  private volatile   boolean                  pessimisticLock;
  /**
   * Set of pages which were detected as broken and need to be repaired.
   */
  private final      Set<OPair<String, Long>> brokenPages      = Collections.newSetFromMap(new ConcurrentHashMap<>(0));

  private volatile Throwable dataFlushException;

  private final int id;

  private final    Map<String, OBaseIndexEngine> indexEngineNameMap = new HashMap<>();
  private final    List<OBaseIndexEngine>        indexEngines       = new ArrayList<>();
  private final    AtomicOperationIdGen          idGen              = new AtomicOperationIdGen();
  private          boolean                       wereDataRestoredAfterOpen;
  private volatile Optional<byte[]>              lastMetadata       = Optional.empty();

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

  private final AtomicInteger sessionCount  = new AtomicInteger(0);
  private final AtomicLong    lastCloseTime = new AtomicLong(System.currentTimeMillis());

  protected AtomicOperationsTable atomicOperationsTable;

  public OAbstractPaginatedStorage(final String name, final String filePath, final String mode, final int id) {
    super(name, filePath, mode);

    this.id = id;
    lockManager = new ORIDOLockManager();
    recordVersionManager = new OPartitionedLockManager<>();

    registerProfilerHooks();
    sbTreeCollectionManager = new OSBTreeCollectionManagerShared(this);
  }

  private static void checkPageSizeAndRelatedParametersInGlobalConfiguration() {
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
    final int freeListBoundary = OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger() * 1024;
    final int maxKeySize = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();

    if (freeListBoundary > pageSize / 2) {
      throw new OStorageException("Value of parameter " + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
          + " should be at least 2 times bigger than value of parameter "
          + OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getKey() + " but real values are :"
          + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey() + " = " + pageSize + " , "
          + OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getKey() + " = " + freeListBoundary);
    }

    if (maxKeySize > pageSize / 4) {
      throw new OStorageException("Value of parameter " + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
          + " should be at least 4 times bigger than value of parameter " + OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getKey()
          + " but real values are :" + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey() + " = " + pageSize + " , "
          + OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getKey() + " = " + maxKeySize);
    }
  }

  private static TreeMap<String, OTransactionIndexChanges> getSortedIndexOperations(final OTransactionInternal clientTx) {
    return new TreeMap<>(clientTx.getIndexOperations());
  }

  @Override
  public final void open(final String iUserName, final String iUserPassword, final OContextConfiguration contextConfiguration) {
    open(contextConfiguration);
  }

  public final void open(final OContextConfiguration contextConfiguration) {
    try {
      stateLock.acquireReadLock();
      try {
        if (status == STATUS.OPEN)
        // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
        // REUSED
        {
          return;
        }
      } finally {
        stateLock.releaseReadLock();
      }

      stateLock.acquireWriteLock();
      try {

        if (status == STATUS.OPEN)
        // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
        // REUSED
        {
          return;
        }

        if (!exists()) {
          throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);
        }

        pessimisticLock = contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING);

        initWalAndDiskCache(contextConfiguration);
        transaction = new ThreadLocal<>();

        final long lastTxId = checkIfStorageDirty();
        if (lastTxId > 0) {
          idGen.setStartId(lastTxId + 1);
        } else {
          idGen.setStartId(0);
        }

        atomicOperationsTable = new AtomicOperationsTable(
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_INTERVAL),
            idGen.getLastId() + 1);
        atomicOperationsManager = new OAtomicOperationsManager(this, atomicOperationsTable);
        recoverIfNeeded();

        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
          if (OClusterBasedStorageConfiguration.exists(writeCache)) {
            configuration = new OClusterBasedStorageConfiguration(this);
            ((OClusterBasedStorageConfiguration) configuration).load(contextConfiguration);

            //otherwise delayed to disk based storage to convert old format to new format.
          }

          initConfiguration(atomicOperation, contextConfiguration);

          checkPageSizeAndRelatedParameters();

          componentsFactory = new OCurrentStorageComponentsFactory(configuration);

          openClusters();
          openIndexes();

          status = STATUS.OPEN;

          final String cs = configuration.getConflictStrategy();
          if (cs != null) {
            // SET THE CONFLICT STORAGE STRATEGY FROM THE LOADED CONFIGURATION
            doSetConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(cs), atomicOperation);
          }

          readCache.loadCacheState(writeCache);

          initTxProfiler(contextConfiguration);

          if (!lastMetadata.isPresent()) {
            lastMetadata = Optional.ofNullable(((OClusterBasedStorageConfiguration) configuration).getLastMetadata());
          }

        });
      } catch (final RuntimeException e) {
        try {
          if (writeCache != null) {
            readCache.closeStorage(writeCache);
          }
        } catch (final Exception ee) {
          //ignore
        }

        try {
          if (writeAheadLog != null) {
            writeAheadLog.close();
          }
        } catch (final Exception ee) {
          //ignore
        }

        try {
          postCloseSteps(false, false, idGen.getLastId());
        } catch (final Exception ee) {
          //ignore
        }

        status = STATUS.CLOSED;
        throw e;
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    OLogManager.instance()
        .infoNoDb(this, "Storage '%s' is opened under OrientDB distribution : %s", getURL(), OConstants.getVersion());
  }

  private void initTxProfiler(OContextConfiguration contextConfiguration) {
    txProfilerTrigger = contextConfiguration.getValueAsInteger(OGlobalConfiguration.PROFILER_TX_TRIGGER_INTERVAL);
    txProfilerInterval = contextConfiguration.getValueAsInteger(OGlobalConfiguration.PROFILER_TX_TRACK_INTERVAL);
    txProfilerStackTraceMapThreshold = contextConfiguration
        .getValueAsInteger(OGlobalConfiguration.PROFILER_TX_STACK_TRACE_MAP_THRESHOLD);
  }

  /**
   * That is internal method which is called once we encounter any error inside of JVM. In such case we need to restart JVM to avoid
   * any data corruption. Till JVM is not restarted storage will be put in read-only state.
   */
  public final void handleJVMError(final Error e) {
    if (jvmError.compareAndSet(null, e)) {
      OLogManager.instance().errorNoDb(this, "JVM error was thrown", e);
    }
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
  public final String getCreatedAtVersion() {
    return configuration.getCreatedAtVersion();
  }

  @SuppressWarnings("WeakerAccess")
  protected final void openIndexes() {
    final OCurrentStorageComponentsFactory cf = componentsFactory;
    if (cf == null) {
      throw new OStorageException("Storage '" + name + "' is not properly initialized");
    }

    final Set<String> indexNames = configuration.indexEngines();
    for (final String indexName : indexNames) {
      final OStorageConfiguration.IndexEngineData engineData = configuration.getIndexEngine(indexName);
      final OBaseIndexEngine engine = OIndexes
          .createIndexEngine(engineData.getName(), engineData.getAlgorithm(), engineData.getIndexType(),
              engineData.getDurableInNonTxMode(), this, engineData.getVersion(), engineData.getApiVersion(),
              engineData.isMultivalue(), engineData.getEngineProperties(), null);

      final OEncryption encryption;
      if (engineData.getEncryption() == null || engineData.getEncryption().toLowerCase(configuration.getLocaleInstance())
          .equals(ONothingEncryption.NAME)) {
        encryption = null;
      } else {
        encryption = OEncryptionFactory.INSTANCE.getEncryption(engineData.getEncryption(), engineData.getEncryptionOptions());
      }

      if (engineData.getApiVersion() < 1) {
        ((OIndexEngine) engine)
            .load(engineData.getName(), cf.binarySerializerFactory.getObjectSerializer(engineData.getValueSerializerId()),
                engineData.isAutomatic(), cf.binarySerializerFactory.getObjectSerializer(engineData.getKeySerializedId()),
                engineData.getKeyTypes(), engineData.isNullValuesSupport(), engineData.getKeySize(),
                engineData.getEngineProperties(), encryption);

      } else {
        ((OV1IndexEngine) engine).load(engineData.getName(), engineData.getKeySize(), engineData.getKeyTypes(),
            cf.binarySerializerFactory.getObjectSerializer(engineData.getKeySerializedId()), encryption);
      }

      indexEngineNameMap.put(engineData.getName(), engine);
      indexEngines.add(engine);
    }
  }

  @SuppressWarnings("WeakerAccess")
  protected final void openClusters() throws IOException {
    // OPEN BASIC SEGMENTS
    int pos;

    // REGISTER CLUSTER
    final List<OStorageClusterConfiguration> configurationClusters = configuration.getClusters();
    for (int i = 0; i < configurationClusters.size(); ++i) {
      final OStorageClusterConfiguration clusterConfig = configurationClusters.get(i);

      if (clusterConfig != null) {
        pos = createClusterFromConfig(clusterConfig);

        try {
          if (pos == -1) {
            clusters.get(i).open();
          } else {
            if (clusterConfig.getName().equals(CLUSTER_DEFAULT_NAME)) {
              defaultClusterId = pos;
            }

            clusters.get(pos).open();
          }
        } catch (final FileNotFoundException e) {
          OLogManager.instance().warn(this, "Error on loading cluster '" + configurationClusters.get(i).getName() + "' (" + i
              + "): file not found. It will be excluded from current database '" + getName() + "'.", e);

          clusterMap.remove(configurationClusters.get(i).getName().toLowerCase(configuration.getLocaleInstance()));

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
  public void create(final OContextConfiguration contextConfiguration) {
    checkPageSizeAndRelatedParametersInGlobalConfiguration();

    try {
      stateLock.acquireWriteLock();
      try {
        if (name == null) {
          throw new OInvalidDatabaseNameException("Database name can not be null");
        }

        if (name.isEmpty()) {
          throw new OInvalidDatabaseNameException("Database name can not be empty");
        }

        final Pattern namePattern = Pattern.compile("[^\\w\\d$_-]+");
        final Matcher matcher = namePattern.matcher(name);
        if (matcher.find()) {
          throw new OInvalidDatabaseNameException(
              "Only letters, numbers, `$`, `_` and `-` are allowed in database name. Provided name :`" + name + "`");
        }

        if (status != STATUS.CLOSED) {
          throw new OStorageExistsException("Cannot create new storage '" + getURL() + "' because it is not closed");
        }

        if (exists()) {
          throw new OStorageExistsException("Cannot create new storage '" + getURL() + "' because it already exists");
        }

        pessimisticLock = contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING);

        initWalAndDiskCache(contextConfiguration);
        atomicOperationsTable = new AtomicOperationsTable(
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_INTERVAL),
            idGen.getLastId() + 1);
        atomicOperationsManager = new OAtomicOperationsManager(this, atomicOperationsTable);
        transaction = new ThreadLocal<>();

        preCreateSteps();
        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
          configuration = new OClusterBasedStorageConfiguration(this);
          ((OClusterBasedStorageConfiguration) configuration).create(atomicOperation, contextConfiguration);

          componentsFactory = new OCurrentStorageComponentsFactory(configuration);

          status = STATUS.OPEN;

          // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
          doAddCluster(atomicOperation, OMetadataDefault.CLUSTER_INTERNAL_NAME, null);

          ((OClusterBasedStorageConfiguration) configuration).setCreationVersion(atomicOperation, OConstants.getVersion());
          ((OClusterBasedStorageConfiguration) configuration)
              .setPageSize(atomicOperation, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
          ((OClusterBasedStorageConfiguration) configuration).setFreeListBoundary(atomicOperation,
              OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger() * 1024);
          ((OClusterBasedStorageConfiguration) configuration)
              .setMaxKeySize(atomicOperation, OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger());

          // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
          // INDEXING
          doAddCluster(atomicOperation, OMetadataDefault.CLUSTER_INDEX_NAME, null);

          // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
          // INDEXING
          doAddCluster(atomicOperation, OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null);

          // ADD THE DEFAULT CLUSTER
          defaultClusterId = doAddCluster(atomicOperation, CLUSTER_DEFAULT_NAME, null);

          postCreateSteps();

          //binary compatibility with previous version, this record contained configuration of storage
          doCreateRecord(atomicOperation, new ORecordId(0, -1), new byte[] { 0, 0, 0, 0 }, 0, OBlob.RECORD_TYPE, null,
              doGetAndCheckCluster(0), null);
          initTxProfiler(contextConfiguration);
        });
      } catch (final InterruptedException e) {
        throw OException.wrapException(new OStorageException("Storage creation was interrupted"), e);
      } catch (final OStorageException e) {
        close();
        throw e;
      } catch (final IOException e) {
        close();
        throw OException.wrapException(new OStorageException("Error on creation of storage '" + name + "'"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    OLogManager.instance()
        .infoNoDb(this, "Storage '%s' is created under OrientDB distribution : %s", getURL(), OConstants.getVersion());

  }

  private void checkPageSizeAndRelatedParameters() {
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
    final int freeListBoundary = OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger() * 1024;
    final int maxKeySize = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();

    if (configuration.getPageSize() != -1 && configuration.getPageSize() != pageSize) {
      throw new OStorageException(
          "Storage is created with value of " + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey() + " parameter equal to "
              + configuration.getPageSize() + " but current value is " + pageSize);
    }

    if (configuration.getFreeListBoundary() != -1 && configuration.getFreeListBoundary() != freeListBoundary) {
      throw new OStorageException(
          "Storage is created with value of " + OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getKey()
              + " parameter equal to " + configuration.getFreeListBoundary() + " but current value is " + freeListBoundary);
    }

    if (configuration.getMaxKeySize() != -1 && configuration.getMaxKeySize() != maxKeySize) {
      throw new OStorageException(
          "Storage is created with value of " + OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getKey() + " parameter equal to "
              + configuration.getMaxKeySize() + " but current value is " + maxKeySize);
    }

  }

  @Override
  public final boolean isClosed() {
    try {
      stateLock.acquireReadLock();
      try {
        return super.isClosed();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final void close(final boolean force, final boolean onDelete) {
    try {
      doClose(force, onDelete);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final void delete() {
    try {
      final long timer = Orient.instance().getProfiler().startChrono();

      stateLock.acquireWriteLock();
      try {
        // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
        close(true, true);
        postDeleteSteps();
      } finally {
        stateLock.releaseWriteLock();
        Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
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

          final OPageDataVerificationError[] pageErrors = writeCache.checkStoredPages(verbose ? listener : null);

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
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final int addCluster(final String clusterName, final Object... parameters) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        makeStorageDirty();

        return atomicOperationsManager
            .calculateInsideAtomicOperation((atomicOperation) -> doAddCluster(atomicOperation, clusterName, parameters));

      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error in creation of new cluster '" + clusterName), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final int addCluster(final String clusterName, final int requestedId, final Object... parameters) {
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

        return atomicOperationsManager.calculateInsideAtomicOperation(
            (atomicOperation) -> addClusterInternal(atomicOperation, clusterName, requestedId, parameters));

      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error in creation of new cluster '" + clusterName + "'"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final boolean dropCluster(final int clusterId, final boolean iTruncate) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {

        checkOpenness();
        checkClusterId(clusterId);

        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          return false;
        }

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
          if (iTruncate) {
            cluster.truncate(atomicOperation);
          }
          cluster.delete(atomicOperation);

          clusterMap.remove(cluster.getName().toLowerCase(configuration.getLocaleInstance()));
          clusters.set(clusterId, null);

          // UPDATE CONFIGURATION
          ((OClusterBasedStorageConfiguration) configuration).dropCluster(atomicOperation, clusterId);
        });

        return true;
      } catch (final Exception e) {
        throw OException.wrapException(new OStorageException("Error while removing cluster '" + clusterId + "'"), e);

      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void checkClusterId(int clusterId) {
    if (clusterId < 0 || clusterId >= clusters.size()) {
      throw new OStorageException(
          "Cluster id '" + clusterId + "' is outside the of range of configured clusters (0-" + (clusters.size() - 1)
              + ") in database '" + name + "'");
    }
  }

  @Override
  public String getClusterNameById(int clusterId) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getName();
      } finally {
        stateLock.releaseReadLock();
      }

    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long getClusterRecordsSizeById(int clusterId) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getRecordsSize();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long getClusterRecordsSizeByName(String clusterName) {
    Objects.requireNonNull(clusterName);

    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (cluster == null) {
          throwClusterDoesNotExist(clusterName);
        }

        return cluster.getRecordsSize();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public boolean isSystemCluster(int clusterId) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.isSystemCluster();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long getLastClusterPosition(int clusterId) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getLastPosition();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void setClusterAttribute(int clusterId, OCluster.ATTRIBUTES attribute, Object value) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        makeStorageDirty();
        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> cluster.set(atomicOperation, attribute, value));
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public Object setClusterAttribute(String clusterName, OCluster.ATTRIBUTES attribute, Object value) {
    Objects.requireNonNull(clusterName);

    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        final OCluster cluster = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (cluster == null) {
          throwClusterDoesNotExist(clusterName);
        }

        makeStorageDirty();
        return atomicOperationsManager
            .calculateInsideAtomicOperation((atomicOperation) -> cluster.set(atomicOperation, attribute, value));
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public String getClusterRecordConflictStrategy(int clusterId) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return Optional.ofNullable(cluster.getRecordConflictStrategy()).
            map(ORecordConflictStrategy::getName).orElse(null);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public String getClusterEncryption(int clusterId) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.encryption();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public void truncateCluster(String clusterName) {
    Objects.requireNonNull(clusterName);

    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        final OCluster cluster = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (cluster == null) {
          throwClusterDoesNotExist(clusterName);
        }

        makeStorageDirty();
        atomicOperationsManager.executeInsideAtomicOperation(cluster::truncate);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long countRecords() {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        int totalRecords = 0;
        for (final OCluster cluster : clusters) {
          if (cluster != null) {
            totalRecords += cluster.getEntries();
          }
        }

        return totalRecords;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long getClusterNextPosition(int clusterId) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getNextPosition();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPaginatedCluster.RECORD_STATUS getRecordStatus(ORID rid) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final int clusterId = rid.getClusterId();
        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return ((OPaginatedCluster) cluster).getRecordStatus(rid.getClusterPosition());
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void throwClusterDoesNotExist(int clusterId) {
    throw new OStorageException("Cluster with id " + clusterId + " does not exist inside of storage " + name);
  }

  private void throwClusterDoesNotExist(String clusterName) {
    throw new OStorageException("Cluster with name `" + clusterName + "` does not exist inside of storage " + name);
  }

  @Override
  public final int getId() {
    return id;
  }

  /**
   * INTERNAL METHOD !!!
   */
  public final boolean setClusterStatus(final OAtomicOperation atomicOperation, final OCluster cluster,
      final OStorageClusterConfiguration.STATUS status) throws IOException {
    if (status == OStorageClusterConfiguration.STATUS.OFFLINE && cluster instanceof OOfflineCluster
        || status == OStorageClusterConfiguration.STATUS.ONLINE && !(cluster instanceof OOfflineCluster)) {
      return false;
    }

    // UPDATE CONFIGURATION
    makeStorageDirty();

    final OCluster newCluster;
    if (status == OStorageClusterConfiguration.STATUS.OFFLINE) {
      cluster.close(true);
      newCluster = new OOfflineCluster(this, cluster.getId(), cluster.getName());

      boolean configured = false;
      for (final OStorageClusterConfiguration clusterConfiguration : configuration.getClusters()) {
        if (clusterConfiguration.getId() == cluster.getId()) {
          newCluster.configure(this, clusterConfiguration);
          configured = true;
          break;
        }
      }

      if (!configured) {
        throw new OStorageException("Can not configure offline cluster with id " + cluster.getId());
      }
    } else {

      newCluster = OPaginatedClusterFactory
          .createCluster(cluster.getName(), configuration.getVersion(), cluster.getBinaryVersion(), this);
      newCluster.configure(this, cluster.getId(), cluster.getName());
      newCluster.open();
    }

    clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), newCluster);
    clusters.set(cluster.getId(), newCluster);

    ((OClusterBasedStorageConfiguration) configuration).setClusterStatus(atomicOperation, cluster.getId(), status);

    return true;
  }

  @Override
  public final OSBTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public OReadCache getReadCache() {
    return readCache;
  }

  public OWriteCache getWriteCache() {
    return writeCache;
  }

  @Override
  public final long count(final int iClusterId) {
    return count(iClusterId, false);
  }

  @Override
  public final long count(final int clusterId, final boolean countTombstones) {
    try {
      if (clusterId == -1) {
        throw new OStorageException("Cluster Id " + clusterId + " is invalid in database '" + name + "'");
      }

      // COUNT PHYSICAL CLUSTER IF ANY
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          return 0;
        }

        if (countTombstones) {
          return cluster.getEntries();
        }

        return cluster.getEntries() - cluster.getTombstonesCount();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final long[] getClusterDataRange(final int iClusterId) {
    try {
      if (iClusterId == -1) {
        return new long[] { ORID.CLUSTER_POS_INVALID, ORID.CLUSTER_POS_INVALID };
      }

      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return clusters.get(iClusterId) != null ?
            new long[] { clusters.get(iClusterId).getFirstPosition(), clusters.get(iClusterId).getLastPosition() } :
            OCommonConst.EMPTY_LONG_ARRAY;

      } catch (final IOException ioe) {
        throw OException.wrapException(new OStorageException("Cannot retrieve information about data range"), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public OLogSequenceNumber getLSN() {
    try {
      if (writeAheadLog == null) {
        return null;
      }

      return writeAheadLog.end();
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public final void onException(final Throwable e) {
    dataFlushException = e;
  }

  /**
   * This method finds all the records which were updated starting from (but not including) current LSN and write result in provided
   * output stream. In output stream will be included all thw records which were updated/deleted/created since passed in LSN till
   * the current moment. Deleted records are written in output stream first, then created/updated records. All records are sorted by
   * record id. Data format: <ol> <li>Amount of records (single entry) - 8 bytes</li> <li>Record's cluster id - 4 bytes</li>
   * <li>Record's cluster position - 8 bytes</li> <li>Delete flag, 1 if record is deleted - 1 byte</li> <li>Record version , only
   * if record is not deleted - 4 bytes</li> <li>Record type, only if record is not deleted - 1 byte</li> <li>Length of binary
   * presentation of record, only if record is not deleted - 4 bytes</li> <li>Binary presentation of the record, only if record is
   * not deleted - length of content is provided in above entity</li> </ol>
   *
   * @param lsn LSN from which we should find changed records
   * @return Last LSN processed during examination of changed records, or <code>null</code> if it was impossible to find changed
   * records: write ahead log is absent, record with start LSN was not found in WAL, etc.
   * @see OGlobalConfiguration#STORAGE_TRACK_CHANGED_RECORDS_IN_WAL
   */
  public OBackgroundDelta recordsChangedAfterLSN(final OLogSequenceNumber lsn, final OCommandOutputListener outputListener) {
    final OLogSequenceNumber endLsn;
    // container of rids of changed records
    final SortedSet<ORID> sortedRids = new TreeSet<>();
    try {
      if (!configuration.getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL)) {
        throw new IllegalStateException(
            "Cannot find records which were changed starting from provided LSN because tracking of rids of changed records in WAL is switched off, "
                + "to switch it on please set property " + OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getKey()
                + " to the true value, please note that only records"
                + " which are stored after this property was set will be retrieved");
      }

      stateLock.acquireReadLock();
      try {
        if (writeAheadLog == null) {
          return null;
        }

        // we iterate till the last record is contained in wal at the moment when we call this method
        endLsn = writeAheadLog.end();

        if (endLsn == null || lsn.compareTo(endLsn) > 0) {
          OLogManager.instance()
              .warn(this, "Cannot find requested LSN=%s for database sync operation. Last available LSN is %s", lsn, endLsn);
          return null;
        }

        if (lsn.equals(endLsn)) {
          // nothing has changed
          return new OBackgroundDelta(endLsn);
        }

        List<OWriteableWALRecord> records = writeAheadLog.next(lsn, 1);
        if (records.isEmpty()) {
          OLogManager.instance()
              .info(this, "Cannot find requested LSN=%s for database sync operation (last available LSN is %s)", lsn, endLsn);
          return null;
        }

        final OLogSequenceNumber freezeLsn = records.get(0).getLsn();

        writeAheadLog.addCutTillLimit(freezeLsn);
        try {
          records = writeAheadLog.next(lsn, 1_000);
          if (records.isEmpty()) {
            OLogManager.instance()
                .info(this, "Cannot find requested LSN=%s for database sync operation (last available LSN is %s)", lsn, endLsn);
            return null;
          }

          // all information about changed records is contained in atomic operation metadata
          long read = 0;

          readLoop:
          while (!records.isEmpty()) {
            for (final OWALRecord record : records) {
              final OLogSequenceNumber recordLSN = record.getLsn();

              if (endLsn.compareTo(recordLSN) >= 0) {
                if (record instanceof OFileCreatedWALRecord) {
                  throw new ODatabaseException("Cannot execute delta-sync because a new file has been added. Filename: '"
                      + ((OFileCreatedWALRecord<?>) record).getFileName() + "' (id=" + ((OFileCreatedWALRecord<?>) record)
                      .getFileId() + ")");
                }

                if (record instanceof OFileDeletedWALRecord) {
                  throw new ODatabaseException(
                      "Cannot execute delta-sync because a file has been deleted. File id: " + ((OFileDeletedWALRecord<?>) record)
                          .getFileId());
                }

                if (record instanceof OAtomicUnitEndRecord) {
                  final OAtomicUnitEndRecord<?> atomicUnitEndRecord = (OAtomicUnitEndRecord<?>) record;
                  if (atomicUnitEndRecord.getAtomicOperationMetadata().containsKey(ORecordOperationMetadata.RID_METADATA_KEY)) {
                    final ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) atomicUnitEndRecord
                        .getAtomicOperationMetadata().get(ORecordOperationMetadata.RID_METADATA_KEY);
                    final Set<ORID> rids = recordOperationMetadata.getValue();
                    sortedRids.addAll(rids);
                  }
                }

                read++;

                if (outputListener != null) {
                  outputListener.onMessage("read " + read + " records from WAL and collected " + sortedRids.size() + " records");
                }
              } else {
                break readLoop;
              }
            }

            records = writeAheadLog.next(records.get(records.size() - 1).getLsn(), 1_000);
          }
        } finally {
          writeAheadLog.removeCutTillLimit(freezeLsn);
        }

      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error of reading of records changed after LSN " + lsn), e);
      } finally {
        stateLock.releaseReadLock();
      }

      return new OBackgroundDelta(this, outputListener, sortedRids, lsn, endLsn);
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public Optional<OBackgroundNewDelta> extractTransactionsFromWal(List<OTransactionId> transactionsMetadata) {
    List<OTransactionData> finished = new ArrayList<>();
    stateLock.acquireReadLock();
    try {
      Set<OTransactionId> transactionsToRead = new HashSet<>(transactionsMetadata);
      // we iterate till the last record is contained in wal at the moment when we call this method
      OLogSequenceNumber beginLsn = writeAheadLog.end();
      Map<Long, OTransactionData> units = new HashMap<>();

      writeAheadLog.addCutTillLimit(beginLsn);
      OLogSequenceNumber lsn = beginLsn;
      try {
        List<OWriteableWALRecord> records = writeAheadLog.next(lsn, 1_000);
        // all information about changed records is contained in atomic operation metadata
        while (!records.isEmpty()) {
          for (final OWALRecord record : records) {

            if (record instanceof OFileCreatedWALRecord) {
              throw new ODatabaseException(
                  "Cannot execute delta-sync because a new file has been added. Filename: '" + ((OFileCreatedWALRecord) record)
                      .getFileName() + "' (id=" + ((OFileCreatedWALRecord) record).getFileId() + ")");
            }

            if (record instanceof OFileDeletedWALRecord) {
              throw new ODatabaseException(
                  "Cannot execute delta-sync because a file has been deleted. File id: " + ((OFileDeletedWALRecord) record)
                      .getFileId());
            }

            if (record instanceof OAtomicUnitStartMetadataRecord) {
              byte[] meta = ((OAtomicUnitStartMetadataRecord) record).getMetadata();
              OTxMetadataHolder data = OTxMetadataHolderImpl.read(meta);
              //TODO: This will not be a byte to byte compare, but should compare only the tx id not all status
              if (transactionsToRead.contains(data.getId())) {
                long unitId = ((OAtomicUnitStartMetadataRecord) record).getOperationUnitId();
                units.put(unitId, new OTransactionData(data.getId()));
              }
              transactionsToRead.remove(data.getId());
            }
            if (record instanceof OAtomicUnitEndRecordV2) {
              long opId = ((OAtomicUnitEndRecordV2) record).getOperationUnitId();
              OTransactionData opes = units.remove(opId);
              finished.add(opes);
            }
            if (record instanceof OHighLevelTransactionChangeRecord) {
              byte[] data = ((OHighLevelTransactionChangeRecord) record).getData();
              long unitId = ((OHighLevelTransactionChangeRecord) record).getOperationUnitId();
              OTransactionData tx = units.get(unitId);
              tx.addRecord(data);
            }
            if (transactionsToRead.isEmpty() && units.isEmpty()) {
              //all read stop scanning and return the transactions
              return Optional.of(new OBackgroundNewDelta(finished));
            }
          }

          records = writeAheadLog.next(records.get(records.size() - 1).getLsn(), 1_000);
        }
      } finally {
        writeAheadLog.removeCutTillLimit(beginLsn);
      }
      if (transactionsToRead.isEmpty()) {
        return Optional.of(new OBackgroundNewDelta(finished));
      } else {
        return Optional.empty();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error of reading of records from  WAL"), e);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  protected void serializeDeltaContent(OutputStream stream, OCommandOutputListener outputListener, SortedSet<ORID> sortedRids,
      OLogSequenceNumber lsn) {
    try {
      stateLock.acquireReadLock();
      final int totalRecords = sortedRids.size();
      OLogManager.instance().info(this, "Exporting records after LSN=%s. Found %d records", lsn, totalRecords);
      // records may be deleted after we flag them as existing and as result rule of sorting of records
      // (deleted records go first will be broken), so we prohibit any modifications till we do not complete method execution
      final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
      try {
        try (final DataOutputStream dataOutputStream = new DataOutputStream(stream)) {

          dataOutputStream.writeLong(sortedRids.size());

          long exportedRecord = 1;

          for (ORID rid : sortedRids) {
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
              dataOutputStream.writeByte(rawBuffer.recordType);
              dataOutputStream.writeInt(rawBuffer.buffer.length);
              dataOutputStream.write(rawBuffer.buffer);

              OLogManager.instance()
                  .debug(this, "Exporting modified record rid=%s type=%d size=%d v=%d - buffer size=%d", rid, rawBuffer.recordType,
                      rawBuffer.buffer.length, rawBuffer.version, dataOutputStream.size());
            }

            if (outputListener != null) {
              outputListener.onMessage("exporting record " + exportedRecord + "/" + totalRecords);
            }

            exportedRecord++;
          }
        }
      } finally {
        atomicOperationsManager.releaseAtomicOperations(lockId);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      stateLock.releaseReadLock();
    }

  }

  /**
   * This method finds all the records changed in the last X transactions.
   *
   * @param maxEntries Maximum number of entries to check back from last log.
   * @return A set of record ids of the changed records
   * @see OGlobalConfiguration#STORAGE_TRACK_CHANGED_RECORDS_IN_WAL
   */
  public Set<ORecordId> recordsChangedRecently(final int maxEntries) {
    final SortedSet<ORecordId> result = new TreeSet<>();

    try {
      if (!OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getValueAsBoolean()) {
        throw new IllegalStateException(
            "Cannot find records which were changed starting from provided LSN because tracking of rids of changed records in WAL is switched off, "
                + "to switch it on please set property " + OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getKey()
                + " to the true value, please note that only records"
                + " which are stored after this property was set will be retrieved");
      }

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

          List<OWriteableWALRecord> walRecords = writeAheadLog.read(startLsn, 1_000);
          if (walRecords.isEmpty()) {
            OLogManager.instance()
                .info(this, "Cannot find requested LSN=%s for database sync operation (record in WAL is absent)", startLsn);
            return null;
          }

          // KEEP LAST MAX-ENTRIES TRANSACTIONS' LSN
          final List<OAtomicUnitEndRecord<?>> lastTx = new ArrayList<>(1024);
          readLoop:
          while (!walRecords.isEmpty()) {
            for (final OWriteableWALRecord walRecord : walRecords) {
              final OLogSequenceNumber recordLSN = walRecord.getLsn();
              if (endLsn.compareTo(recordLSN) >= 0) {
                if (walRecord instanceof OAtomicUnitEndRecord<?>) {
                  if (lastTx.size() >= maxEntries) {
                    lastTx.remove(0);
                  }

                  lastTx.add((OAtomicUnitEndRecord<?>) walRecord);
                }
              } else {
                break readLoop;
              }
            }

            walRecords = writeAheadLog.next(walRecords.get(walRecords.size() - 1).getLsn(), 1_000);
          }

          // COLLECT ALL THE MODIFIED RECORDS
          for (final OAtomicUnitEndRecord<?> atomicUnitEndRecord : lastTx) {
            if (atomicUnitEndRecord.getAtomicOperationMetadata().containsKey(ORecordOperationMetadata.RID_METADATA_KEY)) {
              final ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) atomicUnitEndRecord
                  .getAtomicOperationMetadata().get(ORecordOperationMetadata.RID_METADATA_KEY);

              final Set<ORID> rids = recordOperationMetadata.getValue();
              for (final ORID rid : rids) {
                result.add((ORecordId) rid);
              }
            }
          }

          OLogManager.instance().info(this, "Found %d records changed in last %d operations", result.size(), lastTx.size());

          return result;
        } finally {
          writeAheadLog.removeCutTillLimit(freezeLSN);
        }

      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error on reading last changed records"), e);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final long count(final int[] iClusterIds, final boolean countTombstones) {
    try {
      checkOpenness();

      long tot = 0;

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        for (final int iClusterId : iClusterIds) {
          if (iClusterId >= clusters.size()) {
            throw new OConfigurationException("Cluster id " + iClusterId + " was not found in database '" + name + "'");
          }

          if (iClusterId > -1) {
            final OCluster c = clusters.get(iClusterId);
            if (c != null) {
              tot += c.getEntries() - (countTombstones ? 0L : c.getTombstonesCount());
            }
          }
        }

        return tot;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId rid, final byte[] content,
      final int recordVersion, final byte recordType, final int mode, final ORecordCallback<Long> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());

        return atomicOperationsManager.calculateInsideAtomicOperation(
            (atomicOperation) -> doCreateRecord(atomicOperation, rid, content, recordVersion, recordType, callback, cluster, null));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final ORecordMetadata getRecordMetadata(final ORID rid) {
    try {
      if (rid.isNew()) {
        throw new OStorageException("Passed record with id " + rid + " is new and cannot be stored.");
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
        checkOpenness();

        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
        if (ppos == null) {
          return null;
        }

        return new ORecordMetadata(rid, ppos.recordVersion);
      } catch (final IOException ioe) {
        OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        stateLock.releaseReadLock();
      }

      return null;
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public boolean isDeleted(final ORID rid) {
    try {
      if (rid.isNew()) {
        throw new OStorageException("Passed record with id " + rid + " is new and cannot be stored.");
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
        checkOpenness();

        return cluster.isDeleted(new OPhysicalPosition(rid.getClusterPosition()));

      } catch (final IOException ioe) {
        OLogManager.instance().error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        stateLock.releaseReadLock();
      }

      return false;
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public Iterator<OClusterBrowsePage> browseCluster(final int clusterId) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final int finalClusterId;

        if (clusterId == ORID.CLUSTER_ID_INVALID) {
          // GET THE DEFAULT CLUSTER
          finalClusterId = defaultClusterId;
        } else {
          finalClusterId = clusterId;
        }
        return new Iterator<OClusterBrowsePage>() {
          private OClusterBrowsePage page;
          private long lastPos = -1;

          @Override
          public boolean hasNext() {
            if (page == null) {
              page = nextPage(finalClusterId, lastPos);
              if (page != null) {
                lastPos = page.getLastPosition();
              }
            }
            return page != null;
          }

          @Override
          public OClusterBrowsePage next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            final OClusterBrowsePage curPage = page;
            page = null;
            return curPage;
          }
        };
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OClusterBrowsePage nextPage(final int clusterId, final long lastPosition) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(clusterId);
        return cluster.nextPage(lastPosition);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OCluster doGetAndCheckCluster(final int clusterId) {
    checkClusterSegmentIndexRange(clusterId);

    final OCluster cluster = clusters.get(clusterId);
    if (cluster == null) {
      throw new IllegalArgumentException("Cluster " + clusterId + " is null");
    }
    return cluster;
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      final boolean prefetchRecords, final ORecordCallback<ORawBuffer> iCallback) {
    try {
      checkOpenness();
      final OCluster cluster;
      try {
        cluster = doGetAndCheckCluster(iRid.getClusterId());
      } catch (final IllegalArgumentException e) {
        throw OException.wrapException(new ORecordNotFoundException(iRid), e);
      }

      return new OStorageOperationResult<>(readRecord(cluster, iRid, prefetchRecords));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    try {
      checkOpenness();
      return new OStorageOperationResult<>(readRecordIfNotLatest(doGetAndCheckCluster(rid.getClusterId()), rid, recordVersion));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final OStorageOperationResult<Integer> updateRecord(final ORecordId rid, final boolean updateContent, final byte[] content,
      final int version, final byte recordType, @SuppressWarnings("unused") final int mode,
      final ORecordCallback<Integer> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpenness();
          final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());

          return atomicOperationsManager.calculateInsideAtomicOperation(
              (atomicOperation) -> doUpdateRecord(atomicOperation, rid, updateContent, content, version, recordType, callback,
                  cluster));
        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OStorageOperationResult<Integer> recyclePosition(final ORecordId rid, final byte[] content, final int version,
      final byte recordType) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpenness();

          final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
          return atomicOperationsManager.calculateInsideAtomicOperation(
              (atomicOperation) -> doRecycleRecord(atomicOperation, rid, content, version, cluster, recordType));

        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public OStorageTransaction getStorageTransaction() {
    return transaction.get();
  }

  public final OAtomicOperationsManager getAtomicOperationsManager() {
    return atomicOperationsManager;
  }

  public OWriteAheadLog getWALInstance() {
    return writeAheadLog;
  }

  public AtomicOperationIdGen getIdGen() {
    return idGen;
  }

  @Override
  public final OStorageOperationResult<Boolean> deleteRecord(final ORecordId rid, final int version, final int mode,
      final ORecordCallback<Boolean> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
        return atomicOperationsManager
            .calculateInsideAtomicOperation((atomicOperation) -> doDeleteRecord(atomicOperation, rid, version, cluster));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OStorageOperationResult<Boolean> hideRecord(final ORecordId rid, final int mode,
      final ORecordCallback<Boolean> callback) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      stateLock.acquireReadLock();
      try {
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpenness();

          final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
          return atomicOperationsManager
              .calculateInsideAtomicOperation((atomicOperation) -> doHideMethod(atomicOperation, rid, cluster));
        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  protected OPerformanceStatisticManager getPerformanceStatisticManager() {
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
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
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
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final Set<String> getClusterNames() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return new HashSet<>(clusterMap.keySet());
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final int getClusterIdByName(final String clusterName) {
    try {
      checkOpenness();

      if (clusterName == null) {
        throw new IllegalArgumentException("Cluster name is null");
      }

      if (clusterName.length() == 0) {
        throw new IllegalArgumentException("Cluster name is empty");
      }

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        // SEARCH IT BETWEEN PHYSICAL CLUSTERS

        final OCluster segment = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (segment != null) {
          return segment.getId();
        }

        return -1;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
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

      final Iterable<ORecordOperation> entries = clientTx.getRecordOperations();
      final TreeMap<Integer, OCluster> clustersToLock = new TreeMap<>();

      final Set<ORecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

      for (final ORecordOperation txEntry : entries) {

        if (txEntry.type == ORecordOperation.CREATED) {
          newRecords.add(txEntry);
          final int clusterId = txEntry.getRID().getClusterId();
          clustersToLock.put(clusterId, doGetAndCheckCluster(clusterId));
        }
      }
      stateLock.acquireReadLock();
      try {

        checkOpenness();

        makeStorageDirty();
        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
          lockClusters(clustersToLock);

          for (final ORecordOperation txEntry : newRecords) {
            final ORecord rec = txEntry.getRecord();
            if (!rec.getIdentity().isPersistent()) {
              if (rec.isDirty()) {
                //This allocate a position for a new record
                final ORecordId rid = (ORecordId) rec.getIdentity().copy();
                final ORecordId oldRID = rid.copy();
                final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
                final OPhysicalPosition ppos = cluster.allocatePosition(atomicOperation, ORecordInternal.getRecordType(rec));
                rid.setClusterPosition(ppos.clusterPosition);
                clientTx.updateIdentityAfterCommit(oldRID, rid);
              }
            } else {
              //This allocate position starting from a valid rid, used in distributed for allocate the same position on other nodes
              final ORecordId rid = (ORecordId) rec.getIdentity();
              final OPaginatedCluster cluster = (OPaginatedCluster) doGetAndCheckCluster(rid.getClusterId());
              OPaginatedCluster.RECORD_STATUS recordStatus = cluster.getRecordStatus(rid.getClusterPosition());
              if (recordStatus == OPaginatedCluster.RECORD_STATUS.NOT_EXISTENT) {
                OPhysicalPosition ppos = cluster.allocatePosition(atomicOperation, ORecordInternal.getRecordType(rec));
                while (ppos.clusterPosition < rid.getClusterPosition()) {
                  ppos = cluster.allocatePosition(atomicOperation, ORecordInternal.getRecordType(rec));
                }
                if (ppos.clusterPosition != rid.getClusterPosition()) {
                  throw new OConcurrentCreateException(rid, new ORecordId(rid.getClusterId(), ppos.clusterPosition));
                }
              } else if (recordStatus == OPaginatedCluster.RECORD_STATUS.PRESENT
                  || recordStatus == OPaginatedCluster.RECORD_STATUS.REMOVED) {
                final OPhysicalPosition ppos = cluster.allocatePosition(atomicOperation, ORecordInternal.getRecordType(rec));
                throw new OConcurrentCreateException(rid, new ORecordId(rid.getClusterId(), ppos.clusterPosition));
              }

            }
          }
        });
      } catch (final IOException | RuntimeException ioe) {
        throw OException.wrapException(new OStorageException("Could not preallocate RIDs"), ioe);
      } finally {
        stateLock.releaseReadLock();
      }

    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Traditional commit that support already temporary rid and already assigned rids
   *
   * @param clientTx the transaction to commit
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
   * @return The list of operations applied by the transaction
   */
  private List<ORecordOperation> commit(final OTransactionInternal transaction, final boolean allocated) {
    // XXX: At this moment, there are two implementations of the commit method. One for regular client transactions and one for
    // implicit micro-transactions. The implementations are quite identical, but operate on slightly different data. If you change
    // this method don't forget to change its counterpart:
    //
    //  OAbstractPaginatedStorage.commit(com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction)

    final AtomicBoolean txIsCompleted = new AtomicBoolean();
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      txBegun.incrementAndGet();

      if (txProfilerTrigger > 0) {
        final TransactionProfiler transactionProfiler = new TransactionProfiler(txProfilerTrigger, txIsCompleted,
            Thread.currentThread(), txProfilerStackTraceMapThreshold);

        transactionProfiler.future = storageProfilerExecutor
            .scheduleWithFixedDelay(transactionProfiler, txProfilerTrigger, txProfilerInterval, TimeUnit.MILLISECONDS);
      }

      final ODatabaseDocumentInternal database = transaction.getDatabase();
      final OIndexManager indexManager = database.getMetadata().getIndexManager();
      final TreeMap<String, OTransactionIndexChanges> indexOperations = getSortedIndexOperations(transaction);

      database.getMetadata().makeThreadLocalSchemaSnapshot();

      final Collection<ORecordOperation> recordOperations = transaction.getRecordOperations();
      final TreeMap<Integer, OCluster> clustersToLock = new TreeMap<>();
      final Map<ORecordOperation, Integer> clusterOverrides = new IdentityHashMap<>(8);

      final Set<ORecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

      for (final ORecordOperation recordOperation : recordOperations) {
        if (recordOperation.type == ORecordOperation.CREATED || recordOperation.type == ORecordOperation.UPDATED) {
          final ORecord record = recordOperation.getRecord();
          if (record instanceof ODocument) {
            ((ODocument) record).validate();
          }
        }

        if (recordOperation.type == ORecordOperation.UPDATED || recordOperation.type == ORecordOperation.DELETED) {
          final int clusterId = recordOperation.getRecord().getIdentity().getClusterId();
          clustersToLock.put(clusterId, doGetAndCheckCluster(clusterId));
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

          clustersToLock.put(clusterId, doGetAndCheckCluster(clusterId));
        }
      }

      final List<ORecordOperation> result = new ArrayList<>(8);
      stateLock.acquireReadLock();
      try {
        if (pessimisticLock) {
          final List<ORID> recordLocks = new ArrayList<>(8);
          for (final ORecordOperation recordOperation : recordOperations) {
            if (recordOperation.type == ORecordOperation.UPDATED || recordOperation.type == ORecordOperation.DELETED) {
              recordLocks.add(recordOperation.getRID());
            }
          }
          Collections.sort(recordLocks);
          for (final ORID rid : recordLocks) {
            acquireWriteLock(rid);
          }
        }
        try {
          checkOpenness();

          makeStorageDirty();

          List<OIndexInternal<?>> lockedIndexes = null;
          boolean rollback = false;
          final OAtomicOperation atomicOperation = startStorageTx(transaction);
          try {
            lockClusters(clustersToLock);

            checkReadOnlyConditions();

            final Map<ORecordOperation, OPhysicalPosition> positions = new IdentityHashMap<>(8);
            for (final ORecordOperation recordOperation : newRecords) {
              final ORecord rec = recordOperation.getRecord();

              if (allocated) {
                if (rec.getIdentity().isPersistent()) {
                  positions.put(recordOperation, new OPhysicalPosition(rec.getIdentity().getClusterPosition()));
                } else {
                  throw new OStorageException("Impossible to commit a transaction with not valid rid in pre-allocated commit");
                }
              } else if (rec.isDirty() && !rec.getIdentity().isPersistent()) {
                final ORecordId rid = (ORecordId) rec.getIdentity().copy();
                final ORecordId oldRID = rid.copy();

                final Integer clusterOverride = clusterOverrides.get(recordOperation);
                final int clusterId = Optional.ofNullable(clusterOverride).orElseGet(rid::getClusterId);

                final OCluster cluster = doGetAndCheckCluster(clusterId);

                OPhysicalPosition physicalPosition = cluster.allocatePosition(atomicOperation, ORecordInternal.getRecordType(rec));
                rid.setClusterId(cluster.getId());

                if (rid.getClusterPosition() > -1) {
                  // CREATE EMPTY RECORDS UNTIL THE POSITION IS REACHED. THIS IS THE CASE WHEN A SERVER IS OUT OF SYNC
                  // BECAUSE A TRANSACTION HAS BEEN ROLLED BACK BEFORE TO SEND THE REMOTE CREATES. SO THE OWNER NODE DELETED
                  // RECORD HAVING A HIGHER CLUSTER POSITION
                  while (rid.getClusterPosition() > physicalPosition.clusterPosition) {
                    physicalPosition = cluster.allocatePosition(atomicOperation, ORecordInternal.getRecordType(rec));
                  }

                  if (rid.getClusterPosition() != physicalPosition.clusterPosition) {
                    throw new OConcurrentCreateException(rid, new ORecordId(rid.getClusterId(), physicalPosition.clusterPosition));
                  }
                }
                positions.put(recordOperation, physicalPosition);

                rid.setClusterPosition(physicalPosition.clusterPosition);

                transaction.updateIdentityAfterCommit(oldRID, rid);
              }
            }

            lockRidBags(clustersToLock, indexOperations, indexManager);

            checkReadOnlyConditions();

            for (final ORecordOperation recordOperation : recordOperations) {
              commitEntry(atomicOperation, recordOperation, positions.get(recordOperation), database.getSerializer());
              result.add(recordOperation);
            }

            lockIndexes(indexOperations);

            checkReadOnlyConditions();

            commitIndexes(indexOperations);
          } catch (final IOException | RuntimeException e) {
            rollback = true;
            if (e instanceof RuntimeException) {
              throw ((RuntimeException) e);
            } else {
              throw OException.wrapException(new OStorageException("Error during transaction commit"), e);
            }
          } finally {
            if (rollback) {
              rollback(transaction);
            } else {
              endStorageTx(transaction, recordOperations);
            }

            this.transaction.set(null);
          }
        } finally {
          atomicOperationsManager.ensureThatComponentsUnlocked();
          database.getMetadata().clearThreadLocalSchemaSnapshot();
        }
      } finally {
        try {
          if (pessimisticLock) {
            for (final ORecordOperation recordOperation : recordOperations) {
              if (recordOperation.type == ORecordOperation.UPDATED || recordOperation.type == ORecordOperation.DELETED) {
                releaseWriteLock(recordOperation.getRID());
              }
            }
          }
        } finally {
          stateLock.releaseReadLock();
        }
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance()
            .debug(this, "%d Committed transaction %d on database '%s' (result=%s)", Thread.currentThread().getId(),
                transaction.getId(), database.getName(), result);
      }

      return result;
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      handleJVMError(ee);
      OAtomicOperationsManager.alarmClearOfAtomicOperation();
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      txIsCompleted.set(true);
    }
  }

  private void commitIndexes(final Map<String, OTransactionIndexChanges> indexesToCommit) {
    for (final OTransactionIndexChanges changes : indexesToCommit.values()) {
      final OIndexInternal<?> index = changes.getAssociatedIndex();
      if (!index.isNativeTxSupported()) {
        final OIndexAbstract.IndexTxSnapshot snapshot = new OIndexAbstract.IndexTxSnapshot();
        index.addTxOperation(snapshot, changes);
        index.commit(snapshot);
      } else {
        try {
          final int indexId = index.getIndexId();
          if (changes.cleared) {
            clearIndex(indexId);
          }

          for (final OTransactionIndexChangesPerKey changesPerKey : changes.changesPerKey.values()) {
            applyTxChanges(changesPerKey, index);
          }
          applyTxChanges(changes.nullKeyChanges, index);
        } catch (OInvalidIndexEngineIdException e) {
          throw OException.wrapException(new OStorageException("Error during index commit"), e);
        }

      }
    }

  }

  private void applyTxChanges(OTransactionIndexChangesPerKey changes, OIndexInternal<?> index)
      throws OInvalidIndexEngineIdException {

    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry op : index.interpretTxKeyChanges(changes)) {
      switch (op.operation) {
      case PUT:
        index.doPut(this, changes.key, op.value.getIdentity());
        break;
      case REMOVE:
        if (op.value != null)
          index.doRemove(this, changes.key, op.value.getIdentity());
        else
          index.doRemove(this, changes.key);
        break;
      case CLEAR:
        // SHOULD NEVER BE THE CASE HANDLE BY cleared FLAG
        break;
      }
    }
  }

  public int loadIndexEngine(final String name) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OBaseIndexEngine engine = indexEngineNameMap.get(name);
        if (engine == null) {
          return -1;
        }

        final int indexId = indexEngines.indexOf(engine);
        assert indexId >= 0;

        return generateIndexId(indexId, engine);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public int loadExternalIndexEngine(final String engineName, final String algorithm, final String indexType,
      final OIndexDefinition indexDefinition, final OBinarySerializer<?> valueSerializer, final boolean isAutomatic,
      final Boolean durableInNonTxMode, final int version, final int apiVersion, final boolean multivalue,
      final Map<String, String> engineProperties) {
    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        // this method introduced for binary compatibility only
        if (configuration.getBinaryFormatVersion() > 15) {
          return -1;
        }

        if (indexEngineNameMap.containsKey(engineName)) {
          throw new OIndexException("Index with name " + engineName + " already exists");
        }

        makeStorageDirty();

        final OBinarySerializer<?> keySerializer = determineKeySerializer(indexDefinition);
        final int keySize = determineKeySize(indexDefinition);
        final OType[] keyTypes = Optional.ofNullable(indexDefinition).map(OIndexDefinition::getTypes).orElse(null);
        final boolean nullValuesSupport = indexDefinition != null && !indexDefinition.isNullValuesIgnored();

        final OBaseIndexEngine engine = OIndexes
            .createIndexEngine(engineName, algorithm, indexType, durableInNonTxMode, this, version, apiVersion, multivalue,
                engineProperties, null);

        final OStorageConfiguration.IndexEngineData engineData = new OStorageConfigurationImpl.IndexEngineData(engineName,
            algorithm, indexType, durableInNonTxMode, version, engine.getEngineAPIVersion(), multivalue, valueSerializer.getId(),
            keySerializer.getId(), isAutomatic, keyTypes, nullValuesSupport, keySize, null, null, engineProperties);

        if (engineData.getApiVersion() < 1) {
          ((OIndexEngine) engine)
              .load(engineName, valueSerializer, isAutomatic, keySerializer, keyTypes, nullValuesSupport, keySize,
                  engineData.getEngineProperties(), null);
        } else {
          ((OV1IndexEngine) engine).load(engineName, keySize, keyTypes, keySerializer, null);
        }

        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
          ((OClusterBasedStorageConfiguration) configuration).addIndexEngine(atomicOperation, engineName, engineData);
          indexEngineNameMap.put(engineName, engine);
          indexEngines.add(engine);
        });

        return generateIndexId(indexEngines.size() - 1, engine);
      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public int addIndexEngine(final String engineName, final String algorithm, final String indexType,
      final OIndexDefinition indexDefinition, final OBinarySerializer<?> valueSerializer, final boolean isAutomatic,
      final Boolean durableInNonTxMode, final int version, final int apiVersion, final boolean multivalue,
      final Map<String, String> engineProperties, final Set<String> clustersToIndex, final ODocument metadata) {
    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        makeStorageDirty();

        OBaseIndexEngine engine = atomicOperationsManager.calculateInsideAtomicOperation((atomicOperation) -> {
          if (indexEngineNameMap.containsKey(engineName)) {
            // OLD INDEX FILE ARE PRESENT: THIS IS THE CASE OF PARTIAL/BROKEN INDEX
            OLogManager.instance()
                .warn(this, "Index with name '%s' already exists, removing it and re-create the index", engineName);
            final OBaseIndexEngine indexEngine = indexEngineNameMap.remove(engineName);
            if (indexEngine != null) {
              indexEngines.remove(indexEngine);
              ((OClusterBasedStorageConfiguration) configuration).deleteIndexEngine(atomicOperation, engineName);
              indexEngine.delete(atomicOperation);
            }
          }

          final OBinarySerializer<?> keySerializer = determineKeySerializer(indexDefinition);
          final int keySize = determineKeySize(indexDefinition);
          final OType[] keyTypes = Optional.ofNullable(indexDefinition).map(OIndexDefinition::getTypes).orElse(null);
          final boolean nullValuesSupport = indexDefinition != null && !indexDefinition.isNullValuesIgnored();
          final byte serializerId;

          if (valueSerializer != null) {
            serializerId = valueSerializer.getId();
          } else {
            serializerId = -1;
          }

          final OBaseIndexEngine indexEngine = OIndexes
              .createIndexEngine(engineName, algorithm, indexType, durableInNonTxMode, this, version, apiVersion, multivalue,
                  engineProperties, metadata);

          final OContextConfiguration ctxCfg = configuration.getContextConfiguration();
          final String cfgEncryption = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
          final String cfgEncryptionKey = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

          final OEncryption encryption;
          if (cfgEncryption == null || cfgEncryption.equals(ONothingEncryption.NAME)) {
            encryption = null;
          } else {
            encryption = OEncryptionFactory.INSTANCE.getEncryption(cfgEncryption, cfgEncryptionKey);
          }

          indexEngine.create(atomicOperation, valueSerializer, isAutomatic, keyTypes, nullValuesSupport, keySerializer, keySize,
              clustersToIndex, engineProperties, metadata, encryption);

          indexEngineNameMap.put(engineName, indexEngine);

          indexEngines.add(indexEngine);

          final OStorageConfiguration.IndexEngineData engineData = new OStorageConfigurationImpl.IndexEngineData(engineName,
              algorithm, indexType, durableInNonTxMode, version, indexEngine.getEngineAPIVersion(), multivalue, serializerId,
              keySerializer.getId(), isAutomatic, keyTypes, nullValuesSupport, keySize, cfgEncryption, cfgEncryptionKey,
              engineProperties);

          ((OClusterBasedStorageConfiguration) configuration).addIndexEngine(atomicOperation, engineName, engineData);
          return indexEngine;
        });

        return generateIndexId(indexEngines.size() - 1, engine);
      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private static int generateIndexId(final int internalId, final OBaseIndexEngine indexEngine) {
    return indexEngine.getEngineAPIVersion() << (OIntegerSerializer.INT_SIZE * 8 - 5) | internalId;
  }

  private static int extractInternalId(final int externalId) {
    if (externalId < 0) {
      throw new IllegalStateException("Index id has to be positive");
    }

    return externalId & 0x7_FF_FF_FF;
  }

  public static int extractEngineAPIVersion(final int externalId) {
    return externalId >>> (OIntegerSerializer.INT_SIZE * 8 - 5);
  }

  private static int determineKeySize(final OIndexDefinition indexDefinition) {
    if (indexDefinition == null || indexDefinition instanceof ORuntimeKeyIndexDefinition) {
      return 1;
    } else {
      return indexDefinition.getTypes().length;
    }
  }

  private OBinarySerializer<?> determineKeySerializer(final OIndexDefinition indexDefinition) {
    final OBinarySerializer<?> keySerializer;
    if (indexDefinition != null) {
      if (indexDefinition instanceof ORuntimeKeyIndexDefinition<?>) {
        keySerializer = ((ORuntimeKeyIndexDefinition<?>) indexDefinition).getSerializer();
      } else {
        if (indexDefinition.getTypes().length > 1) {
          keySerializer = OCompositeKeySerializer.INSTANCE;
        } else {
          final OType keyType = indexDefinition.getTypes()[0];

          if (keyType == OType.STRING && configuration.getBinaryFormatVersion() >= 13) {
            return OUTF8Serializer.INSTANCE;
          }

          final OCurrentStorageComponentsFactory currentStorageComponentsFactory = componentsFactory;
          if (currentStorageComponentsFactory != null) {
            keySerializer = currentStorageComponentsFactory.binarySerializerFactory.getObjectSerializer(keyType);
          } else {
            throw new IllegalStateException("Cannot load binary serializer, storage is not properly initialized");
          }
        }
      }
    } else {
      //noinspection rawtypes
      keySerializer = new OSimpleKeySerializer();
    }
    return keySerializer;
  }

  public void deleteIndexEngine(int indexId) throws OInvalidIndexEngineIdException {
    final int internalId = extractInternalId(indexId);

    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        checkIndexId(internalId);

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
          final OBaseIndexEngine engine = indexEngines.get(internalId);

          indexEngines.set(internalId, null);

          engine.delete(atomicOperation);

          final String engineName = engine.getName();
          indexEngineNameMap.remove(engineName);
          ((OClusterBasedStorageConfiguration) configuration).deleteIndexEngine(atomicOperation, engineName);
        });
      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error on index deletion"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void checkIndexId(final int indexId) throws OInvalidIndexEngineIdException {
    if (indexId < 0 || indexId >= indexEngines.size() || indexEngines.get(indexId) == null) {
      throw new OInvalidIndexEngineIdException("Engine with id " + indexId + " is not registered inside of storage");
    }
  }

  public boolean indexContainsKey(int indexId, final Object key) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIndexContainsKey(indexId, key);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return doIndexContainsKey(indexId, key);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doIndexContainsKey(final int indexId, final Object key) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.contains(key);
  }

  public boolean removeKeyFromIndex(int indexId, final Object key) throws OInvalidIndexEngineIdException {
    final int internalId = extractInternalId(indexId);
    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);
        return doRemoveKeyFromIndex(atomicOperation, internalId, key);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        return atomicOperationsManager
            .calculateInsideAtomicOperation((atomicOperation) -> doRemoveKeyFromIndex(atomicOperation, internalId, key));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doRemoveKeyFromIndex(OAtomicOperation atomicOperation, final int indexId, final Object key)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      makeStorageDirty();
      final OBaseIndexEngine engine = indexEngines.get(indexId);
      return engine.remove(atomicOperation, key);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during removal of entry with key " + key + " from index "), e);
    }
  }

  public void clearIndex(int indexId) throws OInvalidIndexEngineIdException {
    final int internalId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);

        doClearIndex(atomicOperation, internalId);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> doClearIndex(atomicOperation, internalId));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doClearIndex(OAtomicOperation atomicOperation, final int indexId) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);

      makeStorageDirty();
      engine.clear(atomicOperation);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during clearing of index"), e);
    }

  }

  public Object getIndexValue(int indexId, final Object key) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexValue(indexId, key);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexValue(indexId, key);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private Object doGetIndexValue(final int indexId, final Object key) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    return engine.get(key);
  }

  public OBaseIndexEngine getIndexEngine(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      checkIndexId(indexId);
      return indexEngines.get(indexId);
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void updateIndexEntry(int indexId, final Object key, final OIndexKeyUpdater<Object> valueCreator)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalId = extractInternalId(indexId);

    if (engineAPIVersion != 0) {
      throw new IllegalStateException("Unsupported version of index engine API. Required 0 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);
        doUpdateIndexEntry(atomicOperation, internalId, key, valueCreator);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        atomicOperationsManager
            .executeInsideAtomicOperation((atomicOperation) -> doUpdateIndexEntry(atomicOperation, internalId, key, valueCreator));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public <T> T callIndexEngine(final boolean atomicOperation, final boolean readOperation, int indexId,
      final OIndexEngineCallback<T> callback) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doCallIndexEngine(atomicOperation, readOperation, indexId, callback);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        return doCallIndexEngine(atomicOperation, readOperation, indexId, callback);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private <T> T doCallIndexEngine(final boolean atomicOperation, final boolean readOperation, final int indexId,
      final OIndexEngineCallback<T> callback) throws OInvalidIndexEngineIdException, IOException {
    checkIndexId(indexId);
    boolean rollback = false;
    if (atomicOperation) {
      atomicOperationsManager.startAtomicOperation();
    }

    try {
      if (!readOperation) {
        makeStorageDirty();
      }

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      return callback.callEngine(engine);
    } catch (final Exception e) {
      rollback = true;
      throw OException.wrapException(new OStorageException("Cannot put key value entry in index"), e);
    } finally {
      if (atomicOperation) {
        atomicOperationsManager.endAtomicOperation(rollback);
      }
    }
  }

  private void doUpdateIndexEntry(final OAtomicOperation atomicOperation, final int indexId, final Object key,
      final OIndexKeyUpdater<Object> valueCreator) throws OInvalidIndexEngineIdException, IOException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    makeStorageDirty();

    ((OIndexEngine) engine).update(atomicOperation, key, valueCreator);
  }

  public void putRidIndexEntry(int indexId, final Object key, final ORID value) throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalId = extractInternalId(indexId);

    if (engineAPIVersion != 1) {
      throw new IllegalStateException("Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);
        doPutRidIndexEntry(atomicOperation, internalId, key, value);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        atomicOperationsManager
            .executeInsideAtomicOperation((atomicOperation) -> doPutRidIndexEntry(atomicOperation, internalId, key, value));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doPutRidIndexEntry(final OAtomicOperation atomicOperation, final int indexId, final Object key, final ORID value)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      ((OV1IndexEngine) engine).put(atomicOperation, key, value);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key " + key + " value " + value + " entry to the index"), e);
    }
  }

  public boolean removeRidIndexEntry(int indexId, final Object key, final ORID value) throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalId = extractInternalId(indexId);

    if (engineAPIVersion != 1) {
      throw new IllegalStateException("Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);
        return doRemoveRidIndexEntry(atomicOperation, internalId, key, value);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        return atomicOperationsManager
            .calculateInsideAtomicOperation((atomicOperation) -> doRemoveRidIndexEntry(atomicOperation, internalId, key, value));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doRemoveRidIndexEntry(final OAtomicOperation atomicOperation, final int indexId, final Object key,
      final ORID value) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      return ((OMultiValueIndexEngine) engine).remove(atomicOperation, key, value);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key " + key + " value " + value + " entry to the index"), e);
    }
  }

  public void putIndexValue(int indexId, final Object key, final Object value) throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalId = extractInternalId(indexId);

    if (engineAPIVersion != 0) {
      throw new IllegalStateException("Unsupported version of index engine API. Required 0 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);
        doPutIndexValue(atomicOperation, internalId, key, value);
        return;
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        atomicOperationsManager
            .executeInsideAtomicOperation((atomicOperation) -> doPutIndexValue(atomicOperation, internalId, key, value));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doPutIndexValue(OAtomicOperation atomicOperation, final int indexId, final Object key, final Object value)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      ((OIndexEngine) engine).put(atomicOperation, key, value);
    } catch (final IOException e) {
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
   * @return {@code true} if the validator allowed the put, {@code false} otherwise.
   * @see OBaseIndexEngine.Validator#validate(Object, Object, Object)
   */
  @SuppressWarnings("UnusedReturnValue")
  public boolean validatedPutIndexValue(int indexId, final Object key, final ORID value,
      final OBaseIndexEngine.Validator<Object, ORID> validator) throws OInvalidIndexEngineIdException {
    final int internalId = extractInternalId(indexId);
    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        Objects.requireNonNull(atomicOperation);
        return doValidatedPutIndexValue(atomicOperation, internalId, key, value, validator);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            (atomicOperation) -> doValidatedPutIndexValue(atomicOperation, internalId, key, value, validator));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doValidatedPutIndexValue(OAtomicOperation atomicOperation, final int indexId, final Object key, final ORID value,
      final OBaseIndexEngine.Validator<Object, ORID> validator) throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      makeStorageDirty();

      if (engine instanceof OIndexEngine) {
        return ((OIndexEngine) engine).validatedPut(atomicOperation, key, value, validator);
      }

      if (engine instanceof OSingleValueIndexEngine) {
        return ((OSingleValueIndexEngine) engine).validatedPut(key, atomicOperation, value.getIdentity(), validator);
      }

      throw new IllegalStateException("Invalid type of index engine " + engine.getClass().getName());
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Cannot put key " + key + " value " + value + " entry to the index"), e);
    }
  }

  public Object getIndexFirstKey(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexFirstKey(indexId);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexFirstKey(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private Object doGetIndexFirstKey(final int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.getFirstKey();
  }

  public Object getIndexLastKey(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexFirstKey(indexId);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexLastKey(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private Object doGetIndexLastKey(final int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.getLastKey();
  }

  public OIndexCursor iterateIndexEntriesBetween(int indexId, final Object rangeFrom, final boolean fromInclusive,
      final Object rangeTo, final boolean toInclusive, final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIterateIndexEntriesBetween(indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doIterateIndexEntriesBetween(indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doIterateIndexEntriesBetween(final int indexId, final Object rangeFrom, final boolean fromInclusive,
      final Object rangeTo, final boolean toInclusive, final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateIndexEntriesMajor(int indexId, final Object fromKey, final boolean isInclusive,
      final boolean ascSortOrder, final OBaseIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doIterateIndexEntriesMajor(final int indexId, final Object fromKey, final boolean isInclusive,
      final boolean ascSortOrder, final OBaseIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateIndexEntriesMinor(int indexId, final Object toKey, final boolean isInclusive,
      final boolean ascSortOrder, final OBaseIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doIterateIndexEntriesMinor(final int indexId, final Object toKey, final boolean isInclusive,
      final boolean ascSortOrder, final OBaseIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor getIndexCursor(int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexCursor(indexId, valuesTransformer);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexCursor(indexId, valuesTransformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doGetIndexCursor(final int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.cursor(valuesTransformer);
  }

  public OIndexCursor getIndexDescCursor(int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexDescCursor(indexId, valuesTransformer);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexDescCursor(indexId, valuesTransformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexCursor doGetIndexDescCursor(final int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.descCursor(valuesTransformer);
  }

  public OIndexKeyCursor getIndexKeyCursor(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexKeyCursor(indexId);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexKeyCursor(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OIndexKeyCursor doGetIndexKeyCursor(final int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.keyCursor();
  }

  public long getIndexSize(int indexId, final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexSize(indexId, transformer);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doGetIndexSize(indexId, transformer);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private long doGetIndexSize(final int indexId, final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.size(transformer);
  }

  public boolean hasIndexRangeQuerySupport(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doHasRangeQuerySupport(indexId);
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return doHasRangeQuerySupport(indexId);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final OInvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doHasRangeQuerySupport(final int indexId) throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return engine.hasRangeQuerySupport();
  }

  @Override
  public void rollback(final OTransactionInternal clientTx) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        try {
          checkOpenness();

          assert transaction.get() != null;

          if (transaction.get().getClientTx().getId() != clientTx.getId()) {
            throw new OStorageException(
                "Passed in and active transaction are different transactions. Passed in transaction cannot be rolled back.");
          }

          makeStorageDirty();
          rollbackStorageTx();

          OTransactionAbstract.updateCacheFromEntries(clientTx.getDatabase(), clientTx.getRecordOperations(), false);

          txRollback.incrementAndGet();

        } catch (final IOException e) {
          throw OException.wrapException(new OStorageException("Error during transaction rollback"), e);
        } finally {
          transaction.set(null);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Rollbacks the given micro-transaction.
   *
   * @param microTransaction the micro-transaction to rollback.
   */
  public void rollback(final OMicroTransaction microTransaction) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        try {
          checkOpenness();

          if (transaction.get() == null) {
            return;
          }

          if (transaction.get().getMicroTransaction().getId() != microTransaction.getId()) {
            throw new OStorageException(
                "Passed in and active micro-transaction are different micro-transactions. Passed in micro-transaction cannot be "
                    + "rolled back.");
          }

          makeStorageDirty();
          rollbackStorageTx();

          microTransaction.updateRecordCacheAfterRollback();

          txRollback.incrementAndGet();

        } catch (final IOException e) {
          throw OException.wrapException(new OStorageException("Error during micro-transaction rollback"), e);
        } finally {
          transaction.set(null);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    try {
      return ppos != null && !ORecordVersionHelper.isTombstone(ppos.recordVersion);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final void synch() {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        final long timer = Orient.instance().getProfiler().startChrono();
        final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
        try {
          checkOpenness();
          if (jvmError.get() == null) {
            for (final OBaseIndexEngine indexEngine : indexEngines) {
              try {
                if (indexEngine != null) {
                  indexEngine.flush();
                }
              } catch (final Throwable t) {
                OLogManager.instance().error(this, "Error while flushing index via index engine of class %s.", t,
                    indexEngine.getClass().getSimpleName());
              }
            }

            if (writeAheadLog != null) {
              makeFullCheckpoint();
              return;
            }

            writeCache.flush();

            clearStorageDirty();
          } else {
            OLogManager.instance().errorNoDb(this, "Sync can not be performed because of JVM error on storage", null);
          }

        } catch (final IOException e) {
          throw OException.wrapException(new OStorageException("Error on synch storage '" + name + "'"), e);

        } finally {
          atomicOperationsManager.releaseAtomicOperations(lockId);
          Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final String getPhysicalClusterNameById(final int iClusterId) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (iClusterId < 0 || iClusterId >= clusters.size()) {
          return null;
        }

        return clusters.get(iClusterId) != null ? clusters.get(iClusterId).getName() : null;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final int getDefaultClusterId() {
    return defaultClusterId;
  }

  @Override
  public final void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  @Override
  public final long getSize() {
    try {
      try {
        long size = 0;

        stateLock.acquireReadLock();
        try {
          for (final OCluster c : clusters) {
            if (c != null) {
              size += c.getRecordsSize();
            }
          }
        } finally {
          stateLock.releaseReadLock();
        }

        return size;
      } catch (final IOException ioe) {
        throw OException.wrapException(new OStorageException("Cannot calculate records size"), ioe);
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final int getClusters() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return clusterMap.size();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Method that completes the cluster rename operation. <strong>IT WILL NOT RENAME A CLUSTER, IT JUST CHANGES THE NAME IN THE
   * INTERNAL MAPPING</strong>
   */
  public final void renameCluster(final String oldName, final String newName) {
    try {
      clusterMap.put(newName.toLowerCase(configuration.getLocaleInstance()),
          clusterMap.remove(oldName.toLowerCase(configuration.getLocaleInstance())));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final boolean cleanOutRecord(final ORecordId recordId, final int recordVersion, final int iMode,
      final ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, iMode, callback).getResult();
  }

  @Override
  public final void freeze(final boolean throwException) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (throwException) {
          atomicOperationsManager
              .freezeAtomicOperations(OModificationOperationProhibitedException.class, "Modification requests are prohibited");
        } else {
          atomicOperationsManager.freezeAtomicOperations(null, null);
        }

        final List<OFreezableStorageComponent> frozenIndexes = new ArrayList<>(indexEngines.size());
        try {
          for (final OBaseIndexEngine indexEngine : indexEngines) {
            if (indexEngine instanceof OFreezableStorageComponent) {
              ((OFreezableStorageComponent) indexEngine).freeze(false);
              frozenIndexes.add((OFreezableStorageComponent) indexEngine);
            }
          }
        } catch (final Exception e) {
          // RELEASE ALL THE FROZEN INDEXES
          for (final OFreezableStorageComponent indexEngine : frozenIndexes) {
            indexEngine.release();
          }

          throw OException.wrapException(new OStorageException("Error on freeze of storage '" + name + "'"), e);
        }

        synch();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final void release() {
    try {
      for (final OBaseIndexEngine indexEngine : indexEngines) {
        if (indexEngine instanceof OFreezableStorageComponent) {
          ((OFreezableStorageComponent) indexEngine).release();
        }
      }

      atomicOperationsManager.releaseAtomicOperations(-1);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final boolean isRemote() {
    return false;
  }

  public boolean wereDataRestoredAfterOpen() {
    return wereDataRestoredAfterOpen;
  }

  public boolean wereNonTxOperationsPerformedInPreviousOpen() {
    return wereNonTxOperationsPerformedInPreviousOpen;
  }

  @Override
  public final void reload() {
    try {
      close();
      open(null, null, null);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @SuppressWarnings("unused")
  public String getMode() {
    return mode;
  }

  @Override
  public final void lowDiskSpace(final OLowDiskSpaceInformation information) {
    lowDiskSpace = information;
  }

  /**
   * @inheritDoc
   */
  @Override
  public final void pageIsBroken(final String fileName, final long pageIndex) {
    brokenPages.add(new OPair<>(fileName, pageIndex));
  }

  @Override
  public final void requestCheckpoint() {
    try {
      if (!walVacuumInProgress.get() && walVacuumInProgress.compareAndSet(false, true)) {
        fuzzyCheckpointExecutor.submit(new WALVacuum());
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Executes the command request and return the result back.
   */
  @Override
  public final Object command(final OCommandRequestText iCommand) {
    try {
      while (true) {
        try {
          final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

          // COPY THE CONTEXT FROM THE REQUEST
          executor.setContext(iCommand.getContext());

          executor.setProgressListener(iCommand.getProgressListener());
          executor.parse(iCommand);

          return executeCommand(iCommand, executor);
        } catch (final ORetryQueryException ignore) {

          if (iCommand instanceof OQueryAbstract) {
            final OQueryAbstract<?> query = (OQueryAbstract<?>) iCommand;
            query.reset();
          }

        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @SuppressWarnings("WeakerAccess")
  public final Object executeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor) {
    try {
      if (iCommand.isIdempotent() && !executor.isIdempotent()) {
        throw new OCommandExecutionException("Cannot execute non idempotent command");
      }

      final long beginTime = Orient.instance().getProfiler().startChrono();

      try {

        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();

        // CALL BEFORE COMMAND
        final Iterable<ODatabaseListener> listeners = db.getListeners();
        for (final ODatabaseListener oDatabaseListener : listeners) {
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
              if (result instanceof Collection<?>) {
                for (final Object o : (Collection<?>) result) {
                  iCommand.getResultListener().result(o);
                }
              } else {
                iCommand.getResultListener().result(result);
              }

              // RESET THE RESULT TO AVOID TO SEND IT TWICE
              result = null;
            }
          }
        }

        if (!foundInCache) {
          // EXECUTE THE COMMAND
          final Map<Object, Object> params = iCommand.getParameters();
          result = executor.execute(params);

          if (result != null && iCommand.isCacheableResult() && executor.isCacheable() && (iCommand.getParameters() == null
              || iCommand.getParameters().isEmpty()))
          // CACHE THE COMMAND RESULT
          {
            db.getMetadata().getCommandCache()
                .put(db.getUser(), iCommand.getText(), result, iCommand.getLimit(), executor.getInvolvedClusters(),
                    System.currentTimeMillis() - beginTime);
          }
        }

        // CALL AFTER COMMAND
        for (final ODatabaseListener oDatabaseListener : listeners) {
          oDatabaseListener.onAfterCommand(iCommand, executor, result);
        }

        return result;

      } catch (final OException e) {
        // PASS THROUGH
        throw e;
      } catch (final Exception e) {
        throw OException.wrapException(new OCommandExecutionException("Error on execution of command: " + iCommand), e);

      } finally {
        if (Orient.instance().getProfiler().isRecording()) {
          final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
          if (db != null) {
            final OSecurityUser user = db.getUser();
            final String userString = Optional.ofNullable(user).map(Object::toString).orElse(null);
            Orient.instance().getProfiler()
                .stopChrono("db." + ODatabaseRecordThreadLocal.instance().get().getName() + ".command." + iCommand,
                    "Command executed against the database", beginTime, "db.*.command.*", null, userString);
          }
        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OPhysicalPosition[] higherPhysicalPositions(final int currentClusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1) {
        return new OPhysicalPosition[0];
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(currentClusterId);
        return cluster.higherPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OPhysicalPosition[] ceilingPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1) {
        return new OPhysicalPosition[0];
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(clusterId);
        return cluster.ceilingPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OPhysicalPosition[] lowerPhysicalPositions(final int currentClusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1) {
        return new OPhysicalPosition[0];
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(currentClusterId);

        return cluster.lowerPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OPhysicalPosition[] floorPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1) {
        return new OPhysicalPosition[0];
      }

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = doGetAndCheckCluster(clusterId);

        return cluster.floorPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final void acquireWriteLock(final ORID rid) {
    try {
      lockManager.acquireLock(rid, OComparableLockManager.LOCK.EXCLUSIVE, RECORD_LOCK_TIMEOUT);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final void releaseWriteLock(final ORID rid) {
    try {
      lockManager.releaseLock(this, rid, OComparableLockManager.LOCK.EXCLUSIVE);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final void acquireReadLock(final ORID rid) {
    try {
      lockManager.acquireLock(rid, OComparableLockManager.LOCK.SHARED, RECORD_LOCK_TIMEOUT);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final void releaseReadLock(final ORID rid) {
    try {
      lockManager.releaseLock(this, rid, OComparableLockManager.LOCK.SHARED);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final ORecordConflictStrategy getClusterRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  @Override
  public final void setConflictStrategy(final ORecordConflictStrategy conflictResolver) {
    Objects.requireNonNull(conflictResolver);
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      atomicOperationsManager
          .executeInsideAtomicOperation((atomicOperation) -> doSetConflictStrategy(conflictResolver, atomicOperation));
    } catch (final Exception e) {
      throw OException.wrapException(new OStorageException(
          "Exception during setting of conflict strategy " + conflictResolver.getName() + " for storage " + name), e);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  private void doSetConflictStrategy(ORecordConflictStrategy conflictResolver, OAtomicOperation atomicOperation)
      throws IOException {

    if (recordConflictStrategy == null || !recordConflictStrategy.getName().equals(conflictResolver.getName())) {
      makeStorageDirty();

      this.recordConflictStrategy = conflictResolver;
      ((OClusterBasedStorageConfiguration) configuration).setConflictStrategy(atomicOperation, conflictResolver.getName());
    }
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
    if (this.recoverListener == recoverListener) {
      this.recoverListener = null;
    }
  }

  @SuppressWarnings("unused")
  protected abstract File createWalTempDirectory();

  @SuppressWarnings("unused")
  protected abstract void addFileToDirectory(String name, InputStream stream, File directory) throws IOException;

  @SuppressWarnings("unused")
  protected abstract OWriteAheadLog createWalFromIBUFiles(File directory, final OContextConfiguration contextConfiguration,
      final Locale locale) throws IOException;

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  protected final void checkOpenness() {
    if (status != STATUS.OPEN) {
      throw new OStorageException("Storage " + name + " is not opened.");
    }
  }

  protected final void makeFuzzyCheckpoint() {
    if (writeAheadLog == null) {
      return;
    }

    //check every 1 ms.
    while (!stateLock.tryAcquireReadLock(1_000_000)) {
      if (status != STATUS.OPEN) {
        return;
      }
    }

    try {
      if (status != STATUS.OPEN || writeAheadLog == null) {
        return;
      }

      OLogSequenceNumber beginLSN = writeAheadLog.begin();
      OLogSequenceNumber endLSN = writeAheadLog.end();

      final Long minLSNSegment = writeCache.getMinimalNotFlushedSegment();

      long fuzzySegment;

      if (minLSNSegment != null) {
        fuzzySegment = minLSNSegment;
      } else {
        if (endLSN == null) {
          return;
        }

        fuzzySegment = endLSN.getSegment();
      }

      atomicOperationsTable.compactTable();
      final long minAtomicOperationSegment = atomicOperationsTable.getSegmentEarliestNotPersistedOperation();
      if (minAtomicOperationSegment >= 0 && fuzzySegment > minAtomicOperationSegment) {
        fuzzySegment = minAtomicOperationSegment;
      }

      OLogManager.instance().debugNoDb(this,
          "Before fuzzy checkpoint: min LSN segment is " + minLSNSegment + ", WAL begin is " + beginLSN + ", WAL end is " + endLSN
              + ", fuzzy segment is " + fuzzySegment, null);

      if (fuzzySegment > beginLSN.getSegment() && beginLSN.getSegment() < endLSN.getSegment()) {
        OLogManager.instance().debugNoDb(this, "Making fuzzy checkpoint", null);
        writeCache.makeFuzzyCheckpoint(fuzzySegment, lastMetadata);

        beginLSN = writeAheadLog.begin();
        endLSN = writeAheadLog.end();

        OLogManager.instance().debugNoDb(this, "After fuzzy checkpoint: WAL begin is " + beginLSN + " WAL end is " + endLSN, null);
      } else {
        OLogManager.instance().debugNoDb(this, "No reason to make fuzzy checkpoint", null);
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(new OIOException("Error during fuzzy checkpoint"), ioe);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void tryToDeleteTreeRidBag(final OSBTreeRidBag ridBag) {
    try {
      checkOpenness();
      checkLowDiskSpaceRequestsAndReadOnlyConditions();

      if (transaction.get() == null) {
        stateLock.acquireWriteLock();
        try {
          checkOpenness();

          doTryToDeleteTreeRidBag(ridBag);
        } finally {
          stateLock.releaseWriteLock();
        }
      } else {
        doTryToDeleteTreeRidBag(ridBag);
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doTryToDeleteTreeRidBag(OSBTreeRidBag ridBag) {
    final long delay = configuration.getContextConfiguration().getValueAsInteger(RID_BAG_SBTREEBONSAI_DELETE_DALAY);
    final long schedule = delay / 3;

    final OBonsaiCollectionPointer collectionPointer = ridBag.getCollectionPointer();
    final Runnable deleteTask = new Runnable() {
      @Override
      public void run() {
        checkOpenness();
        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        stateLock.acquireWriteLock();
        try {
          if (status == STATUS.OPEN) {
            try {
              makeStorageDirty();
              final OAtomicOperationsManager atomicOperationsManager = OAbstractPaginatedStorage.this.atomicOperationsManager;

              atomicOperationsManager.executeInsideAtomicOperation((operation) -> {
                if (!sbTreeCollectionManager.tryDelete(operation, collectionPointer, delay)) {
                  Orient.instance().scheduleTask(this, schedule, 0);
                }
              });
            } catch (final Exception e) {
              OLogManager.instance().errorNoDb(this, "Error during deletion of rid bag", e);
            }
          }
        } finally {
          stateLock.releaseWriteLock();
        }
      }
    };

    Orient.instance().scheduleTask(deleteTask, schedule, 0);
    ridBag.confirmDelete();
  }

  protected void makeFullCheckpoint() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    if (statistic != null) {
      statistic.startFullCheckpointTimer();
    }
    try {
      if (writeAheadLog == null) {
        return;
      }

      try {

        writeAheadLog.flush();

        //so we will be able to cut almost all the log
        writeAheadLog.appendNewSegment();

        final OLogSequenceNumber lastLSN = writeAheadLog.logFullCheckpointStart(lastMetadata);
        writeCache.flush();

        atomicOperationsTable.compactTable();
        final long operationSegment = atomicOperationsTable.getSegmentEarliestOperationInProgress();
        if (operationSegment >= 0) {
          throw new IllegalStateException("Can not perform full checkpoint if some of atomic operations in progress");
        }

        writeAheadLog.logFullCheckpointEnd();
        writeAheadLog.flush();
        writeAheadLog.cutTill(lastLSN);

        if (jvmError.get() == null) {
          clearStorageDirty();
        }

      } catch (final IOException ioe) {
        throw OException.wrapException(new OStorageException("Error during checkpoint creation for storage " + name), ioe);
      }

      fullCheckpointCount.increment();
    } finally {
      if (statistic != null) {
        statistic.stopFullCheckpointTimer();
      }
    }
  }

  public long getFullCheckpointCount() {
    return fullCheckpointCount.sum();
  }

  protected long checkIfStorageDirty() throws IOException {
    return -1;
  }

  protected void initConfiguration(OAtomicOperation atomicOperation, final OContextConfiguration contextConfiguration)
      throws IOException {
  }

  @SuppressWarnings({ "WeakerAccess", "EmptyMethod" })
  protected final void postCreateSteps() {
  }

  protected void preCreateSteps() throws IOException {
  }

  protected abstract void initWalAndDiskCache(OContextConfiguration contextConfiguration) throws IOException, InterruptedException;

  protected abstract void postCloseSteps(@SuppressWarnings("unused") boolean onDelete, boolean jvmError, long lastTxId)
      throws IOException;

  @SuppressWarnings("EmptyMethod")
  protected void postCloseStepsAfterLock(final Map<String, Object> params) {
  }

  @SuppressWarnings({ "EmptyMethod" })
  protected Map<String, Object> preCloseSteps() {
    return new HashMap<>(2);
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

    if (!rid.isPersistent()) {
      throw new ORecordNotFoundException(rid,
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');
    }

    if (transaction.get() != null) {
      return doReadRecordIfNotLatest(cluster, rid, recordVersion);
    }

    stateLock.acquireReadLock();
    try {
      if (pessimisticLock) {
        acquireReadLock(rid);
      }

      final ORawBuffer buff;
      checkOpenness();

      buff = doReadRecordIfNotLatest(cluster, rid, recordVersion);
      return buff;
    } finally {
      try {
        if (pessimisticLock) {
          releaseReadLock(rid);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    }
  }

  private ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid, final boolean prefetchRecords) {
    checkOpenness();

    if (!rid.isPersistent()) {
      throw new ORecordNotFoundException(rid,
          "Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');
    }

    if (transaction.get() != null) {
      // Disabled this assert have no meaning anymore
      // assert iLockingStrategy.equals(LOCKING_STRATEGY.DEFAULT);
      return doReadRecord(clusterSegment, rid, prefetchRecords);
    }

    stateLock.acquireReadLock();
    try {
      if (pessimisticLock) {
        acquireReadLock(rid);
      }
      checkOpenness();
      return doReadRecord(clusterSegment, rid, prefetchRecords);
    } finally {
      try {
        if (pessimisticLock) {
          releaseReadLock(rid);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    }
  }

  private void endStorageTx(final OTransactionInternal txi, final Collection<ORecordOperation> recordOperations)
      throws IOException {
    atomicOperationsManager.endAtomicOperation(false);
    assert OAtomicOperationsManager.getCurrentOperation() == null;

    OTransactionAbstract.updateCacheFromEntries(txi.getDatabase(), recordOperations, true);
    txCommit.incrementAndGet();
  }

  private OAtomicOperation startStorageTx(final OTransactionInternal clientTx) throws IOException {
    final OStorageTransaction storageTx = transaction.get();
    assert storageTx == null || storageTx.getClientTx().getId() == clientTx.getId();
    assert OAtomicOperationsManager.getCurrentOperation() == null;
    transaction.set(new OStorageTransaction(clientTx));
    try {
      OAtomicOperation operation = atomicOperationsManager.startAtomicOperation(clientTx.getMetadata());
      if (clientTx.getMetadata().isPresent()) {
        this.lastMetadata = clientTx.getMetadata();
      }
      clientTx.storageBegun();
      Iterator<byte[]> ops = clientTx.getSerializedOperations();
      while (ops.hasNext()) {
        byte[] next = ops.next();
        writeAheadLog.log(new OHighLevelTransactionChangeRecord(operation.getOperationUnitId(), next));
      }
      return operation;
    } catch (final RuntimeException e) {
      transaction.set(null);
      throw e;
    }
  }

  private void rollbackStorageTx() throws IOException {
    assert transaction.get() != null;
    atomicOperationsManager.endAtomicOperation(true);

    assert OAtomicOperationsManager.getCurrentOperation() == null;
  }

  private void recoverIfNeeded() throws Exception {
    if (isDirty()) {
      OLogManager.instance().warn(this, "Storage '" + name + "' was not closed properly. Will try to recover from write ahead log");
      try {
        wereDataRestoredAfterOpen = restoreFromWAL() != null;

        if (recoverListener != null) {
          recoverListener.onStorageRecover();
        }

        makeFullCheckpoint();
      } catch (final Exception e) {
        OLogManager.instance().error(this, "Exception during storage data restore", e);
        throw e;
      }

      OLogManager.instance().info(this, "Storage data recover was completed");
    }
  }

  private OStorageOperationResult<OPhysicalPosition> doCreateRecord(final OAtomicOperation atomicOperation, final ORecordId rid,
      final byte[] content, int recordVersion, final byte recordType, final ORecordCallback<Long> callback, final OCluster cluster,
      final OPhysicalPosition allocated) {
    if (content == null) {
      throw new IllegalArgumentException("Record is null");
    }

    try {
      if (recordVersion > -1) {
        recordVersion++;
      } else {
        recordVersion = 0;
      }

      makeStorageDirty();

      OPhysicalPosition ppos;
      try {
        ppos = cluster.createRecord(atomicOperation, content, recordVersion, recordType, allocated);
        rid.setClusterPosition(ppos.clusterPosition);

        final ORecordSerializationContext context = ORecordSerializationContext.getContext();
        if (context != null) {
          context.executeOperations(atomicOperation, this);
        }
      } catch (final IOException e) {
        OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, e);
        throw ODatabaseException.wrapException(new OStorageException("Error during creation of record"), e);
      }

      if (callback != null) {
        callback.call(rid, ppos.clusterPosition);
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance().debug(this, "Created record %s v.%s size=%d bytes", rid, recordVersion, content.length);
      }

      recordCreated.incrementAndGet();

      return new OStorageOperationResult<>(ppos);
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OStorageException("Error during record deletion in cluster " + (cluster != null ? cluster.getName() : "")), ioe);
    }
  }

  private OStorageOperationResult<Integer> doUpdateRecord(OAtomicOperation atomicOperation, final ORecordId rid,
      final boolean updateContent, byte[] content, final int version, final byte recordType,
      final ORecordCallback<Integer> callback, final OCluster cluster) {

    Orient.instance().getProfiler().startChrono();
    try {

      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
      if (!checkForRecordValidity(ppos)) {
        final int recordVersion = -1;
        if (callback != null) {
          callback.call(rid, recordVersion);
        }

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
      if (updateContent) {
        cluster.updateRecord(atomicOperation, rid.getClusterPosition(), content, ppos.recordVersion, recordType);
      }

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }

      //if we do not update content of the record we should keep version of the record the same
      //otherwise we would have issues when two records may have the same version but different content
      final int newRecordVersion;
      if (updateContent) {
        newRecordVersion = ppos.recordVersion;
      } else {
        newRecordVersion = version;
      }

      if (callback != null) {
        callback.call(rid, newRecordVersion);
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance().debug(this, "Updated record %s v.%s size=%d", rid, newRecordVersion, content.length);
      }

      recordUpdated.incrementAndGet();

      if (contentModified) {
        return new OStorageOperationResult<>(newRecordVersion, content, false);
      } else {
        return new OStorageOperationResult<>(newRecordVersion);
      }
    } catch (final OConcurrentModificationException e) {
      recordConflict.incrementAndGet();
      throw e;
    } catch (final IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Error on updating record " + rid + " (cluster: " + cluster.getName() + ")"), ioe);
    }

  }

  private OStorageOperationResult<Integer> doRecycleRecord(final OAtomicOperation atomicOperation, final ORecordId rid,
      final byte[] content, final int version, final OCluster cluster, final byte recordType) {

    try {
      makeStorageDirty();

      cluster.recycleRecord(atomicOperation, rid.getClusterPosition(), content, version, recordType);

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance().debug(this, "Recycled record %s v.%s size=%d", rid, version, content.length);
      }

      return new OStorageOperationResult<>(version, content, false);

    } catch (final IOException ioe) {
      OLogManager.instance().error(this, "Error on recycling record " + rid + " (cluster: " + cluster + ")", ioe);

      throw OException
          .wrapException(new OStorageException("Error on recycling record " + rid + " (cluster: " + cluster + ")"), ioe);
    }
  }

  private OStorageOperationResult<Boolean> doDeleteRecord(OAtomicOperation atomicOperation, final ORecordId rid, final int version,
      final OCluster cluster) {
    Orient.instance().getProfiler().startChrono();
    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));

      if (ppos == null) {
        // ALREADY DELETED
        return new OStorageOperationResult<>(false);
      }

      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (version > -1 && ppos.recordVersion != version) {
        recordConflict.incrementAndGet();

        if (OFastConcurrentModificationException.enabled()) {
          throw OFastConcurrentModificationException.instance();
        } else {
          throw new OConcurrentModificationException(rid, ppos.recordVersion, version, ORecordOperation.DELETED);
        }
      }

      makeStorageDirty();
      cluster.deleteRecord(atomicOperation, ppos.clusterPosition);

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance().debug(this, "Deleted record %s v.%s", rid, version);
      }

      recordDeleted.incrementAndGet();

      return new OStorageOperationResult<>(true);
    } catch (final IOException ioe) {
      throw OException
          .wrapException(new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster.getName() + ")"), ioe);
    }
  }

  private OStorageOperationResult<Boolean> doHideMethod(OAtomicOperation atomicOperation, final ORecordId rid,
      final OCluster cluster) {
    try {
      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));

      if (ppos == null) {
        // ALREADY HIDDEN
        return new OStorageOperationResult<>(false);
      }

      makeStorageDirty();

      cluster.hideRecord(atomicOperation, ppos.clusterPosition);

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }

      return new OStorageOperationResult<>(true);
    } catch (final IOException ioe) {
      OLogManager.instance().error(this, "Error on deleting record " + rid + "( cluster: " + cluster + ")", ioe);
      throw OException.wrapException(new OStorageException("Error on deleting record " + rid + "( cluster: " + cluster + ")"), ioe);
    }
  }

  private ORawBuffer doReadRecord(final OCluster clusterSegment, final ORecordId rid, final boolean prefetchRecords) {
    try {

      final ORawBuffer buff = clusterSegment.readRecord(rid.getClusterPosition(), prefetchRecords);

      if (buff != null && OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance()
            .debug(this, "Read record %s v.%s size=%d bytes", rid, buff.version, buff.buffer != null ? buff.buffer.length : 0);
      }

      recordRead.incrementAndGet();

      return buff;
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private static ORawBuffer doReadRecordIfNotLatest(final OCluster cluster, final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    try {
      return cluster.readRecordIfVersionIsNotLatest(rid.getClusterPosition(), recordVersion);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration config) throws IOException {
    OCluster cluster = clusterMap.get(config.getName().toLowerCase(configuration.getLocaleInstance()));

    if (cluster != null) {
      cluster.configure(this, config);
      return -1;
    }

    if (config.getStatus() == OStorageClusterConfiguration.STATUS.ONLINE) {
      cluster = OPaginatedClusterFactory
          .createCluster(config.getName(), configuration.getVersion(), config.getBinaryVersion(), this);
    } else {
      cluster = new OOfflineCluster(this, config.getId(), config.getName());
    }

    cluster.configure(this, config);

    return registerCluster(cluster);
  }

  private void setCluster(final int id, final OCluster cluster) {
    if (clusters.size() <= id) {
      while (clusters.size() < id) {
        clusters.add(null);
      }

      clusters.add(cluster);
    } else {
      clusters.set(id, cluster);
    }
  }

  /**
   * Register the cluster internally.
   *
   * @param cluster OCluster implementation
   * @return The id (physical position into the array) of the new cluster just created. First is 0.
   */
  private int registerCluster(final OCluster cluster) {
    final int id;

    if (cluster != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (clusterMap.containsKey(cluster.getName().toLowerCase(configuration.getLocaleInstance()))) {
        throw new OConfigurationException(
            "Cannot add cluster '" + cluster.getName() + "' because it is already registered in database '" + name + "'");
      }
      // CREATE AND ADD THE NEW REF SEGMENT
      clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), cluster);
      id = cluster.getId();
    } else {
      id = clusters.size();
    }

    setCluster(id, cluster);

    return id;
  }

  private int doAddCluster(OAtomicOperation atomicOperation, final String clusterName, final Object[] parameters)
      throws IOException {
    // FIND THE FIRST AVAILABLE CLUSTER ID
    int clusterPos = clusters.size();
    for (int i = 0; i < clusters.size(); ++i) {
      if (clusters.get(i) == null) {
        clusterPos = i;
        break;
      }
    }

    return addClusterInternal(atomicOperation, clusterName, clusterPos, parameters);
  }

  private int addClusterInternal(OAtomicOperation atomicOperation, String clusterName, final int clusterPos,
      final Object... parameters) throws IOException {
    final OPaginatedCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase(configuration.getLocaleInstance());

      cluster = OPaginatedClusterFactory
          .createCluster(clusterName, configuration.getVersion(), OPaginatedCluster.getLatestBinaryVersion(), this);
      cluster.configure(this, clusterPos, clusterName, parameters);
    } else {
      cluster = null;
    }

    int createdClusterId = -1;

    if (cluster != null) {
      cluster.create(atomicOperation, -1);
      createdClusterId = registerCluster(cluster);

      cluster.registerInStorageConfig(atomicOperation, (OClusterBasedStorageConfiguration) configuration);
    }

    return createdClusterId;
  }

  private final void storeLastMetadata() throws IOException {
    if (lastMetadata.isPresent()) {
      final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
      atomicOperationsManager.executeInsideAtomicOperation(
          (atomicOperation -> storageConfiguration.setLastMetadata(atomicOperation, lastMetadata.get())));
    }
  }

  private void doClose(final boolean force, final boolean onDelete) {
    if (!force && !onDelete) {
      decOnClose();
      return;
    }

    if (status == STATUS.CLOSED) {
      return;
    }

    final long timer = Orient.instance().getProfiler().startChrono();
    Map<String, Object> params = new HashMap<>(2);

    stateLock.acquireWriteLock();
    try {
      if (status == STATUS.CLOSED) {
        return;
      }

      status = STATUS.CLOSING;
      storeLastMetadata();

      if (jvmError.get() == null) {
        if (!onDelete && jvmError.get() == null) {
          makeFullCheckpoint();
        }

        params = preCloseSteps();

        if (!onDelete) {
          atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
            // we close all files inside cache system so we only clear index metadata and close non core indexes
            for (final OBaseIndexEngine engine : indexEngines) {
              if (engine != null && !(engine instanceof OSBTreeIndexEngine || engine instanceof OHashTableIndexEngine
                  || engine instanceof OCellBTreeSingleValueIndexEngine || engine instanceof OCellBTreeMultiValueIndexEngine
                  || engine instanceof OAutoShardingIndexEngine)) {
                engine.close();
              }
            }
            ((OClusterBasedStorageConfiguration) configuration).close(atomicOperation);
          });
        } else {
          for (final OBaseIndexEngine engine : indexEngines) {
            if (engine != null && !(engine instanceof OSBTreeIndexEngine || engine instanceof OHashTableIndexEngine
                || engine instanceof OCellBTreeSingleValueIndexEngine || engine instanceof OCellBTreeMultiValueIndexEngine
                || engine instanceof OAutoShardingIndexEngine)) {
              //delete method is implemented only in non native indexes, so they do not use ODB atomic operation
              engine.delete(null);
            }
          }
        }

        sbTreeCollectionManager.close();

        // we close all files inside cache system so we only clear cluster metadata
        clusters.clear();
        clusterMap.clear();
        indexEngines.clear();
        indexEngineNameMap.clear();

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

        if (readCache != null) {
          if (!onDelete) {
            readCache.closeStorage(writeCache);
          } else {
            readCache.deleteStorage(writeCache);
          }
        }

        if (writeAheadLog != null) {
          if (onDelete) {
            writeAheadLog.delete();
          } else {
            writeAheadLog.close();
          }
        }

        try {
          performanceStatisticManager.unregisterMBean(name, id);
        } catch (final Exception e) {
          OLogManager.instance().error(this, "MBean for write cache cannot be unregistered", e);
        }

        postCloseSteps(onDelete, jvmError.get() != null, idGen.getLastId());
        transaction = null;
      } else {
        OLogManager.instance()
            .errorNoDb(this, "Because of JVM error happened inside of storage it can not be properly closed", null);
      }

      status = STATUS.CLOSED;
    } catch (final IOException e) {
      final String message = "Error on closing of storage '" + name;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new OStorageException(message), e);

    } finally {
      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
      stateLock.releaseWriteLock();
    }

    postCloseStepsAfterLock(params);
  }

  @SuppressWarnings("unused")
  protected void closeClusters(final boolean onDelete) throws IOException {
    for (final OCluster cluster : clusters) {
      if (cluster != null) {
        cluster.close(!onDelete);
      }
    }

    clusters.clear();
    clusterMap.clear();
  }

  @SuppressWarnings("unused")
  protected void closeIndexes(final boolean onDelete) throws IOException {
    OAtomicOperation atomicOperation = atomicOperationsManager.startAtomicOperation();
    boolean rollback = false;
    try {
      for (final OBaseIndexEngine engine : indexEngines) {
        if (engine != null) {
          if (onDelete) {
            try {
              engine.delete(atomicOperation);
            } catch (final IOException e) {
              OLogManager.instance().error(this, "Can not delete index engine " + engine.getName(), e);
            }
          } else {
            engine.close();
          }
        }
      }

      indexEngines.clear();
      indexEngineNameMap.clear();
    } catch (RuntimeException e) {
      rollback = true;
      throw e;
    } finally {
      atomicOperationsManager.endAtomicOperation(rollback);
    }
  }

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
      {
        iDatabaseVersion.incrementAndGet();
      }
    }

    return null;
  }

  private void commitEntry(final OAtomicOperation atomicOperation, final ORecordOperation txEntry,
      final OPhysicalPosition allocated, final ORecordSerializer serializer) {

    final ORecord rec = txEntry.getRecord();
    if (txEntry.type != ORecordOperation.DELETED && !rec.isDirty())
    // NO OPERATION
    {
      return;
    }

    final ORecordId rid = (ORecordId) rec.getIdentity();

    if (txEntry.type == ORecordOperation.UPDATED && rid.isNew())
    // OVERWRITE OPERATION AS CREATE
    {
      txEntry.type = ORecordOperation.CREATED;
    }

    ORecordSerializationContext.pushContext();
    try {
      final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());

      if (cluster.getName().equals(OMetadataDefault.CLUSTER_INDEX_NAME) || cluster.getName()
          .equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME))
      // AVOID TO COMMIT INDEX STUFF
      {
        return;
      }

      switch (txEntry.type) {
      case ORecordOperation.LOADED:
        break;

      case ORecordOperation.CREATED: {

        final byte[] stream = serializer.toStream(rec, false);
        if (allocated != null) {
          final OPhysicalPosition ppos;
          final byte recordType = ORecordInternal.getRecordType(rec);
          ppos = doCreateRecord(atomicOperation, rid, stream, rec.getVersion(), recordType, null, cluster, allocated).getResult();

          ORecordInternal.setVersion(rec, ppos.recordVersion);
        } else {
          // USE -2 AS VERSION TO AVOID INCREMENTING THE VERSION
          final OStorageOperationResult<Integer> updateRes = doUpdateRecord(atomicOperation, rid,
              ORecordInternal.isContentChanged(rec), stream, -2, ORecordInternal.getRecordType(rec), null, cluster);
          ORecordInternal.setVersion(rec, updateRes.getResult());
          if (updateRes.getModifiedRecordContent() != null) {
            ORecordInternal.fill(rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);
          }
        }

        break;
      }

      case ORecordOperation.UPDATED: {
        final byte[] stream = serializer.toStream(rec, false);

        final OStorageOperationResult<Integer> updateRes = doUpdateRecord(atomicOperation, rid,
            ORecordInternal.isContentChanged(rec), stream, rec.getVersion(), ORecordInternal.getRecordType(rec), null, cluster);
        ORecordInternal.setVersion(rec, updateRes.getResult());
        if (updateRes.getModifiedRecordContent() != null) {
          ORecordInternal.fill(rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);
        }

        break;
      }

      case ORecordOperation.DELETED: {
        if (rec instanceof ODocument) {
          ORidBagDeleter.deleteAllRidBags((ODocument) rec);
        }
        doDeleteRecord(atomicOperation, rid, rec.getVersion(), cluster);
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
    if (iClusterId < 0 || iClusterId > clusters.size() - 1) {
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
    }
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
      } catch (final OWALPageBrokenException ignore) {
        lastCheckPoint = null;
      }

      if (lastCheckPoint == null) {
        OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
        return restoreFromBeginning();
      }

      List<OWriteableWALRecord> checkPointRecord;
      try {
        checkPointRecord = writeAheadLog.read(lastCheckPoint, 1);
      } catch (final OWALPageBrokenException ignore) {
        checkPointRecord = Collections.emptyList();
      }

      if (checkPointRecord.isEmpty()) {
        OLogManager.instance().info(this, "Checkpoints are absent, the restore will start from the beginning.");
        return restoreFromBeginning();
      }

      if (checkPointRecord.get(0) instanceof OFuzzyCheckpointStartRecord) {
        OLogManager.instance().info(this, "Found FUZZY checkpoint.");

        final boolean fuzzyCheckPointIsComplete = checkFuzzyCheckPointIsComplete(lastCheckPoint);
        if (!fuzzyCheckPointIsComplete) {
          OLogManager.instance().warn(this, "FUZZY checkpoint is not complete.");

          OFuzzyCheckpointStartRecord checkpointStartRecord = (OFuzzyCheckpointStartRecord) checkPointRecord.get(0);
          final OLogSequenceNumber previousCheckpoint = checkpointStartRecord.getPreviousCheckpoint();
          if (checkpointStartRecord instanceof OFuzzyCheckpointStartMetadataRecord) {
            this.lastMetadata = ((OFuzzyCheckpointStartMetadataRecord) checkpointStartRecord).getMetadata();
          }
          checkPointRecord = Collections.emptyList();

          if (previousCheckpoint != null) {
            checkPointRecord = writeAheadLog.read(previousCheckpoint, 1);
          }

          if (!checkPointRecord.isEmpty()) {
            OLogManager.instance().warn(this, "Restore will start from the previous checkpoint.");
            return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord.get(0));
          } else {
            OLogManager.instance().warn(this, "Restore will start from the beginning.");
            return restoreFromBeginning();
          }
        } else {
          return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord.get(0));
        }
      }

      if (checkPointRecord.get(0) instanceof OFullCheckpointStartRecord) {
        OLogManager.instance().info(this, "FULL checkpoint found.");
        final boolean fullCheckPointIsComplete = checkFullCheckPointIsComplete(lastCheckPoint);

        if (!fullCheckPointIsComplete) {
          OLogManager.instance().warn(this, "FULL checkpoint has not completed.");

          OFullCheckpointStartRecord checkpointStartRecord = (OFullCheckpointStartRecord) checkPointRecord.get(0);
          final OLogSequenceNumber previousCheckpoint = checkpointStartRecord.getPreviousCheckpoint();
          if (checkpointStartRecord instanceof OFullCheckpointStartMetadataRecord) {
            this.lastMetadata = ((OFullCheckpointStartMetadataRecord) checkpointStartRecord).getMetadata();
          }

          checkPointRecord = Collections.emptyList();
          if (previousCheckpoint != null) {
            checkPointRecord = writeAheadLog.read(previousCheckpoint, 1);
          }

          if (!checkPointRecord.isEmpty()) {
            OLogManager.instance().warn(this, "Restore will start from the previous checkpoint.");
            return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord.get(0));
          } else {
            OLogManager.instance().warn(this, "Restore will start from the beginning.");
            return restoreFromBeginning();
          }
        } else {
          return restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord.get(0));
        }
      }

      throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.get(0).getClass().getName());
    } finally {
      writeAheadLog.removeCutTillLimit(end);
    }
  }

  private boolean checkFullCheckPointIsComplete(final OLogSequenceNumber lastCheckPoint) throws IOException {
    try {
      List<OWriteableWALRecord> walRecords = writeAheadLog.next(lastCheckPoint, 10);

      while (!walRecords.isEmpty()) {
        for (final OWriteableWALRecord walRecord : walRecords) {
          if (walRecord instanceof OCheckpointEndRecord) {
            return true;
          }
        }

        walRecords = writeAheadLog.next(walRecords.get(walRecords.size() - 1).getLsn(), 10);
      }
    } catch (final OWALPageBrokenException ignore) {
      return false;
    }

    return false;
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public String incrementalBackup(final String backupDirectory, final OCallable<Void, Void> started)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Incremental backup is supported only in enterprise version");
  }

  @Override
  public boolean supportIncremental() {
    return false;
  }

  @Override
  public void fullIncrementalBackup(final OutputStream stream) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Incremental backup is supported only in enterprise version");
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    throw new UnsupportedOperationException("Incremental backup is supported only in enterprise version");
  }

  @Override
  public void restoreFullIncrementalBackup(final InputStream stream) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Incremental backup is supported only in enterprise version");
  }

  private boolean checkFuzzyCheckPointIsComplete(final OLogSequenceNumber lastCheckPoint) throws IOException {
    try {
      List<OWriteableWALRecord> walRecords = writeAheadLog.next(lastCheckPoint, 10);

      while (!walRecords.isEmpty()) {
        for (final OWriteableWALRecord walRecord : walRecords) {
          if (walRecord instanceof OFuzzyCheckpointEndRecord) {
            return true;
          }
        }

        walRecords = writeAheadLog.next(walRecords.get(walRecords.size() - 1).getLsn(), 10);
      }
    } catch (final OWALPageBrokenException ignore) {
      return false;
    }

    return false;
  }

  private OLogSequenceNumber restoreFromCheckPoint(final OAbstractCheckPointStartRecord checkPointRecord) throws IOException {
    if (checkPointRecord instanceof OFuzzyCheckpointStartRecord) {
      return restoreFromFuzzyCheckPoint((OFuzzyCheckpointStartRecord) checkPointRecord);
    }

    if (checkPointRecord instanceof OFullCheckpointStartRecord) {
      return restoreFromFullCheckPoint((OFullCheckpointStartRecord) checkPointRecord);
    }

    throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());
  }

  private OLogSequenceNumber restoreFromFullCheckPoint(final OFullCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().info(this, "Data restore procedure from full checkpoint is started. Restore is performed from LSN %s",
        checkPointRecord.getLsn());

    if (checkPointRecord instanceof OFullCheckpointStartMetadataRecord) {
      this.lastMetadata = ((OFullCheckpointStartMetadataRecord) checkPointRecord).getMetadata();
    }
    final OLogSequenceNumber lsn = writeAheadLog.next(checkPointRecord.getLsn(), 1).get(0).getLsn();
    return restoreFrom(lsn, writeAheadLog);
  }

  private OLogSequenceNumber restoreFromFuzzyCheckPoint(final OFuzzyCheckpointStartRecord checkPointRecord) throws IOException {
    OLogManager.instance().infoNoDb(this, "Data restore procedure from FUZZY checkpoint is started.");
    OLogSequenceNumber flushedLsn = checkPointRecord.getFlushedLsn();

    if (checkPointRecord instanceof OFuzzyCheckpointStartMetadataRecord) {
      this.lastMetadata = ((OFuzzyCheckpointStartMetadataRecord) checkPointRecord).getMetadata();
    }

    if (flushedLsn.compareTo(writeAheadLog.begin()) < 0) {
      OLogManager.instance().errorNoDb(this,
          "Fuzzy checkpoint points to removed part of the log, " + "will try to restore data from the rest of the WAL", null);
      flushedLsn = writeAheadLog.begin();
    }

    return restoreFrom(flushedLsn, writeAheadLog);
  }

  private OLogSequenceNumber restoreFromBeginning() throws IOException {
    OLogManager.instance().info(this, "Data restore procedure is started.");
    final OLogSequenceNumber lsn = writeAheadLog.begin();

    return restoreFrom(lsn, writeAheadLog);
  }

  @SuppressWarnings("WeakerAccess")
  protected final OLogSequenceNumber restoreFrom(final OLogSequenceNumber lsn, final OWriteAheadLog writeAheadLog)
      throws IOException {
    OLogSequenceNumber logSequenceNumber = null;
    final OModifiableBoolean atLeastOnePageUpdate = new OModifiableBoolean();

    long recordsProcessed = 0;

    final int reportBatchSize = OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();
    final Map<Long, byte[]> operationMetadata = new LinkedHashMap<>(1024);

    final Map<OOperationUnitId, List<OWALRecord>> operationUnitsByOperationId = new HashMap<>(1024);
    final Map<Long, List<OWALRecord>> operationUnitsByLongId = new HashMap<>(1024);

    long lastReportTime = 0;

    OLogSequenceNumber currentLSN;
    try {
      List<OWriteableWALRecord> records = writeAheadLog.read(lsn, 1_000);
      while (!records.isEmpty()) {
        for (final OWriteableWALRecord walRecord : records) {
          currentLSN = walRecord.getLsn();
          if (walRecord instanceof OOperationUnitRecord) {
            logSequenceNumber = currentLSN;

            final OOperationUnitRecord<?> operationUnitRecord = (OOperationUnitRecord<?>) walRecord;
            if (walRecord instanceof OAtomicUnitEndRecord) {
              final List<OWALRecord> atomicUnit;
              if (operationUnitRecord instanceof OperationUnitOperationId) {
                atomicUnit = removeAtomicUnitOperation(operationUnitsByOperationId, (OperationUnitOperationId) operationUnitRecord);
              } else if (operationUnitRecord instanceof LongOperationId) {
                atomicUnit = removeAtomicUnitOperation(operationUnitsByLongId, (LongOperationId) operationUnitRecord);
              } else {
                throw new IllegalStateException("Invalid type of operation id interface");
              }

              // in case of data restore from fuzzy checkpoint part of operations may be already flushed to the disk
              if (atomicUnit != null) {
                atomicUnit.add(walRecord);
                restoreAtomicUnit(atomicUnit, atLeastOnePageUpdate);
              }
              byte[] metadata = operationMetadata.remove(operationUnitRecord.getOperationUnitId());
              if (metadata != null) {
                this.lastMetadata = Optional.of(metadata);
              }

            } else if (walRecord instanceof OAtomicUnitStartRecord) {
              if (walRecord instanceof OAtomicUnitStartMetadataRecord) {
                byte[] metadata = ((OAtomicUnitStartMetadataRecord) walRecord).getMetadata();
                operationMetadata.put(((OAtomicUnitStartMetadataRecord) walRecord).getOperationUnitId(), metadata);
              }

              final List<OWALRecord> operationList;

              if (operationUnitRecord instanceof OperationUnitOperationId) {
                operationList = initAtomicOperation(operationUnitsByOperationId, (OperationUnitOperationId) operationUnitRecord);
              } else if (operationUnitRecord instanceof LongOperationId) {
                operationList = initAtomicOperation(operationUnitsByLongId, (LongOperationId) operationUnitRecord);
              } else {
                throw new IllegalStateException("Invalid type of operation id interface");
              }

              operationList.add(walRecord);
            } else {
              final List<OWALRecord> operationList;

              if (operationUnitRecord instanceof OperationUnitOperationId) {
                operationList = getAtomicUnitOperation(operationUnitsByOperationId, (OperationUnitOperationId) operationUnitRecord);
              } else if (operationUnitRecord instanceof LongOperationId) {
                operationList = getAtomicUnitOperation(operationUnitsByLongId, (LongOperationId) operationUnitRecord);
              } else {
                throw new IllegalStateException("Invalid type of operation id interface");
              }

            }
          } else if (walRecord instanceof ONonTxOperationPerformedWALRecord) {
            if (!wereNonTxOperationsPerformedInPreviousOpen) {
              OLogManager.instance()
                  .warnNoDb(this, "Non tx operation was used during data modification we will need index rebuild.");
              wereNonTxOperationsPerformedInPreviousOpen = true;
            }
          } else if (walRecord instanceof OFuzzyCheckpointStartMetadataRecord) {
            this.lastMetadata = ((OFuzzyCheckpointStartMetadataRecord) walRecord).getMetadata();
          } else if (walRecord instanceof OFullCheckpointStartMetadataRecord) {
            this.lastMetadata = ((OFullCheckpointStartMetadataRecord) walRecord).getMetadata();
          } else {
            OLogManager.instance().warnNoDb(this, "Record %s will be skipped during data restore", walRecord);
          }

          recordsProcessed++;

          final long currentTime = System.currentTimeMillis();
          if (reportBatchSize > 0 && recordsProcessed % reportBatchSize == 0
              || currentTime - lastReportTime > WAL_RESTORE_REPORT_INTERVAL) {
            OLogManager.instance()
                .infoNoDb(this, "%d operations were processed, current LSN is %s last LSN is %s", recordsProcessed, currentLSN,
                    writeAheadLog.end());
            lastReportTime = currentTime;
          }
        }

        records = writeAheadLog.next(records.get(records.size() - 1).getLsn(), 1_000);
      }
    } catch (final OWALPageBrokenException e) {
      OLogManager.instance()
          .errorNoDb(this, "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.",
              e);
    } catch (final RuntimeException e) {
      OLogManager.instance().errorNoDb(this,
          "Data restore was paused because of exception. The rest of changes will be rolled back and WAL files will be backed up."
              + " Please report issue about this exception to bug tracker and provide WAL files which are backed up in 'wal_backup' directory.",
          e);
      backUpWAL(e);
    }

    if (atLeastOnePageUpdate.getValue()) {
      return logSequenceNumber;
    }

    return null;
  }

  private static <T> List<OWALRecord> removeAtomicUnitOperation(final Map<T, List<OWALRecord>> operationUnitsById,
      OperationIdRecord<T> operationIdRecord) {
    return operationUnitsById.remove(operationIdRecord.getOperationUnitId());
  }

  private static <T> List<OWALRecord> getAtomicUnitOperation(final Map<T, List<OWALRecord>> operationUnitsById,
      OperationIdRecord<T> operationIdRecord) {
    final T operationUnitId = operationIdRecord.getOperationUnitId();
    List<OWALRecord> operationList = operationUnitsById.get(operationUnitId);

    if (operationList == null || operationList.isEmpty()) {
      OLogManager.instance()
          .errorNoDb(OAbstractPaginatedStorage.class, "'Start transaction' record is absent for atomic operation", null);

      if (operationList == null) {
        operationList = new ArrayList<>(1024);
        operationUnitsById.put(operationUnitId, operationList);
      }

    }

    return operationList;
  }

  private static <T> List<OWALRecord> initAtomicOperation(final Map<T, List<OWALRecord>> operationUnitsById,
      OperationIdRecord<T> operationIdRecord) {

    final List<OWALRecord> operationList = new ArrayList<>(1024);
    final T operationUnitId = operationIdRecord.getOperationUnitId();

    assert !operationUnitsById.containsKey(operationUnitId);
    operationUnitsById.put(operationUnitId, operationList);

    return operationList;
  }

  private void backUpWAL(final Exception e) {
    try {
      final File rootDir = new File(configuration.getDirectory());
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

          final PrintWriter metadataFileWriter = new PrintWriter(
              new OutputStreamWriter(archiveZipOutputStream, StandardCharsets.UTF_8));
          metadataFileWriter.append("Storage name : ").append(getName()).append("\r\n");
          metadataFileWriter.append("Date : ").append(strDate).append("\r\n");
          metadataFileWriter.append("Stacktrace : \r\n");
          e.printStackTrace(metadataFileWriter);
          metadataFileWriter.flush();
          archiveZipOutputStream.closeEntry();

          final List<String> walPaths = ((OCASDiskWriteAheadLog) writeAheadLog).getWalFiles();
          for (final String walSegment : walPaths) {
            archiveEntry(archiveZipOutputStream, walSegment);
          }

          archiveEntry(archiveZipOutputStream, ((OCASDiskWriteAheadLog) writeAheadLog).getWMRFile().toString());
        }
      }
    } catch (final IOException ioe) {
      OLogManager.instance().error(this, "Error during WAL backup", ioe);
    }
  }

  private static void archiveEntry(final ZipOutputStream archiveZipOutputStream, final String walSegment) throws IOException {
    final File walFile = new File(walSegment);
    final ZipEntry walZipEntry = new ZipEntry(walFile.getName());
    archiveZipOutputStream.putNextEntry(walZipEntry);
    try {
      try (final FileInputStream walInputStream = new FileInputStream(walFile)) {
        try (final BufferedInputStream walBufferedInputStream = new BufferedInputStream(walInputStream)) {
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

  @SuppressWarnings("WeakerAccess")
  protected final void restoreAtomicUnit(final List<OWALRecord> atomicUnit, final OModifiableBoolean atLeastOnePageUpdate)
      throws IOException {
    assert atomicUnit.get(atomicUnit.size() - 1) instanceof OAtomicUnitEndRecord;

    for (final OWALRecord walRecord : atomicUnit) {
      if (walRecord instanceof OFileDeletedWALRecord<?>) {
        final OFileDeletedWALRecord<?> fileDeletedWALRecord = (OFileDeletedWALRecord<?>) walRecord;
        if (writeCache.exists(fileDeletedWALRecord.getFileId())) {
          readCache.deleteFile(fileDeletedWALRecord.getFileId(), writeCache);
        }
      } else if (walRecord instanceof OFileCreatedWALRecord<?>) {
        final OFileCreatedWALRecord<?> fileCreatedCreatedWALRecord = (OFileCreatedWALRecord<?>) walRecord;

        if (!writeCache.exists(fileCreatedCreatedWALRecord.getFileName())) {
          readCache.addFile(fileCreatedCreatedWALRecord.getFileName(), fileCreatedCreatedWALRecord.getFileId(), writeCache);
        }
      } else if (walRecord instanceof OUpdatePageRecord<?>) {
        final OUpdatePageRecord<?> updatePageRecord = (OUpdatePageRecord<?>) walRecord;

        long fileId = updatePageRecord.getFileId();
        if (!writeCache.exists(fileId)) {
          final String fileName = writeCache.restoreFileById(fileId);

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

        OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, true, writeCache, 1, false, null);
        if (cacheEntry == null) {
          do {
            if (cacheEntry != null) {
              readCache.releaseFromWrite(cacheEntry, writeCache);
            }

            cacheEntry = readCache.allocateNewPage(fileId, writeCache, null);
          } while (cacheEntry.getPageIndex() != pageIndex);
        }

        try {
          final ODurablePage durablePage = new ODurablePage(cacheEntry);
          if (durablePage.getLsn().compareTo(walRecord.getLsn()) < 0) {
            durablePage.restoreChanges(updatePageRecord.getChanges());
            durablePage.setLsn(updatePageRecord.getLsn());
          }
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
    if (transaction.get() != null) {
      return;
    }

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
                  + "Required space is now set to " + configuration.getContextConfiguration()
                  .getValueAsInteger(OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT)
                  + "MB (you can change it by setting parameter " + OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getKey()
                  + ") .");
            } else {
              lowDiskSpace = null;
            }
          } else {
            lowDiskSpace = null;
          }
        } catch (final IOException e) {
          throw OException.wrapException(new OStorageException("Error during low disk space handling"), e);
        } finally {
          checkpointInProgress.set(false);
        }
      }
    }

    checkReadOnlyConditions();
  }

  public final void checkReadOnlyConditions() {
    if (dataFlushException != null) {
      throw OException.wrapException(new OStorageException(
              "Error in data flush background thread, please restart database and send full stack trace inside of bug report"),
          dataFlushException);
    }

    if (!brokenPages.isEmpty()) {
      //order pages by file and index
      final Map<String, SortedSet<Long>> pagesByFile = new HashMap<>(0);

      for (final OPair<String, Long> brokenPage : brokenPages) {
        final SortedSet<Long> sortedPages = pagesByFile.computeIfAbsent(brokenPage.key, (fileName) -> new TreeSet<>());
        sortedPages.add(brokenPage.value);
      }

      final StringBuilder brokenPagesList = new StringBuilder();
      brokenPagesList.append("[");

      for (final Map.Entry<String, SortedSet<Long>> stringSortedSetEntry : pagesByFile.entrySet()) {
        brokenPagesList.append('\'').append(stringSortedSetEntry.getKey()).append("' :");

        final SortedSet<Long> pageIndexes = stringSortedSetEntry.getValue();
        final long lastPage = pageIndexes.last();

        for (final Long pageIndex : stringSortedSetEntry.getValue()) {
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
  public void setStorageConfigurationUpdateListener(final OStorageConfigurationUpdateListener storageConfigurationUpdateListener) {
    stateLock.acquireWriteLock();
    try {
      checkOpenness();
      ((OClusterBasedStorageConfiguration) configuration).setConfigurationUpdateListener(storageConfigurationUpdateListener);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public void pauseConfigurationUpdateNotifications() {
    stateLock.acquireReadLock();
    try {
      checkOpenness();
      ((OClusterBasedStorageConfiguration) configuration).pauseUpdateNotifications();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public void fireConfigurationUpdateNotifications() {
    stateLock.acquireReadLock();
    try {
      checkOpenness();
      ((OClusterBasedStorageConfiguration) configuration).fireUpdateNotifications();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  private static final class ORIDOLockManager extends OComparableLockManager<ORID> {
    private ORIDOLockManager() {
      super(true, -1);
    }

    @Override
    protected final ORID getImmutableResourceId(final ORID iResourceId) {
      return new ORecordId(iResourceId);
    }
  }

  @SuppressWarnings("unused")
  protected static Map<Integer, List<ORecordId>> getRidsGroupedByCluster(final Collection<ORecordId> rids) {
    final Map<Integer, List<ORecordId>> ridsPerCluster = new HashMap<>(8);
    for (final ORecordId rid : rids) {
      final List<ORecordId> group = ridsPerCluster.computeIfAbsent(rid.getClusterId(), k -> new ArrayList<>(rids.size()));
      group.add(rid);
    }
    return ridsPerCluster;
  }

  private static void lockIndexes(final TreeMap<String, OTransactionIndexChanges> indexes) {
    for (final OTransactionIndexChanges changes : indexes.values()) {
      assert changes.changesPerKey instanceof TreeMap;

      final OIndexInternal<?> index = changes.getAssociatedIndex();

      final List<Object> orderedIndexNames = new ArrayList<>(changes.changesPerKey.keySet());
      if (orderedIndexNames.size() > 1) {
        orderedIndexNames.sort((o1, o2) -> {
          final String i1 = index.getIndexNameByKey(o1);
          final String i2 = index.getIndexNameByKey(o2);
          return i1.compareTo(i2);
        });
      }

      boolean fullyLocked = false;
      for (final Object key : orderedIndexNames) {
        if (index.acquireAtomicExclusiveLock(key)) {
          fullyLocked = true;
          break;
        }
      }
      if (!fullyLocked && !changes.nullKeyChanges.entries.isEmpty()) {
        index.acquireAtomicExclusiveLock(null);
      }
    }
  }

  private static void lockClusters(final TreeMap<Integer, OCluster> clustersToLock) {
    for (final OCluster cluster : clustersToLock.values()) {
      cluster.acquireAtomicExclusiveLock();
    }
  }

  private void lockRidBags(final TreeMap<Integer, OCluster> clusters, final TreeMap<String, OTransactionIndexChanges> indexes,
      final OIndexManager manager) {
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

    for (final Integer clusterId : clusters.keySet()) {
      atomicOperationsManager
          .acquireExclusiveLockTillOperationComplete(atomicOperation, OSBTreeCollectionManagerAbstract.generateLockName(clusterId));
    }

    for (final Map.Entry<String, OTransactionIndexChanges> entry : indexes.entrySet()) {
      final String indexName = entry.getKey();
      final OIndexInternal<?> index = entry.getValue().resolveAssociatedIndex(indexName, manager);

      if (!index.isUnique()) {
        atomicOperationsManager
            .acquireExclusiveLockTillOperationComplete(atomicOperation, OIndexRIDContainerSBTree.generateLockName(indexName));
      }
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

  protected final RuntimeException logAndPrepareForRethrow(final RuntimeException runtimeException) {
    if (!(runtimeException instanceof OHighLevelException || runtimeException instanceof ONeedRetryException)) {
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", runtimeException, System.identityHashCode(runtimeException),
              getURL(), OConstants.getVersion());
    }
    return runtimeException;
  }

  protected final Error logAndPrepareForRethrow(final Error error) {
    return logAndPrepareForRethrow(error, true);
  }

  private Error logAndPrepareForRethrow(final Error error, final boolean putInReadOnlyMode) {
    if (!(error instanceof OHighLevelException)) {
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", error, System.identityHashCode(error), getURL(),
              OConstants.getVersion());
    }

    if (putInReadOnlyMode) {
      handleJVMError(error);
    }

    return error;
  }

  protected final RuntimeException logAndPrepareForRethrow(final Throwable throwable) {
    if (!(throwable instanceof OHighLevelException || throwable instanceof ONeedRetryException)) {
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", throwable, System.identityHashCode(throwable), getURL(),
              OConstants.getVersion());
    }
    return new RuntimeException(throwable);
  }

  private OInvalidIndexEngineIdException logAndPrepareForRethrow(final OInvalidIndexEngineIdException exception) {
    OLogManager.instance()
        .errorStorage(this, "Exception `%08X` in storage `%s` : %s", exception, System.identityHashCode(exception), getURL(),
            OConstants.getVersion());
    return exception;
  }

  @Override
  public final OStorageConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public final void setSchemaRecordId(final String schemaRecordId) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setSchemaRecordId(atomicOperation, schemaRecordId);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setDateFormat(final String dateFormat) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setDateFormat(atomicOperation, dateFormat);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  @Override
  public final void setTimeZone(final TimeZone timeZoneValue) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setTimeZone(atomicOperation, timeZoneValue);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  @Override
  public final void setLocaleLanguage(final String locale) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setLocaleLanguage(atomicOperation, locale);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  @Override
  public final void setCharset(final String charset) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setCharset(atomicOperation, charset);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setIndexMgrRecordId(final String indexMgrRecordId) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setIndexMgrRecordId(atomicOperation, indexMgrRecordId);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setDateTimeFormat(final String dateTimeFormat) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setDateTimeFormat(atomicOperation, dateTimeFormat);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setLocaleCountry(final String localeCountry) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setLocaleCountry(atomicOperation, localeCountry);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setClusterSelection(final String clusterSelection) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setClusterSelection(atomicOperation, clusterSelection);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setMinimumClusters(final int minimumClusters) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();

      final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
      if (storageConfiguration.getMinimumClusters() == minimumClusters) {
        return;
      }

      atomicOperationsManager
          .executeInsideAtomicOperation((atomicOperation) -> storageConfiguration.setMinimumClusters(minimumClusters));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setValidation(final boolean validation) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setValidation(atomicOperation, validation);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  @Override
  public final void removeProperty(final String property) {
    checkOpenness();
    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.removeProperty(atomicOperation, property);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  @Override
  public final void setProperty(final String property, final String value) {
    checkOpenness();

    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setProperty(atomicOperation, property, value);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public final void setRecordSerializer(final String recordSerializer, final int version) {
    checkOpenness();

    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.setRecordSerializer(atomicOperation, recordSerializer);
        storageConfiguration.setRecordSerializerVersion(atomicOperation, version);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }

  }

  @Override
  public final void clearProperties() {
    checkOpenness();

    stateLock.acquireWriteLock();
    try {
      checkOpenness();

      makeStorageDirty();
      atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
        final OClusterBasedStorageConfiguration storageConfiguration = (OClusterBasedStorageConfiguration) configuration;
        storageConfiguration.clearProperties(atomicOperation);
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public Optional<byte[]> getLastMetadata() {
    return lastMetadata;
  }

  private static final class FuzzyCheckpointThreadFactory implements ThreadFactory {
    @Override
    public final Thread newThread(final Runnable r) {
      final Thread thread = new Thread(storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    }
  }

  private static final class StorageProfilerThreadFactory implements ThreadFactory {
    @Override
    public final Thread newThread(final Runnable r) {
      final Thread thread = new Thread(storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    }
  }

  private final class WALVacuum implements Runnable {

    private WALVacuum() {
    }

    @Override
    public void run() {
      stateLock.acquireReadLock();
      try {
        if (status == STATUS.CLOSED) {
          return;
        }

        final long[] nonActiveSegments = writeAheadLog.nonActiveSegments();
        if (nonActiveSegments.length == 0) {
          return;
        }

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
          final OLogSequenceNumber endLSN = writeAheadLog.end();
          final Long minLSNSegment = writeCache.getMinimalNotFlushedSegment();

          if (minLSNSegment == null) {
            minDirtySegment = endLSN.getSegment();
          } else {
            minDirtySegment = minLSNSegment;
          }
        } while (minDirtySegment < flushTillSegmentId);

        atomicOperationsTable.compactTable();
        final long operationSegment = atomicOperationsTable.getSegmentEarliestNotPersistedOperation();
        if (operationSegment >= 0 && minDirtySegment > operationSegment) {
          minDirtySegment = operationSegment;
        }

        if (minDirtySegment <= nonActiveSegments[0]) {
          return;
        }
        writeCache.makeFuzzyCheckpoint(minDirtySegment, lastMetadata);
      } catch (final Exception e) {
        dataFlushException = e;
        OLogManager.instance().error(this, "Error during flushing of data for fuzzy checkpoint", e);
      } finally {
        stateLock.releaseReadLock();
        walVacuumInProgress.set(false);
      }
    }
  }

  public void incOnOpen() {
    sessionCount.incrementAndGet();
  }

  private void decOnClose() {
    lastCloseTime.set(System.currentTimeMillis());
    sessionCount.decrementAndGet();
  }

  public int getSessionCount() {
    return sessionCount.get();
  }

  public long getLastCloseTime() {
    return lastCloseTime.get();
  }

  private static final class TransactionProfiler implements Runnable {

    private final int                  profilingDelay;
    private final AtomicBoolean        txIsCompleted;
    private final Thread               txThread;
    private final Path                 filePath;
    private final Map<String, Integer> stackMap = new HashMap<>();

    private final int stackTraceMapThreshold;

    private boolean firstTime = true;

    private volatile Future<?> future;
    private final    long      startTxTimeStamp;

    private long zipEntryCounter;

    private OutputStream    fileStream;
    private ZipOutputStream zipStream;

    private TransactionProfiler(int profilingDelay, AtomicBoolean txIsCompleted, Thread txThread, int stackTraceMapThreshold) {
      this.profilingDelay = profilingDelay;
      this.txIsCompleted = txIsCompleted;
      this.txThread = txThread;
      this.stackTraceMapThreshold = stackTraceMapThreshold;

      this.startTxTimeStamp = System.nanoTime();
      filePath = Paths.get(startTxTimeStamp + ".sprof").toAbsolutePath();
    }

    @Override
    public void run() {
      if (!txIsCompleted.get()) {
        if (firstTime) {
          OLogManager.instance().infoNoDb(this, "Transaction processing is taking too much time more than " + profilingDelay
              + " milliseconds, profiling of the transaction is automatically started. File with name " + filePath
              + " will be created to keep profiling data.");

          firstTime = false;
        }

        final StackTraceElement[] traceElements = txThread.getStackTrace();
        if (traceElements.length > 0) {
          final StringBuilder stackData = new StringBuilder();
          for (int i = traceElements.length - 1; i >= 0; i--) {
            final StackTraceElement element = traceElements[i];
            stackData.append(element.getClassName()).append(".").append(element.getMethodName()).append(";");
          }

          stackMap.compute(stackData.toString(), (key, value) -> {
            if (value == null) {
              return 1;
            }

            return value + 1;
          });
        }

        if (stackMap.size() > stackTraceMapThreshold) {
          try {
            flushStackMap(-1);
          } catch (IOException e) {
            OLogManager.instance().errorNoDb(this, "Can not create file" + filePath, e);
            if (future != null) {
              future.cancel(false);

              closeStreams();
            }
          }
        }
      } else {
        final long endTxTimeStamp = System.nanoTime();
        try {
          flushStackMap(endTxTimeStamp);
        } catch (IOException e) {
          OLogManager.instance().errorNoDb(this, "Can not write file" + filePath, e);
        }

        if (future != null) {
          future.cancel(false);
        }
        closeStreams();
      }
    }

    private void closeStreams() {
      if (zipStream != null) {
        try {
          zipStream.close();
          zipStream = null;
        } catch (IOException ex) {
          OLogManager.instance().errorNoDb(this, "Can not close file" + filePath, ex);
        }
      }
      if (fileStream != null) {
        try {
          fileStream.close();
          fileStream = null;
        } catch (IOException ex) {
          OLogManager.instance().errorNoDb(this, "Can not close file" + filePath, ex);
        }
      }
    }

    private void initZipStream() throws IOException {
      if (zipStream == null) {
        fileStream = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        zipStream = new ZipOutputStream(fileStream);
      }
    }

    private void flushStackMap(long endTxTimeStamp) throws IOException {
      if (!stackMap.isEmpty()) {
        zipEntryCounter++;

        initZipStream();

        zipStream.putNextEntry(new ZipEntry("profile" + zipEntryCounter + ".dat"));

        @SuppressWarnings("resource")
        final OutputStreamWriter writer = new OutputStreamWriter(zipStream);
        for (Map.Entry<String, Integer> entry : stackMap.entrySet()) {
          writer.write(entry.getKey() + " " + entry.getValue() + "\n");
        }
        writer.flush();

        zipStream.closeEntry();

        if (endTxTimeStamp > 0) {
          zipStream.putNextEntry(new ZipEntry("stat.dat"));
          final byte[] duration = new byte[8];
          zipStream.write(ByteBuffer.wrap(duration).putLong(endTxTimeStamp - startTxTimeStamp).array());
          zipStream.closeEntry();
        }

        stackMap.clear();
      }
    }
  }
}

