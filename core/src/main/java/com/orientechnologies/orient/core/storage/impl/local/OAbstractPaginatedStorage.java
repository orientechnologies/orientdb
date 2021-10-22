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
import com.orientechnologies.common.profiler.ModifiableLongProfileHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.*;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDeleter;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.encryption.impl.ONothingEncryption;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.exception.OClusterDoesNotExistException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OInvalidDatabaseNameException;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OInvalidInstanceIdException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.ORetryQueryException;
import com.orientechnologies.orient.core.exception.OStorageDoesNotExistException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.*;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeMultiValueIndexEngine;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeSingleValueIndexEngine;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
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
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsTable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.EmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.*;
import com.orientechnologies.orient.core.tx.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 28.03.13
 */
public abstract class OAbstractPaginatedStorage extends OStorageAbstract
    implements OCheckpointRequestListener,
        OIdentifiableStorage,
        OBackgroundExceptionListener,
        OFreezableStorageComponent,
        OPageIsBrokenListener {
  private static final int WAL_RESTORE_REPORT_INTERVAL = 30 * 1000; // milliseconds

  private static final Comparator<ORecordOperation> COMMIT_RECORD_OPERATION_COMPARATOR =
      Comparator.comparing(o -> o.getRecord().getIdentity());

  protected static final OScheduledThreadPoolExecutorWithLogging fuzzyCheckpointExecutor;
  // public static final int STORAGE_CONFIGURATION_INDEX_ID = -1;

  public OStorageInterruptionManager interruptionManager = new OStorageInterruptionManager();

  static {
    fuzzyCheckpointExecutor =
        new OScheduledThreadPoolExecutorWithLogging(1, new FuzzyCheckpointThreadFactory());
    fuzzyCheckpointExecutor.setMaximumPoolSize(1);
  }

  private final OSimpleRWLockManager<ORID> lockManager;
  protected volatile OSBTreeCollectionManagerShared sbTreeCollectionManager;

  /** Lock is used to atomically update record versions. */
  private final OLockManager<ORID> recordVersionManager;

  private final Map<String, OCluster> clusterMap = new HashMap<>();
  private final List<OCluster> clusters = new ArrayList<>();

  private volatile ThreadLocal<OStorageTransaction> transaction;
  private final AtomicBoolean walVacuumInProgress = new AtomicBoolean();

  protected volatile OWriteAheadLog writeAheadLog;
  private OStorageRecoverListener recoverListener;

  protected volatile OReadCache readCache;
  protected volatile OWriteCache writeCache;

  private volatile ORecordConflictStrategy recordConflictStrategy =
      Orient.instance().getRecordConflictStrategy().getDefaultImplementation();

  private volatile int defaultClusterId = -1;
  protected volatile OAtomicOperationsManager atomicOperationsManager;
  private volatile boolean wereNonTxOperationsPerformedInPreviousOpen;
  private volatile boolean modificationLock;
  private volatile boolean readLock;

  private final int id;

  private final Map<String, OBaseIndexEngine> indexEngineNameMap = new HashMap<>();
  private final List<OBaseIndexEngine> indexEngines = new ArrayList<>();
  private final AtomicOperationIdGen idGen = new AtomicOperationIdGen();

  private boolean wereDataRestoredAfterOpen;
  private UUID uuid;
  private volatile byte[] lastMetadata = null;

  private final OModifiableLong recordCreated = new OModifiableLong();
  private final OModifiableLong recordUpdated = new OModifiableLong();
  private final OModifiableLong recordRead = new OModifiableLong();
  private final OModifiableLong recordDeleted = new OModifiableLong();

  private final OModifiableLong recordScanned = new OModifiableLong();
  private final OModifiableLong recordRecycled = new OModifiableLong();
  private final OModifiableLong recordConflict = new OModifiableLong();
  private final OModifiableLong txBegun = new OModifiableLong();
  private final OModifiableLong txCommit = new OModifiableLong();
  private final OModifiableLong txRollback = new OModifiableLong();

  private final AtomicInteger sessionCount = new AtomicInteger(0);
  private final AtomicLong lastCloseTime = new AtomicLong(System.currentTimeMillis());

  protected static final String DATABASE_INSTANCE_ID = "databaseInstenceId";

  protected AtomicOperationsTable atomicOperationsTable;

  private final Set<Thread> blockedThreads = Collections.newSetFromMap(new WeakHashMap<>());

  public OAbstractPaginatedStorage(
      final String name, final String filePath, final String mode, final int id) {
    super(name, filePath, mode);

    this.id = id;
    lockManager = new ONotThreadRWLockManager<>();
    recordVersionManager = new OPartitionedLockManager<>();
    sbTreeCollectionManager = new OSBTreeCollectionManagerShared(this);

    registerProfilerHooks();
  }

  private static void checkPageSizeAndRelatedParametersInGlobalConfiguration() {
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
    final int maxKeySize = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();

    if (maxKeySize > pageSize / 4) {
      throw new OStorageException(
          "Value of parameter "
              + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
              + " should be at least 4 times bigger than value of parameter "
              + OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getKey()
              + " but real values are :"
              + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
              + " = "
              + pageSize
              + " , "
              + OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getKey()
              + " = "
              + maxKeySize);
    }
  }

  private static TreeMap<String, OTransactionIndexChanges> getSortedIndexOperations(
      final OTransactionInternal clientTx) {
    return new TreeMap<>(clientTx.getIndexOperations());
  }

  @Override
  public final void open(
      final String iUserName,
      final String iUserPassword,
      final OContextConfiguration contextConfiguration) {
    open(contextConfiguration);
  }

  public final void open(final OContextConfiguration contextConfiguration) {
    try {
      stateLock.acquireReadLock();
      try {
        if (status == STATUS.OPEN || status == STATUS.INTERNAL_ERROR)
        // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
        // REUSED
        {
          return;
        }
      } finally {
        stateLock.releaseReadLock();
      }

      try {

        stateLock.acquireWriteLock();
        try {
          if (status == STATUS.OPEN || status == STATUS.INTERNAL_ERROR)
          // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
          // REUSED
          {
            return;
          }

          if (status != STATUS.CLOSED) {
            throw new OStorageException(
                "Storage " + name + " is in wrong state " + status + " and can not be opened.");
          }

          if (!exists()) {
            throw new OStorageDoesNotExistException(
                "Cannot open the storage '" + name + "' because it does not exist in path: " + url);
          }

          initLockingStrategy(contextConfiguration);

          readIv();

          initWalAndDiskCache(contextConfiguration);
          transaction = new ThreadLocal<>();

          final StartupMetadata startupMetadata = checkIfStorageDirty();
          final long lastTxId = startupMetadata.lastTxId;
          if (lastTxId > 0) {
            idGen.setStartId(lastTxId + 1);
          } else {
            idGen.setStartId(0);
          }

          atomicOperationsTable =
              new AtomicOperationsTable(
                  contextConfiguration.getValueAsInteger(
                      OGlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_LIMIT),
                  idGen.getLastId() + 1);
          atomicOperationsManager =
              new OAtomicOperationsManager(
                  this,
                  contextConfiguration.getValueAsBoolean(
                      OGlobalConfiguration.STORAGE_TRACK_PAGE_OPERATIONS_IN_TX),
                  contextConfiguration.getValueAsInteger(
                          OGlobalConfiguration.STORAGE_PAGE_OPERATIONS_CACHE_SIZE)
                      * 1024
                      * 1024,
                  atomicOperationsTable);

          recoverIfNeeded();

          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                if (OClusterBasedStorageConfiguration.exists(writeCache)) {
                  configuration = new OClusterBasedStorageConfiguration(this);
                  ((OClusterBasedStorageConfiguration) configuration)
                      .load(contextConfiguration, atomicOperation);

                  // otherwise delayed to disk based storage to convert old format to new format.
                }

                initConfiguration(atomicOperation, contextConfiguration);
              });

          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              (atomicOperation) -> {
                String uuid = configuration.getUuid();
                if (uuid == null) {
                  uuid = UUID.randomUUID().toString();
                  configuration.setUuid(atomicOperation, uuid);
                }
                this.uuid = UUID.fromString(uuid);
              });

          checkPageSizeAndRelatedParameters();

          componentsFactory = new OCurrentStorageComponentsFactory(configuration);

          sbTreeCollectionManager.load();

          atomicOperationsManager.executeInsideAtomicOperation(null, this::openClusters);
          openIndexes();

          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              (atomicOperation) -> {
                final String cs = configuration.getConflictStrategy();
                if (cs != null) {
                  // SET THE CONFLICT STORAGE STRATEGY FROM THE LOADED CONFIGURATION
                  doSetConflictStrategy(
                      Orient.instance().getRecordConflictStrategy().getStrategy(cs),
                      atomicOperation);
                }
                if (lastMetadata == null) {
                  lastMetadata = startupMetadata.txMetadata;
                }
              });

          status = STATUS.MIGRATION;
        } finally {
          stateLock.releaseWriteLock();
        }

        // we need to use read lock to allow for example correctly truncate WAL during data
        // processing
        // all operations are prohibited on storage because of usage of special status.
        stateLock.acquireReadLock();
        try {
          if (status != STATUS.MIGRATION) {
            OLogManager.instance()
                .errorStorage(
                    this,
                    "Unexpected storage status %s, process of creation of storage is aborted",
                    null,
                    status.name());
            return;
          }

          sbTreeCollectionManager.migrate();
        } finally {
          stateLock.releaseReadLock();
        }

        stateLock.acquireWriteLock();
        try {
          if (status != STATUS.MIGRATION) {
            OLogManager.instance()
                .errorStorage(
                    this,
                    "Unexpected storage status %s, process of creation of storage is aborted",
                    null,
                    status.name());
            return;
          }

          // we need to check presence of ridbags for backward compatibility with previous
          // versions
          atomicOperationsManager.executeInsideAtomicOperation(null, this::checkRidBagsPresence);
          status = STATUS.OPEN;
        } finally {
          stateLock.releaseWriteLock();
        }

      } catch (final RuntimeException e) {
        try {
          if (writeCache != null) {
            readCache.closeStorage(writeCache);
          }
        } catch (final Exception ee) {
          // ignore
        }

        try {
          writeAheadLog.close();
        } catch (final Exception ee) {
          // ignore
        }

        try {
          postCloseSteps(false, true, idGen.getLastId());
        } catch (final Exception ee) {
          // ignore
        }

        status = STATUS.CLOSED;
        throw e;
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    OLogManager.instance()
        .infoNoDb(
            this,
            "Storage '%s' is opened under OrientDB distribution : %s",
            getURL(),
            OConstants.getVersion());
  }

  protected abstract void readIv() throws IOException;

  @SuppressWarnings("unused")
  protected abstract byte[] getIv();

  private void initLockingStrategy(final OContextConfiguration contextConfiguration) {
    final String lockKind =
        contextConfiguration.getValueAsString(OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING);
    if (OrientDBConfig.LOCK_TYPE_MODIFICATION.equals(lockKind)) {
      modificationLock = true;
    } else if (OrientDBConfig.LOCK_TYPE_READWRITE.equals(lockKind)) {
      modificationLock = true;
      readLock = true;
    }
  }

  /** @inheritDoc */
  @Override
  public final String getCreatedAtVersion() {
    return configuration.getCreatedAtVersion();
  }

  protected final void openIndexes() {
    final OCurrentStorageComponentsFactory cf = componentsFactory;
    if (cf == null) {
      throw new OStorageException("Storage '" + name + "' is not properly initialized");
    }
    final Set<String> indexNames = configuration.indexEngines();
    int counter = 0;

    // avoid duplication of index engine ids
    for (final String indexName : indexNames) {
      final OStorageConfiguration.IndexEngineData engineData =
          configuration.getIndexEngine(indexName, -1);
      if (counter <= engineData.getIndexId()) {
        counter = engineData.getIndexId() + 1;
      }
    }

    for (final String indexName : indexNames) {
      final OStorageConfiguration.IndexEngineData engineData =
          configuration.getIndexEngine(indexName, counter);
      final OBaseIndexEngine engine =
          OIndexes.createIndexEngine(
              engineData.getIndexId(),
              engineData.getName(),
              engineData.getAlgorithm(),
              engineData.getIndexType(),
              engineData.getDurableInNonTxMode(),
              this,
              engineData.getVersion(),
              engineData.getApiVersion(),
              engineData.isMultivalue(),
              engineData.getEngineProperties());

      final OEncryption encryption;
      if (engineData.getEncryption() == null
          || engineData
              .getEncryption()
              .toLowerCase(configuration.getLocaleInstance())
              .equals(ONothingEncryption.NAME)) {
        encryption = null;
      } else {
        encryption =
            OEncryptionFactory.INSTANCE.getEncryption(
                engineData.getEncryption(), engineData.getEncryptionOptions());
      }

      if (engineData.getApiVersion() < 1) {
        ((OIndexEngine) engine)
            .load(
                engineData.getName(),
                cf.binarySerializerFactory.getObjectSerializer(engineData.getValueSerializerId()),
                engineData.isAutomatic(),
                cf.binarySerializerFactory.getObjectSerializer(engineData.getKeySerializedId()),
                engineData.getKeyTypes(),
                engineData.isNullValuesSupport(),
                engineData.getKeySize(),
                engineData.getEngineProperties(),
                encryption);

      } else {
        ((OV1IndexEngine) engine)
            .load(
                engineData.getName(),
                engineData.getKeySize(),
                engineData.getKeyTypes(),
                cf.binarySerializerFactory.getObjectSerializer(engineData.getKeySerializedId()),
                encryption);
      }
      indexEngineNameMap.put(engineData.getName(), engine);
      while (engineData.getIndexId() >= indexEngines.size()) {
        indexEngines.add(null);
      }
      indexEngines.set(engineData.getIndexId(), engine);
      counter++;
    }
  }

  protected final void openClusters(final OAtomicOperation atomicOperation) throws IOException {
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
            clusters.get(i).open(atomicOperation);
          } else {
            if (clusterConfig.getName().equals(CLUSTER_DEFAULT_NAME)) {
              defaultClusterId = pos;
            }

            clusters.get(pos).open(atomicOperation);
          }
        } catch (final FileNotFoundException e) {
          OLogManager.instance()
              .warn(
                  this,
                  "Error on loading cluster '"
                      + configurationClusters.get(i).getName()
                      + "' ("
                      + i
                      + "): file not found. It will be excluded from current database '"
                      + getName()
                      + "'.",
                  e);

          clusterMap.remove(
              configurationClusters
                  .get(i)
                  .getName()
                  .toLowerCase(configuration.getLocaleInstance()));

          setCluster(i, null);
        }
      } else {
        setCluster(i, null);
      }
    }
  }

  private void checkRidBagsPresence(final OAtomicOperation operation) {
    for (final OCluster cluster : clusters) {
      // FIXME: this should not be needed, no null clusters should be in the clusters list
      if (cluster != null) {
        final int clusterId = cluster.getId();

        if (!sbTreeCollectionManager.isComponentPresent(operation, clusterId)) {
          OLogManager.instance()
              .info(
                  this,
                  "Cluster with id %d does not have associated rid bag, fixing ...",
                  clusterId);
          sbTreeCollectionManager.createComponent(operation, clusterId);
        }
      }
    }
  }

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
              "Only letters, numbers, `$`, `_` and `-` are allowed in database name. Provided name :`"
                  + name
                  + "`");
        }

        if (status != STATUS.CLOSED) {
          throw new OStorageExistsException(
              "Cannot create new storage '" + getURL() + "' because it is not closed");
        }

        if (exists()) {
          throw new OStorageExistsException(
              "Cannot create new storage '" + getURL() + "' because it already exists");
        }

        uuid = UUID.randomUUID();
        initLockingStrategy(contextConfiguration);
        initIv();

        initWalAndDiskCache(contextConfiguration);

        atomicOperationsTable =
            new AtomicOperationsTable(
                contextConfiguration.getValueAsInteger(
                    OGlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_LIMIT),
                idGen.getLastId() + 1);
        atomicOperationsManager =
            new OAtomicOperationsManager(
                this,
                contextConfiguration.getValueAsBoolean(
                    OGlobalConfiguration.STORAGE_TRACK_PAGE_OPERATIONS_IN_TX),
                contextConfiguration.getValueAsInteger(
                        OGlobalConfiguration.STORAGE_PAGE_OPERATIONS_CACHE_SIZE)
                    * 1024
                    * 1024,
                atomicOperationsTable);
        transaction = new ThreadLocal<>();

        preCreateSteps();
        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            (atomicOperation) -> {
              configuration = new OClusterBasedStorageConfiguration(this);
              ((OClusterBasedStorageConfiguration) configuration)
                  .create(atomicOperation, contextConfiguration);
              configuration.setUuid(atomicOperation, uuid.toString());

              componentsFactory = new OCurrentStorageComponentsFactory(configuration);

              sbTreeCollectionManager.load();

              status = STATUS.OPEN;

              sbTreeCollectionManager = new OSBTreeCollectionManagerShared(this);

              // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
              doAddCluster(atomicOperation, OMetadataDefault.CLUSTER_INTERNAL_NAME);

              ((OClusterBasedStorageConfiguration) configuration)
                  .setCreationVersion(atomicOperation, OConstants.getVersion());
              ((OClusterBasedStorageConfiguration) configuration)
                  .setPageSize(
                      atomicOperation,
                      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
              ((OClusterBasedStorageConfiguration) configuration)
                  .setMaxKeySize(
                      atomicOperation,
                      OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger());

              generateDatabaseInstanceId(atomicOperation);

              // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
              // INDEXING
              doAddCluster(atomicOperation, OMetadataDefault.CLUSTER_INDEX_NAME);

              // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
              // INDEXING
              doAddCluster(atomicOperation, OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);

              // ADD THE DEFAULT CLUSTER
              defaultClusterId = doAddCluster(atomicOperation, CLUSTER_DEFAULT_NAME);

              clearStorageDirty();

              postCreateSteps();

              // binary compatibility with previous version, this record contained configuration of
              // storage
              doCreateRecord(
                  atomicOperation,
                  new ORecordId(0, -1),
                  new byte[] {0, 0, 0, 0},
                  0,
                  OBlob.RECORD_TYPE,
                  null,
                  doGetAndCheckCluster(0),
                  null);
            });

      } catch (final InterruptedException e) {
        throw OException.wrapException(
            new OStorageException("Storage creation was interrupted"), e);
      } catch (final OStorageException e) {
        close();
        throw e;
      } catch (final IOException e) {
        close();
        throw OException.wrapException(
            new OStorageException("Error on creation of storage '" + name + "'"), e);
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
        .infoNoDb(
            this,
            "Storage '%s' is created under OrientDB distribution : %s",
            getURL(),
            OConstants.getVersion());
  }

  protected void generateDatabaseInstanceId(OAtomicOperation atomicOperation) {
    ((OClusterBasedStorageConfiguration) configuration)
        .setProperty(atomicOperation, DATABASE_INSTANCE_ID, UUID.randomUUID().toString());
  }

  protected UUID readDatabaseInstanceId() {
    String id = configuration.getProperty(DATABASE_INSTANCE_ID);
    if (id != null) {
      return UUID.fromString(id);
    } else {
      return null;
    }
  }

  protected void checkDatabaseInstanceId(UUID backupUUID) {
    UUID dbUUID = readDatabaseInstanceId();
    if (backupUUID == null) {
      throw new OInvalidInstanceIdException(
          "The Database Instance Id do not mach, backup UUID is null");
    }
    if (dbUUID != null) {
      if (!dbUUID.equals(backupUUID)) {
        throw new OInvalidInstanceIdException(
            String.format(
                "The Database Instance Id do not mach, database: '%s' backup: '%s'",
                dbUUID, backupUUID));
      }
    }
  }

  protected abstract void initIv() throws IOException;

  private void checkPageSizeAndRelatedParameters() {
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
    final int maxKeySize = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();

    if (configuration.getPageSize() != -1 && configuration.getPageSize() != pageSize) {
      throw new OStorageException(
          "Storage is created with value of "
              + OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
              + " parameter equal to "
              + configuration.getPageSize()
              + " but current value is "
              + pageSize);
    }

    if (configuration.getMaxKeySize() != -1 && configuration.getMaxKeySize() != maxKeySize) {
      throw new OStorageException(
          "Storage is created with value of "
              + OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getKey()
              + " parameter equal to "
              + configuration.getMaxKeySize()
              + " but current value is "
              + maxKeySize);
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
        makeStorageDirty();

        // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
        close(true, true);
        postDeleteSteps();
      } finally {
        stateLock.releaseWriteLock();
        Orient.instance()
            .getProfiler()
            .stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");
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

      stateLock.acquireReadLock();
      try {
        final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
        try {
          interruptionManager.enterCriticalPath();
          checkOpennessAndMigration();
          checkIfThreadIsBlocked();

          final long start = System.currentTimeMillis();

          final OPageDataVerificationError[] pageErrors =
              writeCache.checkStoredPages(verbose ? listener : null);

          String errors =
              pageErrors.length > 0 ? pageErrors.length + " with errors." : " without errors.";
          listener.onMessage(
              "Check of storage completed in "
                  + (System.currentTimeMillis() - start)
                  + "ms. "
                  + errors);

          return pageErrors.length == 0;
        } finally {
          atomicOperationsManager.releaseAtomicOperations(lockId);
          interruptionManager.exitCriticalPath();
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
      stateLock.acquireWriteLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            null, (atomicOperation) -> doAddCluster(atomicOperation, clusterName));

      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Error in creation of new cluster '" + clusterName), e);
      } finally {
        stateLock.releaseWriteLock();
        interruptionManager.exitCriticalPath();
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
  public final int addCluster(final String clusterName, final int requestedId) {
    try {
      stateLock.acquireWriteLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        if (requestedId < 0) {
          throw new OConfigurationException("Cluster id must be positive!");
        }
        if (requestedId < clusters.size() && clusters.get(requestedId) != null) {
          throw new OConfigurationException(
              "Requested cluster ID ["
                  + requestedId
                  + "] is occupied by cluster with name ["
                  + clusters.get(requestedId).getName()
                  + "]");
        }

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            null, atomicOperation -> doAddCluster(atomicOperation, clusterName, requestedId));

      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Error in creation of new cluster '" + clusterName + "'"), e);
      } finally {
        stateLock.releaseWriteLock();
        interruptionManager.exitCriticalPath();
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
  public final boolean dropCluster(final int clusterId) {
    try {
      stateLock.acquireWriteLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();

        checkIfThreadIsBlocked();

        if (clusterId < 0 || clusterId >= clusters.size()) {
          throw new IllegalArgumentException(
              "Cluster id '"
                  + clusterId
                  + "' is outside the of range of configured clusters (0-"
                  + (clusters.size() - 1)
                  + ") in database '"
                  + name
                  + "'");
        }

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation -> {
              if (dropClusterInternal(atomicOperation, clusterId)) {
                return false;
              }

              ((OClusterBasedStorageConfiguration) configuration)
                  .dropCluster(atomicOperation, clusterId);
              sbTreeCollectionManager.deleteComponentByClusterId(atomicOperation, clusterId);

              return true;
            });
      } catch (final Exception e) {
        throw OException.wrapException(
            new OStorageException("Error while removing cluster '" + clusterId + "'"), e);

      } finally {
        stateLock.releaseWriteLock();
        interruptionManager.exitCriticalPath();
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
      throw new OClusterDoesNotExistException(
          "Cluster id '"
              + clusterId
              + "' is outside the of range of configured clusters (0-"
              + (clusters.size() - 1)
              + ") in database '"
              + name
              + "'");
    }
  }

  @Override
  public String getClusterNameById(int clusterId) {
    try {
      stateLock.acquireReadLock();
      try {
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getRecordsSize();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OCluster cluster =
            clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (cluster == null) {
          throwClusterDoesNotExist(clusterName);
        }

        return cluster.getRecordsSize();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return Optional.ofNullable(cluster.getRecordConflictStrategy())
            .map(ORecordConflictStrategy::getName)
            .orElse(null);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.encryption();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.isSystemCluster();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getLastPosition();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return cluster.getNextPosition();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final int clusterId = rid.getClusterId();
        checkClusterId(clusterId);
        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null) {
          throwClusterDoesNotExist(clusterId);
        }

        return ((OPaginatedCluster) cluster).getRecordStatus(rid.getClusterPosition());
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
    throw new OClusterDoesNotExistException(
        "Cluster with id " + clusterId + " does not exist inside of storage " + name);
  }

  private void throwClusterDoesNotExist(String clusterName) {
    throw new OClusterDoesNotExistException(
        "Cluster with name `" + clusterName + "` does not exist inside of storage " + name);
  }

  @Override
  public final int getId() {
    return id;
  }

  public UUID getUuid() {
    return uuid;
  }

  private boolean setClusterStatus(
      final OAtomicOperation atomicOperation,
      final OCluster cluster,
      final OStorageClusterConfiguration.STATUS iStatus)
      throws IOException {
    if (iStatus == OStorageClusterConfiguration.STATUS.OFFLINE && cluster instanceof OOfflineCluster
        || iStatus == OStorageClusterConfiguration.STATUS.ONLINE
            && !(cluster instanceof OOfflineCluster)) {
      return false;
    }

    final OCluster newCluster;
    final int clusterId = cluster.getId();
    if (iStatus == OStorageClusterConfiguration.STATUS.OFFLINE) {
      cluster.close(true);
      newCluster = new OOfflineCluster(this, clusterId, cluster.getName());

      boolean configured = false;
      for (final OStorageClusterConfiguration clusterConfiguration : configuration.getClusters()) {
        if (clusterConfiguration.getId() == cluster.getId()) {
          newCluster.configure(this, clusterConfiguration);
          configured = true;
          break;
        }
      }

      if (!configured) {
        throw new OStorageException("Can not configure offline cluster with id " + clusterId);
      }
    } else {
      newCluster =
          OPaginatedClusterFactory.createCluster(
              cluster.getName(), configuration.getVersion(), cluster.getBinaryVersion(), this);
      newCluster.configure(clusterId, cluster.getName());
      newCluster.open(atomicOperation);
    }

    clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), newCluster);
    clusters.set(clusterId, newCluster);

    ((OClusterBasedStorageConfiguration) configuration)
        .setClusterStatus(atomicOperation, clusterId, iStatus);

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
        throw new OStorageException(
            "Cluster Id " + clusterId + " is invalid in database '" + name + "'");
      }

      // COUNT PHYSICAL CLUSTER IF ANY
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

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
        interruptionManager.exitCriticalPath();
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
        return new long[] {ORID.CLUSTER_POS_INVALID, ORID.CLUSTER_POS_INVALID};
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        if (clusters.get(iClusterId) != null) {
          return new long[] {
            clusters.get(iClusterId).getFirstPosition(), clusters.get(iClusterId).getLastPosition()
          };
        } else {
          return OCommonConst.EMPTY_LONG_ARRAY;
        }

      } catch (final IOException ioe) {
        throw OException.wrapException(
            new OStorageException("Cannot retrieve information about data range"), ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
    stateLock.acquireWriteLock();
    try {
      OLogManager.instance()
          .errorStorage(
              this,
              "Error in data flush background thread, for storage %s ,"
                  + "please restart database and send full stack trace inside of bug report",
              e,
              name);

      if (status == STATUS.CLOSED) {
        return;
      }

      error = e;
      status = STATUS.INTERNAL_ERROR;

      try {
        makeStorageDirty();
      } catch (IOException ioException) {
        // ignore
      }
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public Optional<OBackgroundNewDelta> extractTransactionsFromWal(
      List<OTransactionId> transactionsMetadata) {
    Map<OTransactionId, OTransactionData> finished = new HashMap<>();
    List<OTransactionId> started = new ArrayList<>();
    stateLock.acquireReadLock();
    try {
      Set<OTransactionId> transactionsToRead = new HashSet<>(transactionsMetadata);
      // we iterate till the last record is contained in wal at the moment when we call this method
      OLogSequenceNumber beginLsn = writeAheadLog.begin();
      Map<Long, OTransactionData> units = new HashMap<>();

      writeAheadLog.addCutTillLimit(beginLsn);
      try {
        List<WriteableWALRecord> records = writeAheadLog.next(beginLsn, 1_000);
        // all information about changed records is contained in atomic operation metadata
        while (!records.isEmpty()) {
          for (final OWALRecord record : records) {

            if (record instanceof OFileCreatedWALRecord) {
              return Optional.empty();
            }

            if (record instanceof OFileDeletedWALRecord) {
              return Optional.empty();
            }

            if (record instanceof OAtomicUnitStartMetadataRecord) {
              byte[] meta = ((OAtomicUnitStartMetadataRecord) record).getMetadata();
              OTxMetadataHolder data = OTxMetadataHolderImpl.read(meta);
              // This will not be a byte to byte compare, but should compare only the tx id not all
              // status
              //noinspection ConstantConditions
              OTransactionId txId =
                  new OTransactionId(
                      Optional.empty(), data.getId().getPosition(), data.getId().getSequence());
              if (transactionsToRead.contains(txId)) {
                long unitId = ((OAtomicUnitStartMetadataRecord) record).getOperationUnitId();
                units.put(unitId, new OTransactionData(txId));
                started.add(txId);
              }
            }
            if (record instanceof OAtomicUnitEndRecord) {
              long opId = ((OAtomicUnitEndRecord) record).getOperationUnitId();
              OTransactionData opes = units.remove(opId);
              if (opes != null) {
                transactionsToRead.remove(opes.getTransactionId());
                finished.put(opes.getTransactionId(), opes);
              }
            }
            if (record instanceof OHighLevelTransactionChangeRecord) {
              byte[] data = ((OHighLevelTransactionChangeRecord) record).getData();
              long unitId = ((OHighLevelTransactionChangeRecord) record).getOperationUnitId();
              OTransactionData tx = units.get(unitId);
              if (tx != null) {
                tx.addRecord(data);
              }
            }
            if (transactionsToRead.isEmpty() && units.isEmpty()) {
              // all read stop scanning and return the transactions
              List<OTransactionData> transactions = new ArrayList<>();
              for (OTransactionId id : started) {
                OTransactionData data = finished.get(id);
                if (data != null) {
                  transactions.add(data);
                }
              }
              return Optional.of(new OBackgroundNewDelta(transactions));
            }
          }
          records = writeAheadLog.next(records.get(records.size() - 1).getLsn(), 1_000);
        }
      } finally {
        writeAheadLog.removeCutTillLimit(beginLsn);
      }
      if (transactionsToRead.isEmpty()) {
        List<OTransactionData> transactions = new ArrayList<>();
        for (OTransactionId id : started) {
          OTransactionData data = finished.get(id);
          if (data != null) {
            transactions.add(data);
          }
        }
        return Optional.of(new OBackgroundNewDelta(transactions));
      } else {
        return Optional.empty();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error of reading of records from  WAL"), e);
    } finally {
      stateLock.releaseReadLock();
    }
  }

  void serializeDeltaContent(
      OutputStream stream,
      OCommandOutputListener outputListener,
      SortedSet<ORID> sortedRids,
      OLogSequenceNumber lsn) {
    try {
      stateLock.acquireReadLock();
      final int totalRecords = sortedRids.size();
      OLogManager.instance()
          .info(this, "Exporting records after LSN=%s. Found %d records", lsn, totalRecords);
      // records may be deleted after we flag them as existing and as result rule of sorting of
      // records
      // (deleted records go first will be broken), so we prohibit any modifications till we do not
      // complete method execution
      final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
      try {
        try (final DataOutputStream dataOutputStream = new DataOutputStream(stream)) {

          dataOutputStream.writeLong(sortedRids.size());

          long exportedRecord = 1;

          for (ORID rid : sortedRids) {
            final OCluster cluster = clusters.get(rid.getClusterId());

            dataOutputStream.writeInt(rid.getClusterId());
            dataOutputStream.writeLong(rid.getClusterPosition());

            if (cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()))
                == null) {
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
                  .debug(
                      this,
                      "Exporting modified record rid=%s type=%d size=%d v=%d - buffer size=%d",
                      rid,
                      rawBuffer.recordType,
                      rawBuffer.buffer.length,
                      rawBuffer.version,
                      dataOutputStream.size());
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

  @Override
  public final long count(final int[] iClusterIds, final boolean countTombstones) {
    try {
      long tot = 0;

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        for (final int iClusterId : iClusterIds) {
          if (iClusterId >= clusters.size()) {
            throw new OConfigurationException(
                "Cluster id " + iClusterId + " was not found in database '" + name + "'");
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
        interruptionManager.exitCriticalPath();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final OStorageOperationResult<OPhysicalPosition> createRecord(
      final ORecordId rid,
      final byte[] content,
      final int recordVersion,
      final byte recordType,
      final ORecordCallback<Long> callback) {
    try {
      interruptionManager.enterCriticalPath();
      final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        return doCreateRecord(
            atomicOperation, rid, content, recordVersion, recordType, callback, cluster, null);
      }

      stateLock.acquireReadLock();
      try {
        checkOpennessAndMigration();

        checkIfThreadIsBlocked();

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                doCreateRecord(
                    atomicOperation,
                    rid,
                    content,
                    recordVersion,
                    recordType,
                    callback,
                    cluster,
                    null));
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final ORecordMetadata getRecordMetadata(final ORID rid) {
    try {
      if (rid.isNew()) {
        throw new OStorageException(
            "Passed record with id " + rid + " is new and cannot be stored.");
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OPhysicalPosition ppos =
            cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
        if (ppos == null) {
          return null;
        }

        return new ORecordMetadata(rid, ppos.recordVersion);
      } catch (final IOException ioe) {
        OLogManager.instance()
            .error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
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
    } finally {
      interruptionManager.exitCriticalPath();
    }
  }

  public boolean isDeleted(final ORID rid) {
    try {
      if (rid.isNew()) {
        throw new OStorageException(
            "Passed record with id " + rid + " is new and cannot be stored.");
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return cluster.isDeleted(new OPhysicalPosition(rid.getClusterPosition()));

      } catch (final IOException ioe) {
        OLogManager.instance()
            .error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

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
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();

        final OCluster cluster = doGetAndCheckCluster(clusterId);
        return cluster.nextPage(lastPosition);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public OStorageOperationResult<ORawBuffer> readRecord(
      final ORecordId rid,
      final String iFetchPlan,
      final boolean iIgnoreCache,
      final boolean prefetchRecords,
      final ORecordCallback<ORawBuffer> iCallback) {
    try {
      return new OStorageOperationResult<>(readRecord(rid, prefetchRecords));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(
      final ORecordId rid,
      final String fetchPlan,
      final boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException {
    try {
      return new OStorageOperationResult<>(readRecordIfNotLatest(rid, recordVersion));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final OStorageOperationResult<Integer> updateRecord(
      final ORecordId rid,
      final boolean updateContent,
      final byte[] content,
      final int version,
      final byte recordType,
      @SuppressWarnings("unused") final int mode,
      final ORecordCallback<Integer> callback) {
    try {
      assert transaction.get() == null;

      stateLock.acquireReadLock();
      try {
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        interruptionManager.enterCriticalPath();
        // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
        final Lock lock = recordVersionManager.acquireExclusiveLock(rid);
        try {
          checkOpennessAndMigration();
          checkIfThreadIsBlocked();

          makeStorageDirty();

          final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
          return atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  doUpdateRecord(
                      atomicOperation,
                      rid,
                      updateContent,
                      content,
                      version,
                      recordType,
                      callback,
                      cluster));
        } finally {
          lock.unlock();
        }
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
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
  public final OStorageOperationResult<Boolean> deleteRecord(
      final ORecordId rid,
      final int version,
      final int mode,
      final ORecordCallback<Boolean> callback) {
    try {
      assert transaction.get() == null;

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            null, atomicOperation -> doDeleteRecord(atomicOperation, rid, version, cluster));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public final Set<String> getClusterNames() {
    try {
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return new HashSet<>(clusterMap.keySet());
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      if (clusterName == null) {
        throw new IllegalArgumentException("Cluster name is null");
      }

      if (clusterName.length() == 0) {
        throw new IllegalArgumentException("Cluster name is empty");
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        // SEARCH IT BETWEEN PHYSICAL CLUSTERS

        final OCluster segment =
            clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (segment != null) {
          return segment.getId();
        }

        return -1;
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
   * Scan the given transaction for new record and allocate a record id for them, the relative
   * record id is inserted inside the transaction for future use.
   *
   * @param clientTx the transaction of witch allocate rids
   */
  public void preallocateRids(final OTransactionInternal clientTx) {
    try {
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
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              lockClusters(clustersToLock);

              for (final ORecordOperation txEntry : newRecords) {
                final ORecord rec = txEntry.getRecord();
                if (!rec.getIdentity().isPersistent()) {
                  if (rec.isDirty()) {
                    // This allocate a position for a new record
                    final ORecordId rid = (ORecordId) rec.getIdentity().copy();
                    final ORecordId oldRID = rid.copy();
                    final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
                    final OPhysicalPosition ppos =
                        cluster.allocatePosition(
                            ORecordInternal.getRecordType(rec), atomicOperation);
                    rid.setClusterPosition(ppos.clusterPosition);
                    clientTx.updateIdentityAfterCommit(oldRID, rid);
                  }
                } else {
                  // This allocate position starting from a valid rid, used in distributed for
                  // allocate the same position on other nodes
                  final ORecordId rid = (ORecordId) rec.getIdentity();
                  final OPaginatedCluster cluster =
                      (OPaginatedCluster) doGetAndCheckCluster(rid.getClusterId());
                  OPaginatedCluster.RECORD_STATUS recordStatus =
                      cluster.getRecordStatus(rid.getClusterPosition());
                  if (recordStatus == OPaginatedCluster.RECORD_STATUS.NOT_EXISTENT) {
                    OPhysicalPosition ppos =
                        cluster.allocatePosition(
                            ORecordInternal.getRecordType(rec), atomicOperation);
                    while (ppos.clusterPosition < rid.getClusterPosition()) {
                      ppos =
                          cluster.allocatePosition(
                              ORecordInternal.getRecordType(rec), atomicOperation);
                    }
                    if (ppos.clusterPosition != rid.getClusterPosition()) {
                      throw new OConcurrentCreateException(
                          rid, new ORecordId(rid.getClusterId(), ppos.clusterPosition));
                    }
                  } else if (recordStatus == OPaginatedCluster.RECORD_STATUS.PRESENT
                      || recordStatus == OPaginatedCluster.RECORD_STATUS.REMOVED) {
                    final OPhysicalPosition ppos =
                        cluster.allocatePosition(
                            ORecordInternal.getRecordType(rec), atomicOperation);
                    throw new OConcurrentCreateException(
                        rid, new ORecordId(rid.getClusterId(), ppos.clusterPosition));
                  }
                }
              }
            });
      } catch (final IOException | RuntimeException ioe) {
        throw OException.wrapException(new OStorageException("Could not preallocate RIDs"), ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
   * The commit operation can be run in 3 different conditions, embedded commit, pre-allocated
   * commit, other node commit. <bold>Embedded commit</bold> is the basic commit where the operation
   * is run in embedded or server side, the transaction arrive with invalid rids that get allocated
   * and committed. <bold>pre-allocated commit</bold> is the commit that happen after an
   * preAllocateRids call is done, this is usually run by the coordinator of a tx in distributed.
   * <bold>other node commit</bold> is the commit that happen when a node execute a transaction of
   * another node where all the rids are already allocated in the other node.
   *
   * @param transaction the transaction to commit
   * @param allocated true if the operation is pre-allocated commit
   * @return The list of operations applied by the transaction
   */
  protected List<ORecordOperation> commit(
      final OTransactionInternal transaction, final boolean allocated) {
    // XXX: At this moment, there are two implementations of the commit method. One for regular
    // client transactions and one for
    // implicit micro-transactions. The implementations are quite identical, but operate on slightly
    // different data. If you change
    // this method don't forget to change its counterpart:
    //
    //
    // OAbstractPaginatedStorage.commit(com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction)

    try {
      txBegun.increment();

      final ODatabaseDocumentInternal database = transaction.getDatabase();
      final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
      final TreeMap<String, OTransactionIndexChanges> indexOperations =
          getSortedIndexOperations(transaction);

      database.getMetadata().makeThreadLocalSchemaSnapshot();

      final Collection<ORecordOperation> recordOperations = transaction.getRecordOperations();
      final TreeMap<Integer, OCluster> clustersToLock = new TreeMap<>();
      final Map<ORecordOperation, Integer> clusterOverrides = new IdentityHashMap<>(8);

      final Set<ORecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

      for (final ORecordOperation recordOperation : recordOperations) {
        if (recordOperation.type == ORecordOperation.CREATED
            || recordOperation.type == ORecordOperation.UPDATED) {
          final ORecord record = recordOperation.getRecord();
          if (record instanceof ODocument) {
            ((ODocument) record).validate();
          }
        }

        if (recordOperation.type == ORecordOperation.UPDATED
            || recordOperation.type == ORecordOperation.DELETED) {
          final int clusterId = recordOperation.getRecord().getIdentity().getClusterId();
          clustersToLock.put(clusterId, doGetAndCheckCluster(clusterId));
        } else if (recordOperation.type == ORecordOperation.CREATED) {
          newRecords.add(recordOperation);

          final ORecord record = recordOperation.getRecord();
          final ORID rid = record.getIdentity();

          int clusterId = rid.getClusterId();

          if (record.isDirty()
              && clusterId == ORID.CLUSTER_ID_INVALID
              && record instanceof ODocument) {
            // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS

            final OImmutableClass class_ =
                ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
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
        interruptionManager.enterCriticalPath();
        if (modificationLock) {
          final List<ORID> recordLocks = new ArrayList<>();
          for (final ORecordOperation recordOperation : recordOperations) {
            if (recordOperation.type == ORecordOperation.UPDATED
                || recordOperation.type == ORecordOperation.DELETED) {
              recordLocks.add(recordOperation.getRID());
            }
          }
          final Set<ORID> locked = transaction.getLockedRecords();
          if (locked != null) {
            recordLocks.removeAll(locked);
          }
          Collections.sort(recordLocks);
          for (final ORID rid : recordLocks) {
            acquireWriteLock(rid);
          }
        }
        try {
          checkOpennessAndMigration();

          checkIfThreadIsBlocked();

          makeStorageDirty();

          Throwable error = null;
          startStorageTx(transaction);
          try {
            final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
            lockClusters(clustersToLock);

            final Map<ORecordOperation, OPhysicalPosition> positions = new IdentityHashMap<>(8);
            for (final ORecordOperation recordOperation : newRecords) {
              final ORecord rec = recordOperation.getRecord();

              if (allocated) {
                if (rec.getIdentity().isPersistent()) {
                  positions.put(
                      recordOperation,
                      new OPhysicalPosition(rec.getIdentity().getClusterPosition()));
                } else {
                  throw new OStorageException(
                      "Impossible to commit a transaction with not valid rid in pre-allocated commit");
                }
              } else if (rec.isDirty() && !rec.getIdentity().isPersistent()) {
                final ORecordId rid = (ORecordId) rec.getIdentity().copy();
                final ORecordId oldRID = rid.copy();

                final Integer clusterOverride = clusterOverrides.get(recordOperation);
                final int clusterId =
                    Optional.ofNullable(clusterOverride).orElseGet(rid::getClusterId);

                final OCluster cluster = doGetAndCheckCluster(clusterId);

                OPhysicalPosition physicalPosition =
                    cluster.allocatePosition(ORecordInternal.getRecordType(rec), atomicOperation);
                rid.setClusterId(cluster.getId());

                if (rid.getClusterPosition() > -1) {
                  // CREATE EMPTY RECORDS UNTIL THE POSITION IS REACHED. THIS IS THE CASE WHEN A
                  // SERVER IS OUT OF SYNC
                  // BECAUSE A TRANSACTION HAS BEEN ROLLED BACK BEFORE TO SEND THE REMOTE CREATES.
                  // SO THE OWNER NODE DELETED
                  // RECORD HAVING A HIGHER CLUSTER POSITION
                  while (rid.getClusterPosition() > physicalPosition.clusterPosition) {
                    physicalPosition =
                        cluster.allocatePosition(
                            ORecordInternal.getRecordType(rec), atomicOperation);
                  }

                  if (rid.getClusterPosition() != physicalPosition.clusterPosition) {
                    throw new OConcurrentCreateException(
                        rid, new ORecordId(rid.getClusterId(), physicalPosition.clusterPosition));
                  }
                }
                positions.put(recordOperation, physicalPosition);
                rid.setClusterPosition(physicalPosition.clusterPosition);
                transaction.updateIdentityAfterCommit(oldRID, rid);
              }
            }
            lockRidBags(clustersToLock, indexOperations, indexManager, database);

            for (final ORecordOperation recordOperation : recordOperations) {
              commitEntry(
                  atomicOperation,
                  recordOperation,
                  positions.get(recordOperation),
                  database.getSerializer());
              result.add(recordOperation);
            }
            lockIndexes(indexOperations);

            commitIndexes(indexOperations);
          } catch (final IOException | RuntimeException e) {
            error = e;
            if (e instanceof RuntimeException) {
              throw ((RuntimeException) e);
            } else {
              throw OException.wrapException(
                  new OStorageException("Error during transaction commit"), e);
            }
          } finally {
            if (error != null) {
              rollback(transaction, error);
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
          if (modificationLock) {
            final List<ORID> recordLocks = new ArrayList<>();
            for (final ORecordOperation recordOperation : recordOperations) {
              if (recordOperation.type == ORecordOperation.UPDATED
                  || recordOperation.type == ORecordOperation.DELETED) {
                recordLocks.add(recordOperation.getRID());
              }
            }

            final Set<ORID> locked = transaction.getLockedRecords();
            if (locked != null) {
              recordLocks.removeAll(locked);
            }
            for (final ORID rid : recordLocks) {
              releaseWriteLock(rid);
            }
          }
        } finally {
          stateLock.releaseReadLock();
          interruptionManager.exitCriticalPath();
        }
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance()
            .debug(
                this,
                "%d Committed transaction %d on database '%s' (result=%s)",
                Thread.currentThread().getId(),
                transaction.getId(),
                database.getName(),
                result);
      }
      return result;
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      atomicOperationsManager.alarmClearOfAtomicOperation();
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void commitIndexes(final Map<String, OTransactionIndexChanges> indexesToCommit) {
    for (final OTransactionIndexChanges changes : indexesToCommit.values()) {
      final OIndexInternal index = changes.getAssociatedIndex();
      if (!index.isNativeTxSupported()) {
        final OIndexAbstract.IndexTxSnapshot snapshot = new OIndexAbstract.IndexTxSnapshot();
        index.addTxOperation(snapshot, changes);
        index.commit(snapshot);
        applyUniqueIndexChanges(index.getName(), changes);
      } else {
        try {
          final int indexId = index.getIndexId();
          if (changes.cleared) {
            clearIndex(indexId);
          }
          for (final OTransactionIndexChangesPerKey changesPerKey :
              changes.changesPerKey.values()) {
            applyTxChanges(changesPerKey, index);
          }
          applyTxChanges(changes.nullKeyChanges, index);
        } catch (final OInvalidIndexEngineIdException e) {
          throw OException.wrapException(new OStorageException("Error during index commit"), e);
        }
      }
    }
  }

  private void applyTxChanges(OTransactionIndexChangesPerKey changes, OIndexInternal index)
      throws OInvalidIndexEngineIdException {
    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry op :
        index.interpretTxKeyChanges(changes)) {
      switch (op.getOperation()) {
        case PUT:
          index.doPut(this, changes.key, op.getValue().getIdentity());
          break;
        case REMOVE:
          if (op.getValue() != null) index.doRemove(this, changes.key, op.getValue().getIdentity());
          else index.doRemove(this, changes.key);
          break;
        case CLEAR:
          // SHOULD NEVER BE THE CASE HANDLE BY cleared FLAG
          break;
      }
      applyUniqueIndexChange(index.getName(), changes.key);
    }
  }

  public int loadIndexEngine(final String name) {
    try {
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OBaseIndexEngine engine = indexEngineNameMap.get(name);
        if (engine == null) {
          return -1;
        }
        final int indexId = indexEngines.indexOf(engine);
        assert indexId == engine.getId();
        return generateIndexId(indexId, engine);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public int loadExternalIndexEngine(
      final String engineName,
      final String algorithm,
      final String indexType,
      final OIndexDefinition indexDefinition,
      final OBinarySerializer<?> valueSerializer,
      final boolean isAutomatic,
      final Boolean durableInNonTxMode,
      final int version,
      final int apiVersion,
      final boolean multivalue,
      final Map<String, String> engineProperties) {
    try {
      stateLock.acquireWriteLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        // this method introduced for binary compatibility only
        if (configuration.getBinaryFormatVersion() > 15) {
          return -1;
        }
        if (indexEngineNameMap.containsKey(engineName)) {
          throw new OIndexException("Index with name " + engineName + " already exists");
        }
        makeStorageDirty();

        final OBinarySerializer<?> keySerializer = determineKeySerializer(indexDefinition);
        if (keySerializer == null) {
          throw new OIndexException("Can not determine key serializer");
        }
        final int keySize = determineKeySize(indexDefinition);
        final OType[] keyTypes =
            Optional.of(indexDefinition).map(OIndexDefinition::getTypes).orElse(null);
        final boolean nullValuesSupport = !indexDefinition.isNullValuesIgnored();
        final OBaseIndexEngine engine =
            OIndexes.createIndexEngine(
                indexEngines.size(),
                engineName,
                algorithm,
                indexType,
                durableInNonTxMode,
                this,
                version,
                apiVersion,
                multivalue,
                engineProperties);
        final OStorageConfiguration.IndexEngineData engineData =
            new OStorageConfigurationImpl.IndexEngineData(
                engine.getId(),
                engineName,
                algorithm,
                indexType,
                durableInNonTxMode,
                version,
                engine.getEngineAPIVersion(),
                multivalue,
                valueSerializer.getId(),
                keySerializer.getId(),
                isAutomatic,
                keyTypes,
                nullValuesSupport,
                keySize,
                null,
                null,
                engineProperties);

        if (engineData.getApiVersion() < 1) {
          ((OIndexEngine) engine)
              .load(
                  engineName,
                  valueSerializer,
                  isAutomatic,
                  keySerializer,
                  keyTypes,
                  nullValuesSupport,
                  keySize,
                  engineData.getEngineProperties(),
                  null);
        } else {
          ((OV1IndexEngine) engine).load(engineName, keySize, keyTypes, keySerializer, null);
        }
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              indexEngineNameMap.put(engineName, engine);
              indexEngines.add(engine);
              ((OClusterBasedStorageConfiguration) configuration)
                  .addIndexEngine(atomicOperation, engineName, engineData);
            });
        return generateIndexId(indexEngines.size() - 1, engine);
      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
      } finally {
        stateLock.releaseWriteLock();
        interruptionManager.exitCriticalPath();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public int addIndexEngine(
      final String engineName,
      final String algorithm,
      final String indexType,
      final OIndexDefinition indexDefinition,
      final OBinarySerializer<?> valueSerializer,
      final boolean isAutomatic,
      final Boolean durableInNonTxMode,
      final int version,
      final int apiVersion,
      final boolean multivalue,
      final Map<String, String> engineProperties,
      final Set<String> clustersToIndex,
      final ODocument metadata) {
    try {
      if (indexDefinition == null) {
        throw new OIndexException("Index definition hav to be provided");
      }
      final OType[] keyTypes = indexDefinition.getTypes();
      if (keyTypes == null) {
        throw new OIndexException("Types of indexed keys have to be provided");
      }

      stateLock.acquireWriteLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation -> {
              if (indexEngineNameMap.containsKey(engineName)) {
                // OLD INDEX FILE ARE PRESENT: THIS IS THE CASE OF PARTIAL/BROKEN INDEX
                OLogManager.instance()
                    .warn(
                        this,
                        "Index with name '%s' already exists, removing it and re-create the index",
                        engineName);
                final OBaseIndexEngine engine = indexEngineNameMap.remove(engineName);
                if (engine != null) {
                  indexEngines.set(engine.getId(), null);

                  engine.delete(atomicOperation);
                  ((OClusterBasedStorageConfiguration) configuration)
                      .deleteIndexEngine(atomicOperation, engineName);
                }
              }

              final OBinarySerializer<?> keySerializer = determineKeySerializer(indexDefinition);
              if (keySerializer == null) {
                throw new OIndexException("Can not determine key serializer");
              }

              final int keySize = determineKeySize(indexDefinition);

              final boolean nullValuesSupport = !indexDefinition.isNullValuesIgnored();
              final byte serializerId;

              if (valueSerializer != null) {
                serializerId = valueSerializer.getId();
              } else {
                serializerId = -1;
              }

              // this property is used for autosharded index
              if (metadata != null && metadata.containsField("partitions")) {
                engineProperties.put("partitions", metadata.field("partitions"));
              } else {
                engineProperties.put("partitions", Integer.toString(clustersToIndex.size()));
              }

              final OBaseIndexEngine engine =
                  addIndexEngineInternal(
                      atomicOperation,
                      engineName,
                      algorithm,
                      indexType,
                      valueSerializer,
                      isAutomatic,
                      durableInNonTxMode,
                      version,
                      apiVersion,
                      multivalue,
                      engineProperties,
                      keySerializer,
                      keySize,
                      keyTypes,
                      nullValuesSupport);

              final OContextConfiguration ctxCfg = configuration.getContextConfiguration();
              @SuppressWarnings("deprecation")
              final String cfgEncryption =
                  ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
              final String cfgEncryptionKey =
                  ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

              final OStorageConfiguration.IndexEngineData engineData =
                  new OStorageConfigurationImpl.IndexEngineData(
                      engine.getId(),
                      engineName,
                      algorithm,
                      indexType,
                      durableInNonTxMode,
                      version,
                      engine.getEngineAPIVersion(),
                      multivalue,
                      serializerId,
                      keySerializer.getId(),
                      isAutomatic,
                      keyTypes,
                      nullValuesSupport,
                      keySize,
                      cfgEncryption,
                      cfgEncryptionKey,
                      engineProperties);
              ((OClusterBasedStorageConfiguration) configuration)
                  .addIndexEngine(atomicOperation, engineName, engineData);

              if (multivalue
                      && (engine instanceof OSBTreeIndexEngine
                          || engine instanceof OHashTableIndexEngine)
                  || engine instanceof OAutoShardingIndexEngine) {
                final OSBTreeBonsaiLocal<OIdentifiable, Boolean> tree =
                    new OSBTreeBonsaiLocal<>(
                        engineName, OIndexRIDContainerSBTree.INDEX_FILE_EXTENSION, this);
                tree.createComponent(atomicOperation);
              }
              return generateIndexId(indexEngines.size() - 1, engine);
            });
      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Cannot add index engine " + engineName + " in storage."), e);
      } finally {
        stateLock.releaseWriteLock();
        interruptionManager.exitCriticalPath();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private OBaseIndexEngine addIndexEngineInternal(
      final OAtomicOperation atomicOperation,
      final String engineName,
      final String algorithm,
      final String indexType,
      final OBinarySerializer<?> valueSerializer,
      final boolean isAutomatic,
      final Boolean durableInNonTxMode,
      final int version,
      final int apiVersion,
      final boolean multivalue,
      final Map<String, String> engineProperties,
      final OBinarySerializer<?> keySerializer,
      final int keySize,
      final OType[] keyTypes,
      final boolean nullValuesSupport)
      throws IOException {
    final OBaseIndexEngine engine;
    engine =
        OIndexes.createIndexEngine(
            indexEngines.size(),
            engineName,
            algorithm,
            indexType,
            durableInNonTxMode,
            this,
            version,
            apiVersion,
            multivalue,
            engineProperties);
    final OContextConfiguration ctxCfg = configuration.getContextConfiguration();
    @SuppressWarnings("deprecation")
    final String cfgEncryption =
        ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
    final String cfgEncryptionKey =
        ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

    final OEncryption encryption;
    if (cfgEncryption == null || cfgEncryption.equals(ONothingEncryption.NAME)) {
      encryption = null;
    } else {
      encryption = OEncryptionFactory.INSTANCE.getEncryption(cfgEncryption, cfgEncryptionKey);
    }
    engine.create(
        atomicOperation,
        valueSerializer,
        isAutomatic,
        keyTypes,
        nullValuesSupport,
        keySerializer,
        keySize,
        engineProperties,
        encryption);
    indexEngineNameMap.put(engineName, engine);
    indexEngines.add(engine);
    return engine;
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
    if (indexDefinition == null) {
      throw new OStorageException("Index definition has to be provided");
    }

    final OType[] keyTypes = indexDefinition.getTypes();
    if (keyTypes == null || keyTypes.length == 0) {
      throw new OStorageException("Types of index keys has to be defined");
    }
    if (keyTypes.length < indexDefinition.getFields().size()) {
      throw new OStorageException(
          "Types are provided only for "
              + keyTypes.length
              + " fields. But index definition has "
              + indexDefinition.getFields().size()
              + " fields.");
    }

    final OBinarySerializer<?> keySerializer;
    if (indexDefinition.getTypes().length > 1) {
      keySerializer = OCompositeKeySerializer.INSTANCE;
    } else {
      final OType keyType = indexDefinition.getTypes()[0];

      if (keyType == OType.STRING && configuration.getBinaryFormatVersion() >= 13) {
        return OUTF8Serializer.INSTANCE;
      }

      final OCurrentStorageComponentsFactory currentStorageComponentsFactory = componentsFactory;
      if (currentStorageComponentsFactory != null) {
        keySerializer =
            currentStorageComponentsFactory.binarySerializerFactory.getObjectSerializer(keyType);
      } else {
        throw new IllegalStateException(
            "Cannot load binary serializer, storage is not properly initialized");
      }
    }

    return keySerializer;
  }

  public void deleteIndexEngine(int indexId) throws OInvalidIndexEngineIdException {
    final int internalIndexId = extractInternalId(indexId);

    try {
      stateLock.acquireWriteLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        checkIndexId(internalIndexId);

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              final OBaseIndexEngine engine =
                  deleteIndexEngineInternal(atomicOperation, internalIndexId);
              final String engineName = engine.getName();

              final OStorageConfiguration.IndexEngineData engineData =
                  configuration.getIndexEngine(engineName, internalIndexId);
              ((OClusterBasedStorageConfiguration) configuration)
                  .deleteIndexEngine(atomicOperation, engineName);

              if (engineData.isMultivalue()
                  && (engine instanceof OSBTreeIndexEngine
                      || engine instanceof OHashTableIndexEngine
                      || engine instanceof OAutoShardingIndexEngine)) {
                final OSBTreeBonsaiLocal<OIdentifiable, Boolean> tree =
                    new OSBTreeBonsaiLocal<>(
                        engineName, OIndexRIDContainerSBTree.INDEX_FILE_EXTENSION, this);
                tree.deleteComponent(atomicOperation);
              }
            });

      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error on index deletion"), e);
      } finally {
        stateLock.releaseWriteLock();
        interruptionManager.exitCriticalPath();
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

  private OBaseIndexEngine deleteIndexEngineInternal(
      final OAtomicOperation atomicOperation, final int indexId) throws IOException {
    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();
    indexEngines.set(indexId, null);
    engine.delete(atomicOperation);

    final String engineName = engine.getName();
    indexEngineNameMap.remove(engineName);
    return engine;
  }

  private void checkIndexId(final int indexId) throws OInvalidIndexEngineIdException {
    if (indexId < 0 || indexId >= indexEngines.size() || indexEngines.get(indexId) == null) {
      throw new OInvalidIndexEngineIdException(
          "Engine with id " + indexId + " is not registered inside of storage");
    }
  }

  public boolean removeKeyFromIndex(final int indexId, final Object key)
      throws OInvalidIndexEngineIdException {
    final int internalIndexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        return removeKeyFromIndexInternal(atomicOperation, internalIndexId, key);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation -> removeKeyFromIndexInternal(atomicOperation, internalIndexId, key));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private boolean removeKeyFromIndexInternal(
      final OAtomicOperation atomicOperation, final int indexId, final Object key)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      if (engine.getEngineAPIVersion() == OIndexEngine.VERSION) {
        return ((OIndexEngine) engine).remove(atomicOperation, key);
      } else {
        final OV1IndexEngine v1IndexEngine = (OV1IndexEngine) engine;
        if (!v1IndexEngine.isMultiValue()) {
          return ((OSingleValueIndexEngine) engine).remove(atomicOperation, key);
        } else {
          throw new OStorageException(
              "To remove entry from multi-value index not only key but value also should be provided");
        }
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during removal of entry with key " + key + " from index "),
          e);
    }
  }

  public void clearIndex(final int indexId) throws OInvalidIndexEngineIdException {
    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        doClearIndex(atomicOperation, indexId);
        return;
      }

      final int internalIndexId = extractInternalId(indexId);
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> doClearIndex(atomicOperation, internalIndexId));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private void doClearIndex(final OAtomicOperation atomicOperation, final int indexId)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      assert indexId == engine.getId();

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

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doGetIndexValue(indexId, key);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Object doGetIndexValue(final int indexId, final Object key)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    if (engineAPIVersion != 0) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 0 but found " + engineAPIVersion);
    }
    checkIndexId(indexId);
    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();
    return ((OIndexEngine) engine).get(key);
  }

  public Stream<ORID> getIndexValues(int indexId, final Object key)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    indexId = extractInternalId(indexId);

    try {
      interruptionManager.enterCriticalPath();
      if (transaction.get() != null) {
        return doGetIndexValues(indexId, key);
      }

      stateLock.acquireReadLock();
      try {
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doGetIndexValues(indexId, key);
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
    } finally {
      interruptionManager.exitCriticalPath();
    }
  }

  private Stream<ORID> doGetIndexValues(final int indexId, final Object key)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return ((OV1IndexEngine) engine).get(key);
  }

  public OBaseIndexEngine getIndexEngine(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      checkIndexId(indexId);

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OBaseIndexEngine engine = indexEngines.get(indexId);
        assert indexId == engine.getId();
        return engine;
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  public void updateIndexEntry(
      int indexId, final Object key, final OIndexKeyUpdater<Object> valueCreator)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalIndexId = extractInternalId(indexId);

    if (engineAPIVersion != 0) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 0 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        assert atomicOperation != null;
        doUpdateIndexEntry(atomicOperation, indexId, key, valueCreator);
        return;
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                doUpdateIndexEntry(atomicOperation, internalIndexId, key, valueCreator));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  public <T> T callIndexEngine(
      final boolean readOperation, int indexId, final OIndexEngineCallback<T> callback)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        if (readOperation) {
          makeStorageDirty();
        }

        return doCallIndexEngine(indexId, callback);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private <T> T doCallIndexEngine(final int indexId, final OIndexEngineCallback<T> callback)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);

    return callback.callEngine(engine);
  }

  private void doUpdateIndexEntry(
      final OAtomicOperation atomicOperation,
      final int indexId,
      final Object key,
      final OIndexKeyUpdater<Object> valueCreator)
      throws OInvalidIndexEngineIdException, IOException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    ((OIndexEngine) engine).update(atomicOperation, key, valueCreator);
  }

  public void putRidIndexEntry(int indexId, final Object key, final ORID value)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalIndexId = extractInternalId(indexId);

    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        assert atomicOperation != null;
        putRidIndexEntryInternal(atomicOperation, internalIndexId, key, value);
        return;
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                putRidIndexEntryInternal(atomicOperation, internalIndexId, key, value));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private void putRidIndexEntryInternal(
      final OAtomicOperation atomicOperation, final int indexId, final Object key, final ORID value)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert engine.getId() == indexId;

    ((OV1IndexEngine) engine).put(atomicOperation, key, value);
  }

  public boolean removeRidIndexEntry(int indexId, final Object key, final ORID value)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalIndexId = extractInternalId(indexId);

    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        assert atomicOperation != null;
        return removeRidIndexEntryInternal(atomicOperation, internalIndexId, key, value);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                removeRidIndexEntryInternal(atomicOperation, internalIndexId, key, value));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private boolean removeRidIndexEntryInternal(
      final OAtomicOperation atomicOperation, final int indexId, final Object key, final ORID value)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert engine.getId() == indexId;

    return ((OMultiValueIndexEngine) engine).remove(atomicOperation, key, value);
  }

  public void putIndexValue(int indexId, final Object key, final Object value)
      throws OInvalidIndexEngineIdException {
    final int engineAPIVersion = extractEngineAPIVersion(indexId);
    final int internalIndexId = extractInternalId(indexId);

    if (engineAPIVersion != 0) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 0 but found " + engineAPIVersion);
    }

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        assert atomicOperation != null;
        putIndexValueInternal(atomicOperation, indexId, key, value);
        return;
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> putIndexValueInternal(atomicOperation, internalIndexId, key, value));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private void putIndexValueInternal(
      OAtomicOperation atomicOperation, final int indexId, final Object key, final Object value)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      assert engine.getId() == indexId;

      ((OIndexEngine) engine).put(atomicOperation, key, value);
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException(
              "Cannot put key " + key + " value " + value + " entry to the index"),
          e);
    }
  }

  /**
   * Puts the given value under the given key into this storage for the index with the given index
   * id. Validates the operation using the provided validator.
   *
   * @param indexId the index id of the index to put the value into.
   * @param key the key to put the value under.
   * @param value the value to put.
   * @param validator the operation validator.
   * @return {@code true} if the validator allowed the put, {@code false} otherwise.
   * @see OBaseIndexEngine.Validator#validate(Object, Object, Object)
   */
  @SuppressWarnings("UnusedReturnValue")
  public boolean validatedPutIndexValue(
      final int indexId,
      final Object key,
      final ORID value,
      final OBaseIndexEngine.Validator<Object, ORID> validator)
      throws OInvalidIndexEngineIdException {
    final int internalIndexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        assert atomicOperation != null;
        return doValidatedPutIndexValue(atomicOperation, internalIndexId, key, value, validator);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                doValidatedPutIndexValue(atomicOperation, internalIndexId, key, value, validator));
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private boolean doValidatedPutIndexValue(
      OAtomicOperation atomicOperation,
      final int indexId,
      final Object key,
      final ORID value,
      final OBaseIndexEngine.Validator<Object, ORID> validator)
      throws OInvalidIndexEngineIdException {
    try {
      checkIndexId(indexId);

      final OBaseIndexEngine engine = indexEngines.get(indexId);
      assert indexId == engine.getId();

      if (engine instanceof OIndexEngine) {
        return ((OIndexEngine) engine).validatedPut(atomicOperation, key, value, validator);
      }

      if (engine instanceof OSingleValueIndexEngine) {
        return ((OSingleValueIndexEngine) engine)
            .validatedPut(atomicOperation, key, value.getIdentity(), validator);
      }

      throw new IllegalStateException(
          "Invalid type of index engine " + engine.getClass().getName());
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException(
              "Cannot put key " + key + " value " + value + " entry to the index"),
          e);
    }
  }

  public Stream<ORawPair<Object, ORID>> iterateIndexEntriesBetween(
      int indexId,
      final Object rangeFrom,
      final boolean fromInclusive,
      final Object rangeTo,
      final boolean toInclusive,
      final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIterateIndexEntriesBetween(
            indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doIterateIndexEntriesBetween(
            indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Stream<ORawPair<Object, ORID>> doIterateIndexEntriesBetween(
      final int indexId,
      final Object rangeFrom,
      final boolean fromInclusive,
      final Object rangeTo,
      final boolean toInclusive,
      final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.iterateEntriesBetween(
        rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  public Stream<ORawPair<Object, ORID>> iterateIndexEntriesMajor(
      int indexId,
      final Object fromKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Stream<ORawPair<Object, ORID>> doIterateIndexEntriesMajor(
      final int indexId,
      final Object fromKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  public Stream<ORawPair<Object, ORID>> iterateIndexEntriesMinor(
      int indexId,
      final Object toKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Stream<ORawPair<Object, ORID>> doIterateIndexEntriesMinor(
      final int indexId,
      final Object toKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  public Stream<ORawPair<Object, ORID>> getIndexStream(
      int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexStream(indexId, valuesTransformer);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doGetIndexStream(indexId, valuesTransformer);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Stream<ORawPair<Object, ORID>> doGetIndexStream(
      final int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.stream(valuesTransformer);
  }

  public Stream<ORawPair<Object, ORID>> getIndexDescStream(
      int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexDescStream(indexId, valuesTransformer);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doGetIndexDescStream(indexId, valuesTransformer);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Stream<ORawPair<Object, ORID>> doGetIndexDescStream(
      final int indexId, final OBaseIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.descStream(valuesTransformer);
  }

  public Stream<Object> getIndexKeyStream(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexKeyStream(indexId);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doGetIndexKeyStream(indexId);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private Stream<Object> doGetIndexKeyStream(final int indexId)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.keyStream();
  }

  public long getIndexSize(int indexId, final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doGetIndexSize(indexId, transformer);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doGetIndexSize(indexId, transformer);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  private long doGetIndexSize(
      final int indexId, final OBaseIndexEngine.ValuesTransformer transformer)
      throws OInvalidIndexEngineIdException {
    checkIndexId(indexId);

    final OBaseIndexEngine engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.size(transformer);
  }

  public boolean hasIndexRangeQuerySupport(int indexId) throws OInvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      if (transaction.get() != null) {
        return doHasRangeQuerySupport(indexId);
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return doHasRangeQuerySupport(indexId);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
    assert indexId == engine.getId();

    return engine.hasRangeQuerySupport();
  }

  private void rollback(final OTransactionInternal clientTx, final Throwable error)
      throws IOException {
    assert transaction.get() != null;
    atomicOperationsManager.endAtomicOperation(error);

    assert atomicOperationsManager.getCurrentOperation() == null;

    OTransactionAbstract.updateCacheFromEntries(
        clientTx.getDatabase(), clientTx.getRecordOperations(), false);

    txRollback.increment();
  }

  public void moveToErrorStateIfNeeded(final Throwable error) {
    if (error != null
        && !((error instanceof OHighLevelException) || (error instanceof ONeedRetryException))) {
      this.error = error;
      status = STATUS.INTERNAL_ERROR;
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        final long timer = Orient.instance().getProfiler().startChrono();
        final long lockId = atomicOperationsManager.freezeAtomicOperations(null, null);
        try {
          checkOpennessAndMigration();
          checkIfThreadIsBlocked();

          if (status != STATUS.INTERNAL_ERROR) {
            for (final OBaseIndexEngine indexEngine : indexEngines) {
              try {
                if (indexEngine != null) {
                  indexEngine.flush();
                }
              } catch (final Throwable t) {
                OLogManager.instance()
                    .error(
                        this,
                        "Error while flushing index via index engine of class %s.",
                        t,
                        indexEngine.getClass().getSimpleName());
              }
            }

            flushAllData();

          } else {
            OLogManager.instance()
                .errorNoDb(
                    this,
                    "Sync can not be performed because of internal error in storage %s",
                    null,
                    this.name);
          }

        } finally {
          atomicOperationsManager.releaseAtomicOperations(lockId);
          Orient.instance()
              .getProfiler()
              .stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
        }
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

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
  public String getClusterName(int clusterId) {
    stateLock.acquireReadLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      if (clusterId == ORID.CLUSTER_ID_INVALID) {
        clusterId = defaultClusterId;
      }

      return doGetAndCheckCluster(clusterId).getName();

    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseReadLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final long getSize() {
    try {
      try {
        long size = 0;

        stateLock.acquireReadLock();
        try {
          interruptionManager.enterCriticalPath();
          checkOpennessAndMigration();
          checkIfThreadIsBlocked();

          for (final OCluster c : clusters) {
            if (c != null) {
              size += c.getRecordsSize();
            }
          }
        } finally {
          stateLock.releaseReadLock();
          interruptionManager.exitCriticalPath();
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
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        return clusterMap.size();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public final Set<OCluster> getClusterInstances() {
    try {
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final Set<OCluster> result = new HashSet<>(1024);

        // ADD ALL THE CLUSTERS
        for (final OCluster c : clusters) {
          if (c != null) {
            result.add(c);
          }
        }

        return result;

      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public final boolean cleanOutRecord(
      final ORecordId recordId,
      final int recordVersion,
      final int iMode,
      final ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, iMode, callback).getResult();
  }

  @Override
  public final void freeze(final boolean throwException) {
    try {
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        if (throwException) {
          atomicOperationsManager.freezeAtomicOperations(
              OModificationOperationProhibitedException.class,
              "Modification requests are prohibited");
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

          throw OException.wrapException(
              new OStorageException("Error on freeze of storage '" + name + "'"), e);
        }

        synch();
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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

  /** @inheritDoc */
  @Override
  public final void pageIsBroken(final String fileName, final long pageIndex) {
    stateLock.acquireWriteLock();
    try {
      OLogManager.instance()
          .errorStorage(
              this,
              "In storage %s file with name '%s' has broken page under the index %d",
              null,
              name,
              fileName,
              pageIndex);

      if (status == STATUS.CLOSED) {
        return;
      }

      this.error = new OStorageException("Page " + pageIndex + " is broken in file " + fileName);
      status = STATUS.INTERNAL_ERROR;

      try {
        makeStorageDirty();
      } catch (final IOException e) {
        // ignore
      }
    } finally {
      stateLock.releaseWriteLock();
    }
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

  /** Executes the command request and return the result back. */
  @Override
  public final Object command(final OCommandRequestText iCommand) {
    try {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      while (true) {
        try {
          final OCommandExecutor executor =
              db.getSharedContext()
                  .getOrientDB()
                  .getScriptManager()
                  .getCommandManager()
                  .getExecutor(iCommand);
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
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  public final Object executeCommand(
      final OCommandRequestText iCommand, final OCommandExecutor executor) {
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

        // EXECUTE THE COMMAND
        final Map<Object, Object> params = iCommand.getParameters();
        Object result = executor.execute(params);

        // CALL AFTER COMMAND
        for (final ODatabaseListener oDatabaseListener : listeners) {
          oDatabaseListener.onAfterCommand(iCommand, executor, result);
        }

        return result;

      } catch (final OException e) {
        // PASS THROUGH
        throw e;
      } catch (final Exception e) {
        throw OException.wrapException(
            new OCommandExecutionException("Error on execution of command: " + iCommand), e);

      } finally {
        if (Orient.instance().getProfiler().isRecording()) {
          final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
          if (db != null) {
            final OSecurityUser user = db.getUser();
            final String userString = Optional.ofNullable(user).map(Object::toString).orElse(null);
            Orient.instance()
                .getProfiler()
                .stopChrono(
                    "db."
                        + ODatabaseRecordThreadLocal.instance().get().getName()
                        + ".command."
                        + iCommand,
                    "Command executed against the database",
                    beginTime,
                    "db.*.command.*",
                    null,
                    userString);
          }
        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final OPhysicalPosition[] higherPhysicalPositions(
      final int currentClusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1) {
        return new OPhysicalPosition[0];
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OCluster cluster = doGetAndCheckCluster(currentClusterId);
        return cluster.higherPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException.wrapException(
            new OStorageException(
                "Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''),
            ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public final OPhysicalPosition[] ceilingPhysicalPositions(
      final int clusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1) {
        return new OPhysicalPosition[0];
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OCluster cluster = doGetAndCheckCluster(clusterId);
        return cluster.ceilingPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException.wrapException(
            new OStorageException(
                "Cluster Id " + clusterId + " is invalid in storage '" + name + '\''),
            ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public final OPhysicalPosition[] lowerPhysicalPositions(
      final int currentClusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1) {
        return new OPhysicalPosition[0];
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OCluster cluster = doGetAndCheckCluster(currentClusterId);

        return cluster.lowerPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException.wrapException(
            new OStorageException(
                "Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''),
            ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
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
  public final OPhysicalPosition[] floorPhysicalPositions(
      final int clusterId, final OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1) {
        return new OPhysicalPosition[0];
      }

      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
        checkOpennessAndMigration();
        checkIfThreadIsBlocked();

        final OCluster cluster = doGetAndCheckCluster(clusterId);

        return cluster.floorPositions(physicalPosition);
      } catch (final IOException ioe) {
        throw OException.wrapException(
            new OStorageException(
                "Cluster Id " + clusterId + " is invalid in storage '" + name + '\''),
            ioe);
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void acquireWriteLock(final ORID rid, final long timeout) {
    if (!modificationLock) {
      throw new ODatabaseException(
          "Record write locks are off by configuration, set the configuration \"storage.pessimisticLock\" to \""
              + OrientDBConfig.LOCK_TYPE_READWRITE
              + "\" for enable them");
    }

    try {
      lockManager.acquireWriteLock(rid, timeout);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final void acquireWriteLock(final ORID rid) {
    if (!modificationLock) {
      throw new ODatabaseException(
          "Record write locks are off by configuration, set the configuration \"storage.pessimisticLock\" to \""
              + OrientDBConfig.LOCK_TYPE_MODIFICATION
              + "\" for enable them");
    }

    try {
      lockManager.acquireWriteLock(rid, 0);
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
      lockManager.releaseWriteLock(rid);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public final void acquireReadLock(final ORID rid) {
    if (!readLock) {
      throw new ODatabaseException(
          "Record read locks are off by configuration, set the configuration \"storage.pessimisticLock\" to \""
              + OrientDBConfig.LOCK_TYPE_READWRITE
              + "\" for enable them");
    }

    try {
      lockManager.acquireReadLock(rid.copy(), 0);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public void acquireReadLock(final ORID rid, final long timeout) {
    if (!readLock) {
      throw new ODatabaseException(
          "Record read locks are off by configuration, set the configuration \"storage.pessimisticLock\" to \""
              + OrientDBConfig.LOCK_TYPE_READWRITE
              + "\" for enable them");
    }

    try {
      lockManager.acquireReadLock(rid, timeout);
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
      lockManager.releaseReadLock(rid);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final ORecordConflictStrategy getRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  @Override
  public final void setConflictStrategy(final ORecordConflictStrategy conflictResolver) {
    Objects.requireNonNull(conflictResolver);
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> doSetConflictStrategy(conflictResolver, atomicOperation));
    } catch (final Exception e) {
      throw OException.wrapException(
          new OStorageException(
              "Exception during setting of conflict strategy "
                  + conflictResolver.getName()
                  + " for storage "
                  + name),
          e);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  private void doSetConflictStrategy(
      ORecordConflictStrategy conflictResolver, OAtomicOperation atomicOperation) {

    if (recordConflictStrategy == null
        || !recordConflictStrategy.getName().equals(conflictResolver.getName())) {

      this.recordConflictStrategy = conflictResolver;
      ((OClusterBasedStorageConfiguration) configuration)
          .setConflictStrategy(atomicOperation, conflictResolver.getName());
    }
  }

  @SuppressWarnings("unused")
  public long getRecordScanned() {
    return recordScanned.value;
  }

  @SuppressWarnings("unused")
  protected abstract OLogSequenceNumber copyWALToIncrementalBackup(
      ZipOutputStream zipOutputStream, long startSegment) throws IOException;

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
  protected abstract void addFileToDirectory(String name, InputStream stream, File directory)
      throws IOException;

  @SuppressWarnings("unused")
  protected abstract OWriteAheadLog createWalFromIBUFiles(
      File directory,
      final OContextConfiguration contextConfiguration,
      final Locale locale,
      byte[] iv)
      throws IOException;

  /** Checks if the storage is open. If it's closed an exception is raised. */
  protected final void checkOpennessAndMigration() {
    checkErrorState();

    final STATUS status = this.status;

    if (status == STATUS.MIGRATION) {
      throw new OStorageException(
          "Storage data are under migration procedure, please wait till data will be migrated.");
    }

    if (status != STATUS.OPEN) {
      throw new OStorageException("Storage " + name + " is not opened.");
    }
  }

  public void checkErrorState() {
    if (status == STATUS.INTERNAL_ERROR) {
      throw OException.wrapException(
          new OStorageException(
              "Internal error happened in storage "
                  + name
                  + " please restart the server or re-open the storage to undergo the restore process and fix the error."),
          this.error);
    }
  }

  public void rollbackOperationsFromThread(final Thread thread) {
    try {
      stateLock.acquireWriteLock();
      try {
        checkOpennessAndMigration();
        blockedThreads.add(thread);
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

  private void checkIfThreadIsBlocked() {
    final Thread thread = Thread.currentThread();
    if (!blockedThreads.isEmpty() && blockedThreads.contains(thread)) {
      thread.interrupt();

      OLogManager.instance().warnNoDb(this, "Execution  of thread '%s' is interrupted", thread);

      throw new OStorageException(
          "Operations on thread '"
              + thread
              + "' are blocked and can not be performed. Thread will be interrupted.");
    }
  }

  protected final void makeFuzzyCheckpoint() {
    // check every 1 ms.
    while (!stateLock.tryAcquireReadLock(1_000_000)) {
      if (status != STATUS.OPEN || status != STATUS.MIGRATION) {
        return;
      }
    }

    try {
      interruptionManager.enterCriticalPath();
      if (status != STATUS.OPEN || status != STATUS.MIGRATION) {
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
      final long minAtomicOperationSegment =
          atomicOperationsTable.getSegmentEarliestNotPersistedOperation();
      if (minAtomicOperationSegment >= 0 && fuzzySegment > minAtomicOperationSegment) {
        fuzzySegment = minAtomicOperationSegment;
      }

      OLogManager.instance()
          .debugNoDb(
              this,
              "Before fuzzy checkpoint: min LSN segment is "
                  + minLSNSegment
                  + ", WAL begin is "
                  + beginLSN
                  + ", WAL end is "
                  + endLSN
                  + ", fuzzy segment is "
                  + fuzzySegment,
              null);

      if (fuzzySegment > beginLSN.getSegment() && beginLSN.getSegment() < endLSN.getSegment()) {
        OLogManager.instance().debugNoDb(this, "Making fuzzy checkpoint", null);
        writeCache.syncDataFiles(fuzzySegment, lastMetadata);

        beginLSN = writeAheadLog.begin();
        endLSN = writeAheadLog.end();

        OLogManager.instance()
            .debugNoDb(
                this,
                "After fuzzy checkpoint: WAL begin is " + beginLSN + " WAL end is " + endLSN,
                null);
      } else {
        OLogManager.instance().debugNoDb(this, "No reason to make fuzzy checkpoint", null);
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(new OIOException("Error during fuzzy checkpoint"), ioe);
    } finally {
      stateLock.releaseReadLock();
      interruptionManager.exitCriticalPath();
    }
  }

  public void deleteTreeRidBag(final OSBTreeRidBag ridBag) {
    try {
      checkOpennessAndMigration();

      if (transaction.get() == null) {
        stateLock.acquireWriteLock();
        try {
          checkOpennessAndMigration();
          checkIfThreadIsBlocked();

          atomicOperationsManager.executeInsideAtomicOperation(
              null, atomicOperation -> deleteTreeRidBag(ridBag, atomicOperation));
        } finally {
          stateLock.releaseWriteLock();
        }
      } else {
        deleteTreeRidBag(ridBag, atomicOperationsManager.getCurrentOperation());
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void deleteTreeRidBag(OSBTreeRidBag ridBag, OAtomicOperation atomicOperation) {
    final OBonsaiCollectionPointer collectionPointer = ridBag.getCollectionPointer();
    checkOpennessAndMigration();

    try {
      makeStorageDirty();
      sbTreeCollectionManager.delete(atomicOperation, collectionPointer);
    } catch (final Exception e) {
      OLogManager.instance().errorNoDb(this, "Error during deletion of rid bag", e);
      throw OException.wrapException(new OStorageException("Error during deletion of ridbag"), e);
    }

    ridBag.confirmDelete();
  }

  protected void flushAllData() {
    try {
      writeAheadLog.flush();

      // so we will be able to cut almost all the log
      writeAheadLog.appendNewSegment();

      final OLogSequenceNumber lastLSN;
      if (lastMetadata != null) {
        lastLSN = writeAheadLog.log(new MetaDataRecord(lastMetadata));
      } else {
        lastLSN = writeAheadLog.log(new EmptyWALRecord());
      }

      writeCache.flush();

      atomicOperationsTable.compactTable();
      final long operationSegment = atomicOperationsTable.getSegmentEarliestOperationInProgress();
      if (operationSegment >= 0) {
        throw new IllegalStateException(
            "Can not perform full checkpoint if some of atomic operations in progress");
      }

      writeAheadLog.flush();

      writeAheadLog.cutTill(lastLSN);

      clearStorageDirty();

    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OStorageException("Error during checkpoint creation for storage " + name), ioe);
    }
  }

  /* public long getFullCheckpointCount() {
    return fullCheckpointCount;
  } */

  protected StartupMetadata checkIfStorageDirty() throws IOException {
    return new StartupMetadata(-1, null);
  }

  protected void initConfiguration(
      OAtomicOperation atomicOperation, final OContextConfiguration contextConfiguration)
      throws IOException {}

  @SuppressWarnings({"EmptyMethod"})
  protected final void postCreateSteps() {}

  protected void preCreateSteps() throws IOException {}

  protected abstract void initWalAndDiskCache(OContextConfiguration contextConfiguration)
      throws IOException, InterruptedException;

  protected abstract void postCloseSteps(
      @SuppressWarnings("unused") boolean onDelete, boolean internalError, long lastTxId)
      throws IOException;

  @SuppressWarnings("EmptyMethod")
  protected void postCloseStepsAfterLock(final Map<String, Object> params) {}

  @SuppressWarnings({"EmptyMethod"})
  protected Map<String, Object> preCloseSteps() {
    return new HashMap<>(2);
  }

  protected void postDeleteSteps() {}

  protected void makeStorageDirty() throws IOException {}

  protected void clearStorageDirty() throws IOException {}

  protected boolean isDirty() {
    return false;
  }

  protected String getOpenedAtVersion() {
    return null;
  }

  private ORawBuffer readRecordIfNotLatest(final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    checkOpennessAndMigration();

    if (!rid.isPersistent()) {
      throw new ORecordNotFoundException(
          rid,
          "Cannot read record "
              + rid
              + " since the position is invalid in database '"
              + name
              + '\'');
    }

    if (transaction.get() != null) {
      final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
      return doReadRecordIfNotLatest(cluster, rid, recordVersion);
    }

    stateLock.acquireReadLock();
    try {
      interruptionManager.enterCriticalPath();
      if (readLock) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
        if (db == null
            || !((OTransactionAbstract) db.getTransaction()).getLockedRecords().contains(rid)) {
          acquireReadLock(rid);
        }
      }

      final ORawBuffer buff;
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OCluster cluster = doGetAndCheckCluster(rid.getClusterId());
      buff = doReadRecordIfNotLatest(cluster, rid, recordVersion);
      return buff;
    } finally {
      try {
        if (readLock) {
          final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
          if (db == null
              || !((OTransactionAbstract) db.getTransaction()).getLockedRecords().contains(rid)) {
            releaseReadLock(rid);
          }
        }
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
      }
    }
  }

  private ORawBuffer readRecord(final ORecordId rid, final boolean prefetchRecords) {
    checkOpennessAndMigration();

    if (!rid.isPersistent()) {
      throw new ORecordNotFoundException(
          rid,
          "Cannot read record "
              + rid
              + " since the position is invalid in database '"
              + name
              + '\'');
    }

    if (transaction.get() != null) {
      final OCluster cluster;
      try {
        cluster = doGetAndCheckCluster(rid.getClusterId());
      } catch (IllegalArgumentException e) {
        return null;
      }
      // Disabled this assert have no meaning anymore
      // assert iLockingStrategy.equals(LOCKING_STRATEGY.DEFAULT);
      return doReadRecord(cluster, rid, prefetchRecords);
    }

    stateLock.acquireReadLock();
    try {
      interruptionManager.enterCriticalPath();
      if (readLock) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
        if (db == null
            || !((OTransactionAbstract) db.getTransaction()).getLockedRecords().contains(rid)) {
          acquireReadLock(rid);
        }
      }
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();
      final OCluster cluster;
      try {
        cluster = doGetAndCheckCluster(rid.getClusterId());
      } catch (IllegalArgumentException e) {
        return null;
      }
      return doReadRecord(cluster, rid, prefetchRecords);
    } finally {
      try {
        if (readLock) {
          final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
          if (db == null
              || !((OTransactionAbstract) db.getTransaction()).getLockedRecords().contains(rid)) {
            releaseReadLock(rid);
          }
        }
      } finally {
        stateLock.releaseReadLock();
        interruptionManager.exitCriticalPath();
      }
    }
  }

  private void endStorageTx(
      final OTransactionInternal txi, final Collection<ORecordOperation> recordOperations)
      throws IOException {
    atomicOperationsManager.endAtomicOperation(null);
    assert atomicOperationsManager.getCurrentOperation() == null;

    OTransactionAbstract.updateCacheFromEntries(txi.getDatabase(), recordOperations, true);
    txCommit.increment();
  }

  private void startStorageTx(final OTransactionInternal clientTx) throws IOException {
    final OStorageTransaction storageTx = transaction.get();
    assert storageTx == null || storageTx.getClientTx().getId() == clientTx.getId();
    assert atomicOperationsManager.getCurrentOperation() == null;
    transaction.set(new OStorageTransaction(clientTx));
    try {
      final OAtomicOperation atomicOperation =
          atomicOperationsManager.startAtomicOperation(clientTx.getMetadata().orElse(null));
      if (clientTx.getMetadata().isPresent()) {
        this.lastMetadata = clientTx.getMetadata().get();
      }
      clientTx.storageBegun();
      Iterator<byte[]> ops = clientTx.getSerializedOperations();
      while (ops.hasNext()) {
        byte[] next = ops.next();
        writeAheadLog.log(
            new OHighLevelTransactionChangeRecord(atomicOperation.getOperationUnitId(), next));
      }
    } catch (final RuntimeException e) {
      transaction.set(null);
      throw e;
    }
  }

  private void recoverIfNeeded() throws Exception {
    if (isDirty()) {
      OLogManager.instance()
          .warn(
              this,
              "Storage '"
                  + name
                  + "' was not closed properly. Will try to recover from write ahead log");
      try {
        final String openedAtVersion = getOpenedAtVersion();

        if (openedAtVersion != null && !openedAtVersion.equals(OConstants.getVersion())) {
          throw new OStorageException(
              "Database has been opened at version "
                  + openedAtVersion
                  + " but is attempted to be restored at version "
                  + OConstants.getVersion()
                  + ". Please use correct version to restore database.");
        }

        wereDataRestoredAfterOpen = restoreFromWAL() != null;

        if (recoverListener != null) {
          recoverListener.onStorageRecover();
        }

        flushAllData();
      } catch (final Exception e) {
        OLogManager.instance().error(this, "Exception during storage data restore", e);
        throw e;
      }

      OLogManager.instance().info(this, "Storage data recover was completed");
    }
  }

  private OStorageOperationResult<OPhysicalPosition> doCreateRecord(
      final OAtomicOperation atomicOperation,
      final ORecordId rid,
      final byte[] content,
      int recordVersion,
      final byte recordType,
      final ORecordCallback<Long> callback,
      final OCluster cluster,
      final OPhysicalPosition allocated) {
    if (content == null) {
      throw new IllegalArgumentException("Record is null");
    }

    if (recordVersion > -1) {
      recordVersion++;
    } else {
      recordVersion = 0;
    }

    OPhysicalPosition ppos;
    try {
      ppos = cluster.createRecord(content, recordVersion, recordType, allocated, atomicOperation);
      rid.setClusterPosition(ppos.clusterPosition);

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }
    } catch (final Exception e) {
      OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, e);
      throw ODatabaseException.wrapException(
          new OStorageException("Error during creation of record"), e);
    }

    if (callback != null) {
      callback.call(rid, ppos.clusterPosition);
    }

    if (OLogManager.instance().isDebugEnabled()) {
      OLogManager.instance()
          .debug(this, "Created record %s v.%s size=%d bytes", rid, recordVersion, content.length);
    }

    recordCreated.increment();

    return new OStorageOperationResult<>(ppos);
  }

  private OStorageOperationResult<Integer> doUpdateRecord(
      final OAtomicOperation atomicOperation,
      final ORecordId rid,
      final boolean updateContent,
      byte[] content,
      final int version,
      final byte recordType,
      final ORecordCallback<Integer> callback,
      final OCluster cluster) {

    Orient.instance().getProfiler().startChrono();
    try {

      final OPhysicalPosition ppos =
          cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));
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

        final byte[] newContent =
            checkAndIncrementVersion(cluster, rid, recVersion, dbVersion, content, recordType);

        ppos.recordVersion = dbVersion.get();

        // REMOVED BECAUSE DISTRIBUTED COULD UNDO AN OPERATION RESTORING A LOWER VERSION
        // assert ppos.recordVersion >= oldVersion;

        if (newContent != null) {
          contentModified = true;
          content = newContent;
        }
      }

      if (updateContent) {
        cluster.updateRecord(
            rid.getClusterPosition(), content, ppos.recordVersion, recordType, atomicOperation);
      }

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }

      // if we do not update content of the record we should keep version of the record the same
      // otherwise we would have issues when two records may have the same version but different
      // content
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
        OLogManager.instance()
            .debug(this, "Updated record %s v.%s size=%d", rid, newRecordVersion, content.length);
      }

      recordUpdated.increment();

      if (contentModified) {
        return new OStorageOperationResult<>(newRecordVersion, content, false);
      } else {
        return new OStorageOperationResult<>(newRecordVersion);
      }
    } catch (final OConcurrentModificationException e) {
      recordConflict.increment();
      throw e;
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OStorageException(
              "Error on updating record " + rid + " (cluster: " + cluster.getName() + ")"),
          ioe);
    }
  }

  private OStorageOperationResult<Boolean> doDeleteRecord(
      final OAtomicOperation atomicOperation,
      final ORecordId rid,
      final int version,
      final OCluster cluster) {
    Orient.instance().getProfiler().startChrono();
    try {

      final OPhysicalPosition ppos =
          cluster.getPhysicalPosition(new OPhysicalPosition(rid.getClusterPosition()));

      if (ppos == null) {
        // ALREADY DELETED
        return new OStorageOperationResult<>(false);
      }

      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (version > -1 && ppos.recordVersion != version) {
        recordConflict.increment();

        if (OFastConcurrentModificationException.enabled()) {
          throw OFastConcurrentModificationException.instance();
        } else {
          throw new OConcurrentModificationException(
              rid, ppos.recordVersion, version, ORecordOperation.DELETED);
        }
      }

      cluster.deleteRecord(atomicOperation, ppos.clusterPosition);

      final ORecordSerializationContext context = ORecordSerializationContext.getContext();
      if (context != null) {
        context.executeOperations(atomicOperation, this);
      }

      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance().debug(this, "Deleted record %s v.%s", rid, version);
      }

      recordDeleted.increment();

      return new OStorageOperationResult<>(true);
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OStorageException(
              "Error on deleting record " + rid + "( cluster: " + cluster.getName() + ")"),
          ioe);
    }
  }

  private ORawBuffer doReadRecord(
      final OCluster clusterSegment, final ORecordId rid, final boolean prefetchRecords) {
    try {

      final ORawBuffer buff = clusterSegment.readRecord(rid.getClusterPosition(), prefetchRecords);

      if (buff != null && OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance()
            .debug(
                this,
                "Read record %s v.%s size=%d bytes",
                rid,
                buff.version,
                buff.buffer != null ? buff.buffer.length : 0);
      }

      recordRead.increment();

      return buff;
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private static ORawBuffer doReadRecordIfNotLatest(
      final OCluster cluster, final ORecordId rid, final int recordVersion)
      throws ORecordNotFoundException {
    try {
      return cluster.readRecordIfVersionIsNotLatest(rid.getClusterPosition(), recordVersion);
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during read of record with rid = " + rid), e);
    }
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration config)
      throws IOException {
    OCluster cluster =
        clusterMap.get(config.getName().toLowerCase(configuration.getLocaleInstance()));

    if (cluster != null) {
      cluster.configure(this, config);
      return -1;
    }

    if (config.getStatus() == OStorageClusterConfiguration.STATUS.ONLINE) {
      cluster =
          OPaginatedClusterFactory.createCluster(
              config.getName(), configuration.getVersion(), config.getBinaryVersion(), this);
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
      if (clusterMap.containsKey(
          cluster.getName().toLowerCase(configuration.getLocaleInstance()))) {
        throw new OConfigurationException(
            "Cannot add cluster '"
                + cluster.getName()
                + "' because it is already registered in database '"
                + name
                + "'");
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

  private int doAddCluster(final OAtomicOperation atomicOperation, final String clusterName)
      throws IOException {
    // FIND THE FIRST AVAILABLE CLUSTER ID
    int clusterPos = clusters.size();
    for (int i = 0; i < clusters.size(); ++i) {
      if (clusters.get(i) == null) {
        clusterPos = i;
        break;
      }
    }

    return doAddCluster(atomicOperation, clusterName, clusterPos);
  }

  private int doAddCluster(
      final OAtomicOperation atomicOperation, String clusterName, final int clusterPos)
      throws IOException {
    final OPaginatedCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase(configuration.getLocaleInstance());

      cluster =
          OPaginatedClusterFactory.createCluster(
              clusterName,
              configuration.getVersion(),
              getConfiguration()
                  .getContextConfiguration()
                  .getValueAsInteger(OGlobalConfiguration.STORAGE_CLUSTER_VERSION),
              this);
      cluster.configure(clusterPos, clusterName);
    } else {
      cluster = null;
    }

    int createdClusterId = -1;

    if (cluster != null) {
      cluster.create(atomicOperation);
      createdClusterId = registerCluster(cluster);

      ((OClusterBasedStorageConfiguration) configuration)
          .updateCluster(atomicOperation, cluster.generateClusterConfig());

      sbTreeCollectionManager.createComponent(atomicOperation, createdClusterId);
    }

    return createdClusterId;
  }

  @Override
  public boolean setClusterAttribute(
      final int id, final OCluster.ATTRIBUTES attribute, final Object value) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      if (id >= clusters.size()) {
        return false;
      }

      final OCluster cluster = clusters.get(id);

      if (cluster == null) {
        return false;
      }

      makeStorageDirty();

      return atomicOperationsManager.calculateInsideAtomicOperation(
          null,
          atomicOperation -> doSetClusterAttributed(atomicOperation, attribute, value, cluster));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public boolean setClusterAttribute(
      String clusterName, OCluster.ATTRIBUTES attribute, Object value) {
    Objects.requireNonNull(clusterName);

    try {
      interruptionManager.enterCriticalPath();
      stateLock.acquireWriteLock();
      try {
        checkOpennessAndMigration();

        checkIfThreadIsBlocked();

        final OCluster cluster =
            clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (cluster == null) {
          throwClusterDoesNotExist(clusterName);
        }

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            (atomicOperation) ->
                doSetClusterAttributed(atomicOperation, attribute, value, cluster));
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      interruptionManager.exitCriticalPath();
    }
  }

  private boolean doSetClusterAttributed(
      final OAtomicOperation atomicOperation,
      final OCluster.ATTRIBUTES attribute,
      final Object value,
      final OCluster cluster)
      throws IOException {
    final String stringValue = Optional.ofNullable(value).map(Object::toString).orElse(null);
    switch (attribute) {
      case NAME:
        Objects.requireNonNull(stringValue);

        final Locale locale = configuration.getLocaleInstance();
        final String oldName = cluster.getName();
        cluster.setClusterName(stringValue);
        clusterMap.remove(oldName.toLowerCase(locale));
        clusterMap.put(stringValue.toLowerCase(locale), cluster);
        break;
      case CONFLICTSTRATEGY:
        cluster.setRecordConflictStrategy(stringValue);
        break;
      case STATUS:
        {
          if (stringValue == null) {
            throw new IllegalStateException("Value of attribute is null");
          }

          return setClusterStatus(
              atomicOperation,
              cluster,
              OStorageClusterConfiguration.STATUS.valueOf(
                  stringValue.toUpperCase(configuration.getLocaleInstance())));
        }
        //noinspection deprecation
      case ENCRYPTION:
        throw new UnsupportedOperationException(
            "Encryption should be configured on storage level.");
      default:
        throw new IllegalArgumentException(
            "Runtime change of attribute '" + attribute + "' is not supported");
    }

    ((OClusterBasedStorageConfiguration) configuration)
        .updateCluster(atomicOperation, ((OPaginatedCluster) cluster).generateClusterConfig());
    return true;
  }

  private boolean dropClusterInternal(final OAtomicOperation atomicOperation, final int clusterId)
      throws IOException {
    final OCluster cluster = clusters.get(clusterId);

    if (cluster == null) {
      return true;
    }

    cluster.delete(atomicOperation);

    clusterMap.remove(cluster.getName().toLowerCase(configuration.getLocaleInstance()));
    clusters.set(clusterId, null);

    return false;
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
      interruptionManager.enterCriticalPath();
      if (status == STATUS.CLOSED) {
        return;
      }

      if (status != STATUS.OPEN && status != STATUS.INTERNAL_ERROR) {
        throw OException.wrapException(
            new OStorageException("Storage " + name + " was not opened, so can not be closed"),
            this.error);
      }

      status = STATUS.CLOSING;

      if (status != STATUS.INTERNAL_ERROR) {
        if (!onDelete) {
          flushAllData();
        }

        params = preCloseSteps();

        if (!onDelete) {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                // we close all files inside cache system so we only clear index metadata and close
                // non core indexes
                for (final OBaseIndexEngine engine : indexEngines) {
                  if (engine != null
                      && !(engine instanceof OSBTreeIndexEngine
                          || engine instanceof OHashTableIndexEngine
                          || engine instanceof OCellBTreeSingleValueIndexEngine
                          || engine instanceof OCellBTreeMultiValueIndexEngine
                          || engine instanceof OAutoShardingIndexEngine)) {
                    engine.close();
                  }
                }
                ((OClusterBasedStorageConfiguration) configuration).close(atomicOperation);
              });
        } else {
          for (final OBaseIndexEngine engine : indexEngines) {
            if (engine != null
                && !(engine instanceof OSBTreeIndexEngine
                    || engine instanceof OHashTableIndexEngine
                    || engine instanceof OCellBTreeSingleValueIndexEngine
                    || engine instanceof OCellBTreeMultiValueIndexEngine
                    || engine instanceof OAutoShardingIndexEngine)) {
              // delete method is implemented only in non native indexes, so they do not use ODB
              // atomic operation
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
          writeCache.removeBackgroundExceptionListener(this);
          writeCache.removePageIsBrokenListener(this);
        }

        writeAheadLog.removeCheckpointListener(this);

        if (readCache != null) {
          if (!onDelete) {
            readCache.closeStorage(writeCache);
          } else {
            readCache.deleteStorage(writeCache);
          }
        }

        if (onDelete) {
          writeAheadLog.delete();
        } else {
          writeAheadLog.close();
        }

        postCloseSteps(onDelete, status == STATUS.INTERNAL_ERROR, idGen.getLastId());
        transaction = null;
        lastMetadata = null;
      } else {
        OLogManager.instance()
            .errorNoDb(
                this,
                "Because of JVM error happened inside of storage it can not be properly closed",
                null);
      }

      status = STATUS.CLOSED;
    } catch (final IOException e) {
      final String message = "Error on closing of storage '" + name;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new OStorageException(message), e);

    } finally {
      Orient.instance()
          .getProfiler()
          .stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
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
  protected void closeIndexes(final OAtomicOperation atomicOperation, final boolean onDelete) {
    for (final OBaseIndexEngine engine : indexEngines) {
      if (engine != null) {
        if (onDelete) {
          try {
            engine.delete(atomicOperation);
          } catch (final IOException e) {
            OLogManager.instance()
                .error(this, "Can not delete index engine " + engine.getName(), e);
          }
        } else {
          engine.close();
        }
      }
    }

    indexEngines.clear();
    indexEngineNameMap.clear();
  }

  private byte[] checkAndIncrementVersion(
      final OCluster iCluster,
      final ORecordId rid,
      final AtomicInteger version,
      final AtomicInteger iDatabaseVersion,
      final byte[] iRecordContent,
      final byte iRecordType) {

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
              iCluster.getRecordConflictStrategy() != null
                  ? iCluster.getRecordConflictStrategy()
                  : recordConflictStrategy;
          return strategy.onUpdate(this, iRecordType, rid, v, iRecordContent, iDatabaseVersion);
        } else
        // OK, INCREMENT DB VERSION
        {
          iDatabaseVersion.incrementAndGet();
        }
    }
    return null;
  }

  private void commitEntry(
      final OAtomicOperation atomicOperation,
      final ORecordOperation txEntry,
      final OPhysicalPosition allocated,
      final ORecordSerializer serializer) {
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

      if (cluster.getName().equals(OMetadataDefault.CLUSTER_INDEX_NAME)
          || cluster.getName().equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME))
      // AVOID TO COMMIT INDEX STUFF
      {
        return;
      }

      switch (txEntry.type) {
        case ORecordOperation.LOADED:
          break;
        case ORecordOperation.CREATED:
          {
            final byte[] stream = serializer.toStream(rec);
            if (allocated != null) {
              final OPhysicalPosition ppos;
              final byte recordType = ORecordInternal.getRecordType(rec);
              ppos =
                  doCreateRecord(
                          atomicOperation,
                          rid,
                          stream,
                          rec.getVersion(),
                          recordType,
                          null,
                          cluster,
                          allocated)
                      .getResult();

              ORecordInternal.setVersion(rec, ppos.recordVersion);
            } else {
              // USE -2 AS VERSION TO AVOID INCREMENTING THE VERSION
              final OStorageOperationResult<Integer> updateRes =
                  doUpdateRecord(
                      atomicOperation,
                      rid,
                      ORecordInternal.isContentChanged(rec),
                      stream,
                      -2,
                      ORecordInternal.getRecordType(rec),
                      null,
                      cluster);
              ORecordInternal.setVersion(rec, updateRes.getResult());
              if (updateRes.getModifiedRecordContent() != null) {
                ORecordInternal.fill(
                    rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);
              }
            }
            break;
          }
        case ORecordOperation.UPDATED:
          {
            final byte[] stream = serializer.toStream(rec);

            final OStorageOperationResult<Integer> updateRes =
                doUpdateRecord(
                    atomicOperation,
                    rid,
                    ORecordInternal.isContentChanged(rec),
                    stream,
                    rec.getVersion(),
                    ORecordInternal.getRecordType(rec),
                    null,
                    cluster);
            txEntry.setResultData(updateRes.getResult());
            ORecordInternal.setVersion(rec, updateRes.getResult());
            if (updateRes.getModifiedRecordContent() != null) {
              ORecordInternal.fill(
                  rec, rid, updateRes.getResult(), updateRes.getModifiedRecordContent(), false);
            }

            break;
          }
        case ORecordOperation.DELETED:
          {
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
      ODocumentInternal.clearTransactionTrackData(((ODocument) rec));
    }
    ORecordInternal.unsetDirty(rec);
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId < 0 || iClusterId > clusters.size() - 1) {
      throw new IllegalArgumentException(
          "Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
    }
  }

  private OLogSequenceNumber restoreFromWAL() throws IOException {
    final OLogSequenceNumber begin = writeAheadLog.begin();
    if (begin == null) {
      OLogManager.instance()
          .error(this, "Restore is not possible because write ahead log is empty.", null);
      return null;
    }

    OLogManager.instance().info(this, "Looking for last checkpoint...");

    writeAheadLog.addCutTillLimit(begin);
    try {
      return restoreFromBeginning();
    } finally {
      writeAheadLog.removeCutTillLimit(begin);
    }
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public String incrementalBackup(final String backupDirectory, final OCallable<Void, Void> started)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Incremental backup is supported only in enterprise version");
  }

  @Override
  public boolean supportIncremental() {
    return false;
  }

  @Override
  public void fullIncrementalBackup(final OutputStream stream)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Incremental backup is supported only in enterprise version");
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    throw new UnsupportedOperationException(
        "Incremental backup is supported only in enterprise version");
  }

  @Override
  public void restoreFullIncrementalBackup(final InputStream stream)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Incremental backup is supported only in enterprise version");
  }

  private OLogSequenceNumber restoreFromBeginning() throws IOException {
    OLogManager.instance().info(this, "Data restore procedure is started.");

    final OLogSequenceNumber lsn = writeAheadLog.begin();

    writeCache.restoreModeOn();
    try {
      return restoreFrom(writeAheadLog, lsn);
    } finally {
      writeCache.restoreModeOff();
    }
  }

  protected OLogSequenceNumber restoreFrom(OWriteAheadLog writeAheadLog, OLogSequenceNumber lsn)
      throws IOException {
    OLogSequenceNumber logSequenceNumber = null;
    final OModifiableBoolean atLeastOnePageUpdate = new OModifiableBoolean();

    long recordsProcessed = 0;

    final int reportBatchSize =
        OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();
    final Map<Long, List<OWALRecord>> operationUnits = new HashMap<>(1024);
    final Map<Long, byte[]> operationMetadata = new LinkedHashMap<>(1024);

    long lastReportTime = 0;

    try {
      List<WriteableWALRecord> records = writeAheadLog.read(lsn, 1_000);

      while (!records.isEmpty()) {
        for (final WriteableWALRecord walRecord : records) {
          logSequenceNumber = walRecord.getLsn();

          if (walRecord instanceof OAtomicUnitEndRecord) {
            final OAtomicUnitEndRecord atomicUnitEndRecord = (OAtomicUnitEndRecord) walRecord;
            final List<OWALRecord> atomicUnit =
                operationUnits.remove(atomicUnitEndRecord.getOperationUnitId());

            // in case of data restore from fuzzy checkpoint part of operations may be already
            // flushed to the disk
            if (atomicUnit != null) {
              atomicUnit.add(walRecord);
              restoreAtomicUnit(atomicUnit, atLeastOnePageUpdate);
            }
            byte[] metadata = operationMetadata.remove(atomicUnitEndRecord.getOperationUnitId());
            if (metadata != null) {
              this.lastMetadata = metadata;
            }
          } else if (walRecord instanceof OAtomicUnitStartRecord) {
            if (walRecord instanceof OAtomicUnitStartMetadataRecord) {
              byte[] metadata = ((OAtomicUnitStartMetadataRecord) walRecord).getMetadata();
              operationMetadata.put(
                  ((OAtomicUnitStartRecord) walRecord).getOperationUnitId(), metadata);
            }

            final List<OWALRecord> operationList = new ArrayList<>(1024);

            assert !operationUnits.containsKey(
                ((OAtomicUnitStartRecord) walRecord).getOperationUnitId());

            operationUnits.put(
                ((OAtomicUnitStartRecord) walRecord).getOperationUnitId(), operationList);
            operationList.add(walRecord);
          } else if (walRecord instanceof OOperationUnitRecord) {
            final OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) walRecord;

            List<OWALRecord> operationList =
                operationUnits.get(operationUnitRecord.getOperationUnitId());

            if (operationList == null || operationList.isEmpty()) {
              OLogManager.instance()
                  .errorNoDb(
                      this, "'Start transaction' record is absent for atomic operation", null);

              if (operationList == null) {
                operationList = new ArrayList<>(1024);
                operationUnits.put(operationUnitRecord.getOperationUnitId(), operationList);
              }
            }

            operationList.add(operationUnitRecord);
          } else if (walRecord instanceof ONonTxOperationPerformedWALRecord) {
            if (!wereNonTxOperationsPerformedInPreviousOpen) {
              OLogManager.instance()
                  .warnNoDb(
                      this,
                      "Non tx operation was used during data modification we will need index rebuild.");
              wereNonTxOperationsPerformedInPreviousOpen = true;
            }
          } else if (walRecord instanceof MetaDataRecord) {
            final MetaDataRecord metaDataRecord = (MetaDataRecord) walRecord;
            this.lastMetadata = metaDataRecord.getMetadata();
          } else {
            OLogManager.instance()
                .warnNoDb(this, "Record %s will be skipped during data restore", walRecord);
          }

          recordsProcessed++;

          final long currentTime = System.currentTimeMillis();
          if (reportBatchSize > 0 && recordsProcessed % reportBatchSize == 0
              || currentTime - lastReportTime > WAL_RESTORE_REPORT_INTERVAL) {
            OLogManager.instance()
                .infoNoDb(
                    this,
                    "%d operations were processed, current LSN is %s last LSN is %s",
                    recordsProcessed,
                    lsn,
                    writeAheadLog.end());
            lastReportTime = currentTime;
          }
        }

        records = writeAheadLog.next(records.get(records.size() - 1).getLsn(), 1_000);
      }
    } catch (final OWALPageBrokenException e) {
      OLogManager.instance()
          .errorNoDb(
              this,
              "Data restore was paused because broken WAL page was found. The rest of changes will be rolled back.",
              e);
    } catch (final RuntimeException e) {
      OLogManager.instance()
          .errorNoDb(
              this,
              "Data restore was paused because of exception. The rest of changes will be rolled back.",
              e);
    }

    if (atLeastOnePageUpdate.getValue()) {
      return logSequenceNumber;
    }

    return null;
  }

  protected final void restoreAtomicUnit(
      final List<OWALRecord> atomicUnit, final OModifiableBoolean atLeastOnePageUpdate)
      throws IOException {
    assert atomicUnit.get(atomicUnit.size() - 1) instanceof OAtomicUnitEndRecord;

    for (final OWALRecord walRecord : atomicUnit) {
      if (walRecord instanceof OFileDeletedWALRecord) {
        final OFileDeletedWALRecord fileDeletedWALRecord = (OFileDeletedWALRecord) walRecord;
        if (writeCache.exists(fileDeletedWALRecord.getFileId())) {
          readCache.deleteFile(fileDeletedWALRecord.getFileId(), writeCache);
        }
      } else if (walRecord instanceof OFileCreatedWALRecord) {
        final OFileCreatedWALRecord fileCreatedCreatedWALRecord = (OFileCreatedWALRecord) walRecord;

        if (!writeCache.exists(fileCreatedCreatedWALRecord.getFileName())) {
          readCache.addFile(
              fileCreatedCreatedWALRecord.getFileName(),
              fileCreatedCreatedWALRecord.getFileId(),
              writeCache);
        }
      } else if (walRecord instanceof OUpdatePageRecord) {
        final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) walRecord;

        long fileId = updatePageRecord.getFileId();
        if (!writeCache.exists(fileId)) {
          final String fileName = writeCache.restoreFileById(fileId);

          if (fileName == null) {
            throw new OStorageException(
                "File with id "
                    + fileId
                    + " was deleted from storage, the rest of operations can not be restored");
          } else {
            OLogManager.instance()
                .warn(
                    this,
                    "Previously deleted file with name "
                        + fileName
                        + " was deleted but new empty file was added to continue restore process");
          }
        }

        final long pageIndex = updatePageRecord.getPageIndex();
        fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

        OCacheEntry cacheEntry =
            readCache.loadForWrite(fileId, pageIndex, true, writeCache, false, null);
        if (cacheEntry == null) {
          do {
            if (cacheEntry != null) {
              readCache.releaseFromWrite(cacheEntry, writeCache, true);
            }

            cacheEntry = readCache.allocateNewPage(fileId, writeCache, null);
          } while (cacheEntry.getPageIndex() != pageIndex);
        }

        try {
          final ODurablePage durablePage = new ODurablePage(cacheEntry);
          if (durablePage.getLSN().compareTo(walRecord.getLsn()) < 0) {
            durablePage.restoreChanges(updatePageRecord.getChanges());
            durablePage.setLsn(updatePageRecord.getLsn());
          }
        } finally {
          readCache.releaseFromWrite(cacheEntry, writeCache, true);
        }

        atLeastOnePageUpdate.setValue(true);
      } else if (walRecord instanceof OAtomicUnitStartRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else if (walRecord instanceof OHighLevelTransactionChangeRecord) {
        //noinspection UnnecessaryContinue
        continue;
      } else {
        OLogManager.instance()
            .error(
                this,
                "Invalid WAL record type was passed %s. Given record will be skipped.",
                null,
                walRecord.getClass());

        assert false : "Invalid WAL record type was passed " + walRecord.getClass().getName();
      }
    }
  }

  @SuppressWarnings("unused")
  public void setStorageConfigurationUpdateListener(
      final OStorageConfigurationUpdateListener storageConfigurationUpdateListener) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      ((OClusterBasedStorageConfiguration) configuration)
          .setConfigurationUpdateListener(storageConfigurationUpdateListener);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  public void pauseConfigurationUpdateNotifications() {
    stateLock.acquireReadLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      ((OClusterBasedStorageConfiguration) configuration).pauseUpdateNotifications();
    } finally {
      stateLock.releaseReadLock();
      interruptionManager.exitCriticalPath();
    }
  }

  public void fireConfigurationUpdateNotifications() {
    stateLock.acquireReadLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      ((OClusterBasedStorageConfiguration) configuration).fireUpdateNotifications();
    } finally {
      stateLock.releaseReadLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @SuppressWarnings("unused")
  protected static Map<Integer, List<ORecordId>> getRidsGroupedByCluster(
      final Collection<ORecordId> rids) {
    final Map<Integer, List<ORecordId>> ridsPerCluster = new HashMap<>(8);
    for (final ORecordId rid : rids) {
      final List<ORecordId> group =
          ridsPerCluster.computeIfAbsent(rid.getClusterId(), k -> new ArrayList<>(rids.size()));
      group.add(rid);
    }
    return ridsPerCluster;
  }

  private static void lockIndexes(final TreeMap<String, OTransactionIndexChanges> indexes) {
    for (final OTransactionIndexChanges changes : indexes.values()) {
      assert changes.changesPerKey instanceof TreeMap;

      final OIndexInternal index = changes.getAssociatedIndex();

      final List<Object> orderedIndexNames = new ArrayList<>(changes.changesPerKey.keySet());
      if (orderedIndexNames.size() > 1) {
        orderedIndexNames.sort(
            (o1, o2) -> {
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
      if (!fullyLocked && !changes.nullKeyChanges.isEmpty()) {
        index.acquireAtomicExclusiveLock(null);
      }
    }
  }

  private static void lockClusters(final TreeMap<Integer, OCluster> clustersToLock) {
    for (final OCluster cluster : clustersToLock.values()) {
      cluster.acquireAtomicExclusiveLock();
    }
  }

  private void lockRidBags(
      final TreeMap<Integer, OCluster> clusters,
      final TreeMap<String, OTransactionIndexChanges> indexes,
      final OIndexManagerAbstract manager,
      ODatabaseDocumentInternal db) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

    for (final Integer clusterId : clusters.keySet()) {
      atomicOperationsManager.acquireExclusiveLockTillOperationComplete(
          atomicOperation, OSBTreeCollectionManagerShared.generateLockName(clusterId));
    }

    for (final Map.Entry<String, OTransactionIndexChanges> entry : indexes.entrySet()) {
      final String indexName = entry.getKey();
      final OIndexInternal index = entry.getValue().resolveAssociatedIndex(indexName, manager, db);

      if (!index.isUnique()) {
        atomicOperationsManager.acquireExclusiveLockTillOperationComplete(
            atomicOperation, OIndexRIDContainerSBTree.generateLockName(indexName));
      }
    }
  }

  private void registerProfilerHooks() {
    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".createRecord",
            "Number of created records",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordCreated),
            "db.*.createRecord");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".readRecord",
            "Number of read records",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordRead),
            "db.*.readRecord");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".updateRecord",
            "Number of updated records",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordUpdated),
            "db.*.updateRecord");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".deleteRecord",
            "Number of deleted records",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordDeleted),
            "db.*.deleteRecord");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".scanRecord",
            "Number of read scanned",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordScanned),
            "db.*.scanRecord");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".recyclePosition",
            "Number of recycled records",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordRecycled),
            "db.*.recyclePosition");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".conflictRecord",
            "Number of conflicts during updating and deleting records",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(recordConflict),
            "db.*.conflictRecord");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".txBegun",
            "Number of transactions begun",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(txBegun),
            "db.*.txBegun");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".txCommit",
            "Number of committed transactions",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(txCommit),
            "db.*.txCommit");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "db." + this.name + ".txRollback",
            "Number of rolled back transactions",
            OProfiler.METRIC_TYPE.COUNTER,
            new ModifiableLongProfileHookValue(txRollback),
            "db.*.txRollback");
  }

  protected final RuntimeException logAndPrepareForRethrow(
      final RuntimeException runtimeException) {
    return logAndPrepareForRethrow(runtimeException, true);
  }

  private RuntimeException logAndPrepareForRethrow(
      final RuntimeException runtimeException, final boolean putInReadOnlyMode) {
    if (!(runtimeException instanceof OHighLevelException
        || runtimeException instanceof ONeedRetryException)) {
      if (putInReadOnlyMode) {
        this.error = runtimeException;
        status = STATUS.INTERNAL_ERROR;
      }

      OLogManager.instance()
          .errorStorage(
              this,
              "Exception `%08X` in storage `%s`: %s",
              runtimeException,
              System.identityHashCode(runtimeException),
              getURL(),
              OConstants.getVersion());
    }

    return runtimeException;
  }

  protected final Error logAndPrepareForRethrow(final Error error) {
    return logAndPrepareForRethrow(error, true);
  }

  private Error logAndPrepareForRethrow(final Error error, final boolean putInReadOnlyMode) {
    if (!(error instanceof OHighLevelException)) {
      if (putInReadOnlyMode) {
        this.error = error;
        status = STATUS.INTERNAL_ERROR;
      }

      OLogManager.instance()
          .errorStorage(
              this,
              "Exception `%08X` in storage `%s`: %s",
              error,
              System.identityHashCode(error),
              getURL(),
              OConstants.getVersion());
    }

    return error;
  }

  protected final RuntimeException logAndPrepareForRethrow(final Throwable throwable) {
    return logAndPrepareForRethrow(throwable, true);
  }

  private RuntimeException logAndPrepareForRethrow(
      final Throwable throwable, final boolean putInReadOnlyMode) {
    if (!(throwable instanceof OHighLevelException || throwable instanceof ONeedRetryException)) {
      if (putInReadOnlyMode) {
        this.error = throwable;
        status = STATUS.INTERNAL_ERROR;
      }
      OLogManager.instance()
          .errorStorage(
              this,
              "Exception `%08X` in storage `%s`: %s",
              throwable,
              System.identityHashCode(throwable),
              getURL(),
              OConstants.getVersion());
    }
    return new RuntimeException(throwable);
  }

  private OInvalidIndexEngineIdException logAndPrepareForRethrow(
      final OInvalidIndexEngineIdException exception) {
    OLogManager.instance()
        .errorStorage(
            this,
            "Exception `%08X` in storage `%s` : %s",
            exception,
            System.identityHashCode(exception),
            getURL(),
            OConstants.getVersion());
    return exception;
  }

  @Override
  public final OStorageConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public final void setSchemaRecordId(final String schemaRecordId) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              storageConfiguration.setSchemaRecordId(atomicOperation, schemaRecordId));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setDateFormat(final String dateFormat) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> storageConfiguration.setDateFormat(atomicOperation, dateFormat));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setTimeZone(final TimeZone timeZoneValue) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> storageConfiguration.setTimeZone(atomicOperation, timeZoneValue));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setLocaleLanguage(final String locale) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> storageConfiguration.setLocaleLanguage(atomicOperation, locale));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setCharset(final String charset) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> storageConfiguration.setCharset(atomicOperation, charset));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setIndexMgrRecordId(final String indexMgrRecordId) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              storageConfiguration.setIndexMgrRecordId(atomicOperation, indexMgrRecordId));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setDateTimeFormat(final String dateTimeFormat) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              storageConfiguration.setDateTimeFormat(atomicOperation, dateTimeFormat));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setLocaleCountry(final String localeCountry) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> storageConfiguration.setLocaleCountry(atomicOperation, localeCountry));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setClusterSelection(final String clusterSelection) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              storageConfiguration.setClusterSelection(atomicOperation, clusterSelection));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setMinimumClusters(final int minimumClusters) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      storageConfiguration.setMinimumClusters(minimumClusters);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setValidation(final boolean validation) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> storageConfiguration.setValidation(atomicOperation, validation));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void removeProperty(final String property) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> storageConfiguration.removeProperty(atomicOperation, property));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setProperty(final String property, final String value) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> storageConfiguration.setProperty(atomicOperation, property, value));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void setRecordSerializer(final String recordSerializer, final int version) {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
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
      interruptionManager.exitCriticalPath();
    }
  }

  @Override
  public final void clearProperties() {
    stateLock.acquireWriteLock();
    try {
      interruptionManager.enterCriticalPath();
      checkOpennessAndMigration();
      checkIfThreadIsBlocked();

      final OClusterBasedStorageConfiguration storageConfiguration =
          (OClusterBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, storageConfiguration::clearProperties);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.releaseWriteLock();
      interruptionManager.exitCriticalPath();
    }
  }

  public Optional<byte[]> getLastMetadata() {
    return Optional.ofNullable(lastMetadata);
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

  private final class WALVacuum implements Runnable {

    private WALVacuum() {}

    @Override
    public void run() {
      stateLock.acquireReadLock();
      try {
        interruptionManager.enterCriticalPath();
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
          flushTillSegmentId =
              (nonActiveSegments[0] + nonActiveSegments[nonActiveSegments.length - 1]) / 2;
        }

        long minDirtySegment;
        do {
          writeCache.flushTillSegment(flushTillSegmentId);

          // we should take active segment BEFORE min write cache LSN call
          // to avoid case when new data are changed before call
          final long activeSegment = writeAheadLog.activeSegment();
          final Long minLSNSegment = writeCache.getMinimalNotFlushedSegment();

          if (minLSNSegment == null) {
            minDirtySegment = activeSegment;
          } else {
            minDirtySegment = minLSNSegment;
          }
        } while (minDirtySegment < flushTillSegmentId);

        atomicOperationsTable.compactTable();
        final long operationSegment =
            atomicOperationsTable.getSegmentEarliestNotPersistedOperation();
        if (operationSegment >= 0 && minDirtySegment > operationSegment) {
          minDirtySegment = operationSegment;
        }

        if (minDirtySegment <= nonActiveSegments[0]) {
          return;
        }

        writeCache.syncDataFiles(minDirtySegment, lastMetadata);
      } catch (final Exception e) {
        OLogManager.instance()
            .error(
                this, "Error during flushing of data for fuzzy checkpoint, in storage %s", e, name);
      } finally {
        stateLock.releaseReadLock();
        walVacuumInProgress.set(false);
        interruptionManager.exitCriticalPath();
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

  protected static final class StartupMetadata {
    private final long lastTxId;
    private final byte[] txMetadata;

    public StartupMetadata(long lastTxId, byte[] txMetadata) {
      this.lastTxId = lastTxId;
      this.txMetadata = txMetadata;
    }
  }

  public int getVersionForKey(final String indexName, final Object key) {
    assert isIndexUniqueByName(indexName);
    if (!isDistributedMode(lastMetadata)) {
      return 0;
    }
    final OBaseIndexEngine indexEngine = indexEngineNameMap.get(indexName);
    return indexEngine.getUniqueIndexVersion(key);
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean isDistributedMode(final byte[] metadata) {
    return metadata == null;
  }

  private boolean isIndexUniqueByName(final String indexName) {
    final OStorageConfiguration.IndexEngineData engineData =
        configuration.getIndexEngine(indexName, 0);
    return isIndexUniqueByType(engineData.getIndexType());
  }

  private boolean isIndexUniqueByType(final String indexType) {
    //noinspection deprecation
    return indexType.equals(OClass.INDEX_TYPE.UNIQUE.name())
        || indexType.equals(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name())
        || indexType.equals(OClass.INDEX_TYPE.DICTIONARY.name())
        || indexType.equals(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name());
  }

  private void applyUniqueIndexChanges(
      final String indexName, final OTransactionIndexChanges changes) {
    for (final OTransactionIndexChangesPerKey changePerKey : changes.changesPerKey.values()) {
      this.applyUniqueIndexChange(indexName, changePerKey.key);
    }
  }

  private void applyUniqueIndexChange(final String indexName, final Object key) {
    if (!isDistributedMode(lastMetadata)) {
      final OBaseIndexEngine indexEngine = indexEngineNameMap.get(indexName);
      indexEngine.updateUniqueIndexVersion(key);
    }
  }

  /**
   * forces interruption of this execution
   *
   * @param thread The thread where this storage instance is running (has to be provided from higher
   *     level)
   */
  public void interruptExecution(Thread thread) {
    interruptionManager.interrupt(thread);
  }
}
