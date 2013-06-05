/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfigurationLocal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.cache.O2QCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.storage.impl.local.ODataLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractCheckPointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OCheckpointEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OClusterAwareWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPagesRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFullCheckpointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OStorageLocalAbstract {
  private static final int                          ONE_KB                  = 1024;
  private final int                                 DELETE_MAX_RETRIES;
  private final int                                 DELETE_WAIT_TIME;

  private final Map<String, OLocalPaginatedCluster> clusterMap              = new LinkedHashMap<String, OLocalPaginatedCluster>();
  private OLocalPaginatedCluster[]                  clusters                = new OLocalPaginatedCluster[0];

  private String                                    storagePath;
  private final OStorageVariableParser              variableParser;
  private int                                       defaultClusterId        = -1;

  private static String[]                           ALL_FILE_EXTENSIONS     = { ".ocf", ".pls", ".pcl", ".oda", ".odh", ".otx",
      ".ocs", ".oef", ".oem", ".oet", ".wal", ".wmr"                       };

  private OModificationLock                         modificationLock        = new OModificationLock();

  private ODiskCache                                diskCache;
  private OWriteAheadLog                            writeAheadLog;

  private final ScheduledExecutorService            fuzzyCheckpointExecutor = Executors
                                                                                .newSingleThreadScheduledExecutor(new ThreadFactory() {
                                                                                  @Override
                                                                                  public Thread newThread(Runnable r) {
                                                                                    Thread thread = new Thread(r);
                                                                                    thread.setDaemon(true);
                                                                                    return thread;
                                                                                  }
                                                                                });
  private final ExecutorService                     checkpointExecutor      = Executors
                                                                                .newSingleThreadExecutor(new ThreadFactory() {
                                                                                  @Override
                                                                                  public Thread newThread(Runnable r) {
                                                                                    Thread thread = new Thread(r);
                                                                                    thread.setDaemon(true);
                                                                                    return thread;
                                                                                  }
                                                                                });

  private Map<OLogSequenceNumber, List<OWALRecord>> operationUnits          = new HashMap<OLogSequenceNumber, List<OWALRecord>>();

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
  }

  private void initWal() throws IOException {
    final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    if (OGlobalConfiguration.USE_WAL.getValueAsBoolean()) {
      writeAheadLog = new OWriteAheadLog(this);

      final int fuzzyCheckpointDelay = OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger();
      fuzzyCheckpointExecutor.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            makeFuzzyCheckpoint();
          } catch (Throwable e) {
            OLogManager.instance().error(this, "Error during background fuzzy checkpoint creation for storage " + name, e);
          }

        }
      }, fuzzyCheckpointDelay, fuzzyCheckpointDelay, TimeUnit.SECONDS);
    } else
      writeAheadLog = null;

    diskCache = new O2QCache(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * ONE_KB * ONE_KB,
        OGlobalConfiguration.DISK_CACHE_WRITE_QUEUE_LENGTH.getValueAsInteger(), directMemory, writeAheadLog,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB, this, false);
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
    lock.acquireExclusiveLock();
    try {

      addUser();

      if (status != STATUS.CLOSED)
        // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
        // REUSED
        return;

      if (!exists())
        throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);

      initWal();

      status = STATUS.OPEN;

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
              clusters[i].open();
            } else {
              if (clusterConfig.getName().equals(CLUSTER_DEFAULT_NAME))
                defaultClusterId = pos;

              clusters[pos].open();
            }
          } catch (FileNotFoundException e) {
            OLogManager.instance().warn(
                this,
                "Error on loading cluster '" + clusters[i].getName() + "' (" + i
                    + "): file not found. It will be excluded from current database '" + getName() + "'.");

            clusterMap.remove(clusters[i].getName());
            clusters[i] = null;
          }
        } else {
          clusters = Arrays.copyOf(clusters, clusters.length + 1);
          clusters[i] = null;
        }
      }

      restoreIfNeeded();
    } catch (Exception e) {
      close(true);
      throw new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode, e);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  private void restoreIfNeeded() throws IOException {
    boolean wasSoftlyClosed = true;
    for (OCluster cluster : clusters)
      if (cluster != null && !cluster.wasSoftlyClosed())
        wasSoftlyClosed = false;

    if (!wasSoftlyClosed) {
      OLogManager.instance().warn(this,
          "Storage " + name + " was not closed properly. Will try to restore from write ahead logging.");
      restoreFromWAL();
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

    OLogManager.instance().info(this, "Try to find last checkpoint.");

    OLogSequenceNumber lastCheckPoint = writeAheadLog.getLastCheckpoint();
    if (lastCheckPoint == null) {
      OLogManager.instance().info(this, "Checkpoints are absent will restore from beginning.");
      restoreFromBegging();
    }

    OWALRecord checkPointRecord = writeAheadLog.read(lastCheckPoint);
    if (checkPointRecord == null) {
      OLogManager.instance().info(this, "Checkpoints are absent will restore from beginning.");
      restoreFromBegging();
      return;
    }

    if (checkPointRecord instanceof OFuzzyCheckpointStartRecord) {
      OLogManager.instance().info(this, "Found checkpoint is fuzzy checkpoint.");

      boolean fuzzyCheckPointIsComplete = checkFuzzyCheckPointIsComplete(lastCheckPoint);
      if (!fuzzyCheckPointIsComplete) {
        OLogManager.instance().warn(this, "Fuzzy checkpoint is not complete.");

        OLogSequenceNumber previousCheckpoint = ((OFuzzyCheckpointStartRecord) checkPointRecord).getPreviousCheckpoint();
        checkPointRecord = null;

        if (previousCheckpoint != null)
          checkPointRecord = writeAheadLog.read(previousCheckpoint);

        if (checkPointRecord != null) {
          OLogManager.instance().warn(this, "Will restore from previous checkpoint.");
          restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);
        } else {
          OLogManager.instance().warn(this, "Will restore from beginning.");
          restoreFromBegging();
        }
      } else
        restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);

      return;
    }

    if (checkPointRecord instanceof OFullCheckpointStartRecord) {
      OLogManager.instance().info(this, "Found checkpoint is full checkpoint.");
      boolean fullCheckPointIsComplete = checkFullCheckPointIsComplete(lastCheckPoint);
      if (!fullCheckPointIsComplete) {
        OLogManager.instance().warn(this, "Full checkpoint is not complete.");

        OLogSequenceNumber previousCheckpoint = ((OFullCheckpointStartRecord) checkPointRecord).getPreviousCheckpoint();
        checkPointRecord = null;
        if (previousCheckpoint != null)
          checkPointRecord = writeAheadLog.read(previousCheckpoint);

        if (checkPointRecord != null) {
          OLogManager.instance().warn(this, "Will restore from previous checkpoint.");

        } else {
          OLogManager.instance().warn(this, "Will restore from beginning.");
          restoreFromBegging();
        }
      } else
        restoreFromCheckPoint((OAbstractCheckPointStartRecord) checkPointRecord);

      return;
    }

    throw new OStorageException("Unknown checkpoint record type " + checkPointRecord.getClass().getName());

  }

  private boolean checkFullCheckPointIsComplete(OLogSequenceNumber lastCheckPoint) throws IOException {
    OLogSequenceNumber lsn = writeAheadLog.next(lastCheckPoint);

    while (lsn != null) {
      OWALRecord walRecord = writeAheadLog.read(lsn);
      if (walRecord instanceof OCheckpointEndRecord)
        return true;
    }

    return false;
  }

  private boolean checkFuzzyCheckPointIsComplete(OLogSequenceNumber lastCheckPoint) throws IOException {
    OLogSequenceNumber lsn = writeAheadLog.next(lastCheckPoint);

    while (lsn != null) {
      OWALRecord walRecord = writeAheadLog.read(lsn);
      if (walRecord instanceof OFuzzyCheckpointEndRecord)
        return true;
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
    OLogManager.instance().info(this, "Data restore procedure from fuzzy checkpoint is started.");
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
    OLogManager.instance().info(this, "Date restore procedure is started.");
    OLogSequenceNumber lsn = writeAheadLog.begin();
    restoreFrom(lsn);
  }

  private void restoreFrom(OLogSequenceNumber lsn) throws IOException {
    while (lsn != null) {
      OWALRecord walRecord = writeAheadLog.read(lsn);
      if (walRecord instanceof OAtomicUnitStartRecord) {
        List<OWALRecord> operationList = new ArrayList<OWALRecord>();
        operationUnits.put(walRecord.getLsn(), operationList);
        operationList.add(walRecord);
      } else if (walRecord instanceof OOperationUnitRecord) {
        OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) walRecord;
        OLogSequenceNumber prevLsn = operationUnitRecord.getPrevLsn();

        if (prevLsn == null) {
          assert false : "Record with LSN " + walRecord.getLsn() + " has no previous record link.";
          OLogManager.instance().error(this, "Record with LSN %s has no previous record link.", walRecord.getLsn());

          List<OWALRecord> operationList = new ArrayList<OWALRecord>();
          operationUnits.put(walRecord.getLsn(), operationList);
          operationList.add(walRecord);
        } else {
          List<OWALRecord> operationList = operationUnits.remove(prevLsn);
          if (operationList == null) {
            OLogManager.instance().error(this, "Record with LSN %s has no previous record link. And will be skipped.",
                walRecord.getLsn());
            continue;
          } else {
            operationList.add(walRecord);

            if (!(operationUnitRecord instanceof OAtomicUnitEndRecord))
              operationUnits.put(walRecord.getLsn(), operationList);
          }

          if (operationUnitRecord instanceof OAtomicUnitEndRecord) {
            OClusterAwareWALRecord clusterAwareRecord = (OClusterAwareWALRecord) operationList.get(0);
            int clusterId = clusterAwareRecord.getClusterId();

            OLocalPaginatedCluster paginatedCluster = getClusterById(clusterId);
            if (paginatedCluster == null) {
              OLogManager.instance().error(this,
                  "Cluster with %d is absent and operation which consist of following WAL records %s will be skipped", clusterId,
                  operationList);
              assert false;
            } else {
              paginatedCluster.restoreAtomicOperation(operationList);
            }
          }
        }
      } else
        OLogManager.instance().warn(this, "Record %s will be skipped during data restore.", walRecord);

      lsn = writeAheadLog.next(lsn);
    }

    rollbackAllUnfinishedWALOperations();
  }

  private void rollbackAllUnfinishedWALOperations() throws IOException {
    for (List<OWALRecord> operationUnit : operationUnits.values()) {
      if (operationUnit.isEmpty())
        continue;

      OWALRecord firstWalRecord = operationUnit.get(0);
      OClusterAwareWALRecord clusterAwareWALRecord = (OClusterAwareWALRecord) firstWalRecord;
      int clusterId = clusterAwareWALRecord.getClusterId();

      OLocalPaginatedCluster paginatedCluster = getClusterById(clusterId);
      if (paginatedCluster == null) {
        OLogManager.instance().error(this,
            "Cluster with %d is absent and operation which consist of following WAL records %s will be skipped", clusterId,
            operationUnit);
        assert false;
      } else {
        if (firstWalRecord instanceof OAtomicUnitStartRecord) {
          OAtomicUnitStartRecord atomicUnitStartRecord = (OAtomicUnitStartRecord) firstWalRecord;
          if (atomicUnitStartRecord.isRollbackSupported())
            paginatedCluster.revertAtomicOperation(operationUnit);
        }

      }
    }
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

      initWal();

      status = STATUS.OPEN;

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadata.CLUSTER_INTERNAL_NAME, null, null, true);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadata.CLUSTER_INDEX_NAME, null, null, true);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF
      // INDEXING
      addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadata.CLUSTER_MANUAL_INDEX_NAME, null, null, true);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), CLUSTER_DEFAULT_NAME, null, null, false);

      configuration.create();

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

  private boolean exists(String path) {
    return new File(path + "/" + OMetadata.CLUSTER_INTERNAL_NAME + OLocalPaginatedCluster.DEF_EXTENSION).exists();
  }

  @Override
  public void close(final boolean iForce) {
    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {

      if (!checkForClose(iForce))
        return;

      status = STATUS.CLOSING;

      makeFullCheckpoint();
      fuzzyCheckpointExecutor.shutdown();
      final int fuzzyCheckpointDelay = OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger();
      fuzzyCheckpointExecutor.awaitTermination(fuzzyCheckpointDelay * 10, TimeUnit.SECONDS);

      checkpointExecutor.shutdown();
      checkpointExecutor.awaitTermination(OGlobalConfiguration.WAL_CHECKPOINT_INTERVAL_TIMEOUT.getValueAsInteger(),
          TimeUnit.SECONDS);

      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.close();
      clusters = new OLocalPaginatedCluster[0];
      clusterMap.clear();

      if (configuration != null)
        configuration.close();

      level2Cache.shutdown();

      super.close(iForce);

      if (writeAheadLog != null)
        writeAheadLog.close();

      diskCache.close();

      Orient.instance().unregisterStorage(this);
      status = STATUS.CLOSED;
    } catch (InterruptedException ie) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, ie, OStorageException.class);
      Thread.interrupted();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, e, OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a local database", timer, "db.*.close");
    }
  }

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

    final long timer = Orient.instance().getProfiler().startChrono();

    // GET REAL DIRECTORY
    File dbDir = new File(OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url)));
    if (!dbDir.exists() || !dbDir.isDirectory())
      dbDir = dbDir.getParentFile();

    lock.acquireExclusiveLock();
    try {

      if (writeAheadLog != null)
        writeAheadLog.delete();

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
        OMemoryWatchDog.freeMemoryForResourceCleanup(DELETE_WAIT_TIME);
      }

      throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ". Database files seem locked");

    } catch (IOException e) {
      throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ".", e);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a local database", timer, "db.*.drop");
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

  public ODataLocal getDataSegmentById(final int iDataSegmentId) {
    throw new UnsupportedOperationException("getDataSegmentById");
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    throw new UnsupportedOperationException("getDataSegmentIdByName");
  }

  /**
   * Add a new data segment in the default segment directory and with filename equals to the cluster name.
   */
  public int addDataSegment(final String iDataSegmentName) {
    return addDataSegment(iDataSegmentName, null);
  }

  public int addDataSegment(String iSegmentName, final String iDirectory) {
    throw new UnsupportedOperationException("addDataSegment");
  }

  public int addCluster(final String clusterType, String clusterName, final String location, final String dataSegmentName,
      boolean forceListBased, final Object... parameters) {
    checkOpeness();

    lock.acquireExclusiveLock();
    try {
      // FIND THE FIRST AVAILABLE CLUSTER ID
      int clusterPos = clusters.length;
      for (int i = 0; i < clusters.length; ++i) {
        if (clusters[i] == null) {
          clusterPos = i;
          break;
        }
      }

      return addClusterInternal(clusterName, clusterPos, location, parameters);

    } catch (Exception e) {
      OLogManager.instance().exception("Error in creation of new cluster '" + clusterName + "' of type: " + clusterType, e,
          OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();
    }

    return -1;
  }

  public int addCluster(String clusterType, String clusterName, int iRequestedId, String location, String dataSegmentName,
      boolean forceListBased, Object... parameters) {

    lock.acquireExclusiveLock();
    try {
      if (iRequestedId < 0) {
        throw new OConfigurationException("Cluster id must be positive!");
      }
      if (iRequestedId < clusters.length && clusters[iRequestedId] != null) {
        throw new OConfigurationException("Requested cluster ID is occupied!");
      }

      return addClusterInternal(clusterName, iRequestedId, location, parameters);

    } catch (Exception e) {
      OLogManager.instance().exception("Error in creation of new cluster '" + clusterName + "' of type: " + clusterType, e,
          OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();
    }

    return -1;
  }

  private int addClusterInternal(String clusterName, int clusterPos, String location, Object... parameters) throws IOException {

    final OLocalPaginatedCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase();

      cluster = new OLocalPaginatedCluster();
      cluster.configure(this, clusterPos, clusterName, location, -1, parameters);
    } else {
      cluster = null;
    }

    final int createdClusterId = registerCluster(cluster);

    if (cluster != null) {
      if (!cluster.exists()) {
        cluster.create(-1);
        makeFullCheckpoint();
      } else {
        cluster.open();
      }
      configuration.update();

    }

    return createdClusterId;
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    lock.acquireExclusiveLock();
    try {

      if (iClusterId < 0 || iClusterId >= clusters.length)
        throw new IllegalArgumentException("Cluster id '" + iClusterId + "' is outside the of range of configured clusters (0-"
            + (clusters.length - 1) + ") in database '" + name + "'");

      final OCluster cluster = clusters[iClusterId];
      if (cluster == null)
        return false;

      getLevel2Cache().freeCluster(iClusterId);

      if (iTruncate)
        cluster.truncate();
      cluster.delete();

      clusterMap.remove(cluster.getName());
      clusters[iClusterId] = null;

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
  public long count(int iClusterId, boolean countTombstones) {
    if (iClusterId == -1)
      throw new OStorageException("Cluster Id " + iClusterId + " is invalid in database '" + name + "'");

    // COUNT PHYSICAL CLUSTER IF ANY
    checkOpeness();

    lock.acquireSharedLock();
    try {

      final OCluster cluster = clusters[iClusterId];
      if (cluster == null)
        return 0;

      if (countTombstones)
        return cluster.getEntries();

      return cluster.getEntries() - cluster.getTombstonesCount();
    } finally {
      lock.releaseSharedLock();
    }
  }

  public OClusterPosition[] getClusterDataRange(final int iClusterId) {
    if (iClusterId == -1)
      return new OClusterPosition[] { OClusterPosition.INVALID_POSITION, OClusterPosition.INVALID_POSITION };

    checkOpeness();

    lock.acquireSharedLock();
    try {

      return clusters[iClusterId] != null ? new OClusterPosition[] { clusters[iClusterId].getFirstPosition(),
          clusters[iClusterId].getLastPosition() } : new OClusterPosition[0];

    } catch (IOException ioe) {
      throw new OStorageException("Can not retrieve information about data range", ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    checkOpeness();

    lock.acquireSharedLock();
    try {

      long tot = 0;

      for (int iClusterId : iClusterIds) {
        if (iClusterId >= clusters.length)
          throw new OConfigurationException("Cluster id " + iClusterId + " was not found in database '" + name + "'");

        if (iClusterId > -1) {
          final OCluster c = clusters[iClusterId];
          if (c != null)
            tot += c.getEntries() - (countTombstones ? 0L : c.getTombstonesCount());
        }
      }

      return tot;
    } finally {
      lock.releaseSharedLock();
    }
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final int dataSegmentId, final ORecordId rid,
      final byte[] content, final ORecordVersion recordVersion, final byte recordType, final int mode,
      final ORecordCallback<OClusterPosition> callback) {
    checkOpeness();

    final OLocalPaginatedCluster cluster = getClusterById(rid.clusterId);
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
            ppos = cluster.createRecord(content, recordVersion, recordType);
            rid.clusterPosition = ppos.clusterPosition;

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

  public void changeRecordIdentity(ORID originalId, ORID newId) {
    throw new UnsupportedOperationException("changeRecordIdentity");
  }

  @Override
  public boolean isHashClustersAreUsed() {
    return OGlobalConfiguration.USE_LHPEPS_CLUSTER.getValueAsBoolean();
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones) {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecord(getClusterById(iRid.clusterId), iRid, true, loadTombstones));
  }

  @Override
  protected ORawBuffer readRecord(final OCluster clusterSegment, final ORecordId rid, boolean atomicLock, boolean loadTombstones) {
    checkOpeness();

    if (!rid.isPersistent())
      throw new IllegalArgumentException("Cannot read record " + rid + " since the position is invalid in database '" + name + '\'');

    final OLocalPaginatedCluster localPaginatedCluster = (OLocalPaginatedCluster) clusterSegment;
    localPaginatedCluster.getExternalModificationLock().requestModificationLock();
    try {
      if (atomicLock)
        lock.acquireSharedLock();

      try {
        lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.SHARED);
        try {
          return localPaginatedCluster.readRecord(rid.clusterPosition);
        } finally {
          lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.SHARED);
        }

      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on reading record " + rid + " (cluster: " + clusterSegment + ')', e);
        return null;
      } finally {
        if (atomicLock)
          lock.releaseSharedLock();
      }
    } finally {
      localPaginatedCluster.getExternalModificationLock().releaseModificationLock();
    }
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId rid, final byte[] content,
      final ORecordVersion version, final byte recordType, final int mode, ORecordCallback<ORecordVersion> callback) {
    checkOpeness();

    final OLocalPaginatedCluster cluster = getClusterById(rid.clusterId);

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

            cluster.updateRecord(rid.clusterPosition, content, ppos.recordVersion, recordType);

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
    }
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId rid, final ORecordVersion version, final int mode,
      ORecordCallback<Boolean> callback) {
    checkOpeness();

    final OLocalPaginatedCluster cluster = getClusterById(rid.clusterId);

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

            cluster.deleteRecord(ppos.clusterPosition);

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
        return super.callInLock(iCallable, iExclusiveLock);
      } finally {
        modificationLock.releaseModificationLock();
      }
    } else {
      return super.callInLock(iCallable, iExclusiveLock);
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

  public void commit(final OTransaction tx) {
    throw new UnsupportedOperationException("commit");
  }

  public void rollback(final OTransaction iTx) {
    throw new UnsupportedOperationException("rollback");
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return ppos != null && !ppos.recordVersion.isTombstone();
  }

  public void synch() {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {
      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.synch();

      if (configuration != null)
        configuration.synch();

    } catch (IOException e) {
      throw new OStorageException("Error on synch storage '" + name + "'", e);

    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a local database", timer, "db.*.synch");
    }
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
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

  public OLocalPaginatedCluster getClusterById(int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      checkClusterSegmentIndexRange(iClusterId);

      final OLocalPaginatedCluster cluster = clusters[iClusterId];
      if (cluster == null)
        throw new IllegalArgumentException("Cluster " + iClusterId + " is null");

      return cluster;
    } finally {
      lock.releaseSharedLock();
    }
  }

  public OWriteAheadLog getWALInstance() {
    return writeAheadLog;
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId > clusters.length - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  @Override
  public OCluster getClusterByName(final String iClusterName) {
    lock.acquireSharedLock();
    try {

      final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());

      if (cluster == null)
        throw new IllegalArgumentException("Cluster " + iClusterName + " does not exist in database '" + name + "'");
      return cluster;

    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public String getURL() {
    return OEngineLocalPaginated.NAME + ":" + url;
  }

  public long getSize() {
    lock.acquireSharedLock();
    try {

      long size = 0;

      for (OCluster c : clusters)
        if (c != null)
          size += c.getRecordsSize();

      return size;

    } catch (IOException ioe) {
      throw new OStorageException("Can not calculate records size");
    } finally {
      lock.releaseSharedLock();
    }
  }

  public String getStoragePath() {
    return storagePath;
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

  public String getMode() {
    return mode;
  }

  public OStorageVariableParser getVariableParser() {
    return variableParser;
  }

  public int getClusters() {
    lock.acquireSharedLock();
    try {

      return clusterMap.size();

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
        if (c != null)
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

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, iMode, callback).getResult();
  }

  public void freeze(boolean throwException) {
    modificationLock.prohibitModifications(throwException);
    synch();

    try {
      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.setSoftlyClosed(true);

      if (configuration != null)
        configuration.setSoftlyClosed(true);

    } catch (IOException e) {
      throw new OStorageException("Error on freeze storage '" + name + "'", e);
    }
  }

  public void release() {
    try {
      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.setSoftlyClosed(false);

      if (configuration != null)
        configuration.setSoftlyClosed(false);

    } catch (IOException e) {
      throw new OStorageException("Error on release storage '" + name + "'", e);
    }

    modificationLock.allowModifications();
  }

  public boolean isClusterSoftlyClosed(String clusterName) {
    lock.acquireSharedLock();
    try {
      final OCluster indexCluster = clusterMap.get(clusterName);
      return !(indexCluster instanceof OClusterLocal) || ((OClusterLocal) indexCluster).isSoftlyClosed();
    } finally {
      lock.releaseSharedLock();
    }
  }

  public void makeFuzzyCheckpoint() {
    if (writeAheadLog == null)
      return;

    try {
      lock.acquireExclusiveLock();
      try {
        writeAheadLog.logFuzzyCheckPointStart();
        diskCache.forceSyncStoredChanges();
        diskCache.logDirtyPagesTable();

        for (OLocalPaginatedCluster cluster : clusters)
          if (cluster != null)
            cluster.flushClusterState();

        writeAheadLog.logFuzzyCheckPointEnd();
        writeAheadLog.flush();
      } finally {
        lock.releaseExclusiveLock();
      }
    } catch (IOException ioe) {
      throw new OStorageException("Error during fuzzy checkpoint creation for storage " + name, ioe);
    }
  }

  public void makeFullCheckpoint() {
    if (writeAheadLog == null)
      return;

    lock.acquireExclusiveLock();
    try {
      writeAheadLog.logFullCheckpointStart();

      for (OLocalPaginatedCluster cluster : clusters)
        if (cluster != null)
          cluster.synch();

      writeAheadLog.logFullCheckpointEnd();
      writeAheadLog.flush();
    } catch (IOException ioe) {
      throw new OStorageException("Error during checkpoint creation for storage " + name, ioe);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void scheduleFullCheckpoint() {
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
    return OEngineLocal.NAME;
  }

  private int createClusterFromConfig(final OStorageClusterConfiguration iConfig) throws IOException {
    OLocalPaginatedCluster cluster = clusterMap.get(iConfig.getName());

    if (cluster != null) {
      cluster.configure(this, iConfig);
      return -1;
    }

    cluster = new OLocalPaginatedCluster();
    cluster.configure(this, iConfig);

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
  private int registerCluster(final OLocalPaginatedCluster iCluster) throws IOException {
    final int id;

    if (iCluster != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (clusterMap.containsKey(iCluster.getName()))
        throw new OConfigurationException("Cannot add segment '" + iCluster.getName()
            + "' because it is already registered in database '" + name + "'");
      // CREATE AND ADD THE NEW REF SEGMENT
      clusterMap.put(iCluster.getName(), iCluster);
      id = iCluster.getId();
    } else {
      id = clusters.length;
    }

    if (id >= clusters.length) {
      clusters = OArrays.copyOf(clusters, id + 1);
    }
    clusters[id] = iCluster;

    return id;
  }

  private void addDefaultClusters() throws IOException {
    createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, -1,
        OMetadata.CLUSTER_INTERNAL_NAME));
    configuration.load();

    createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, -1,
        OMetadata.CLUSTER_INDEX_NAME));

    createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, -1,
        OMetadata.CLUSTER_MANUAL_INDEX_NAME));

    defaultClusterId = createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, -1,
        CLUSTER_DEFAULT_NAME));
  }

  public ODiskCache getDiskCache() {
    return diskCache;
  }

  public void freeze(boolean throwException, int clusterId) {
    final OLocalPaginatedCluster cluster = getClusterById(clusterId);

    final String name = cluster.getName();
    if (OMetadata.CLUSTER_INDEX_NAME.equals(name) || OMetadata.CLUSTER_MANUAL_INDEX_NAME.equals(name)) {
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
    final OLocalPaginatedCluster cluster = getClusterById(clusterId);

    final String name = cluster.getName();
    if (OMetadata.CLUSTER_INDEX_NAME.equals(name) || OMetadata.CLUSTER_MANUAL_INDEX_NAME.equals(name)) {
      throw new IllegalArgumentException("It is impossible to freeze and release index or manualindex cluster!");
    }

    try {
      cluster.setSoftlyClosed(false);
    } catch (IOException e) {
      throw new OStorageException("Error on unfreeze storage '" + name + "'", e);
    }

    cluster.getExternalModificationLock().allowModifications();

  }
}
