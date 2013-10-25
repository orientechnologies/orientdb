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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.lock.OLockManager.LOCK;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.*;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.engine.OLocalHashTableIndexEngine;
import com.orientechnologies.orient.core.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteDiskCache;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.fs.OMMapManagerLocator;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

public class OStorageLocal extends OStorageLocalAbstract {
  private final int                     DELETE_MAX_RETRIES;
  private final int                     DELETE_WAIT_TIME;
  private final Map<String, OCluster>   clusterMap                = new LinkedHashMap<String, OCluster>();
  private OCluster[]                    clusters                  = new OCluster[0];
  private ODataLocal[]                  dataSegments              = new ODataLocal[0];
  private final OStorageLocalTxExecuter txManager;
  private String                        storagePath;
  private final OStorageVariableParser  variableParser;
  private int                           defaultClusterId          = -1;

  private static String[]               ALL_FILE_EXTENSIONS       = { "ocf", ".och", ".ocl", ".oda", ".odh", ".otx", ".ocs",
      ".oef", ".oem", OWriteAheadLog.MASTER_RECORD_EXTENSION, OWriteAheadLog.WAL_SEGMENT_EXTENSION,
      OLocalHashTableIndexEngine.BUCKET_FILE_EXTENSION, OLocalHashTableIndexEngine.METADATA_FILE_EXTENSION,
      OLocalHashTableIndexEngine.TREE_FILE_EXTENSION, OSBTreeIndexEngine.DATA_FILE_EXTENSION };

  private long                          positionGenerator         = 1;
  private OModificationLock             modificationLock          = new OModificationLock();
  private final Set<String>             clustersToSyncImmediately = new HashSet<String>();

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

    storagePath = OIOUtils.getPathFromDatabaseName(storagePath);

    variableParser = new OStorageVariableParser(storagePath);
    configuration = new OStorageConfigurationSegment(this);
    txManager = new OStorageLocalTxExecuter(this, configuration.txSegment);

    DELETE_MAX_RETRIES = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();
    DELETE_WAIT_TIME = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();

    final String[] clustersToSync = OGlobalConfiguration.NON_TX_CLUSTERS_SYNC_IMMEDIATELY.getValueAsString().trim()
        .split("\\s*,\\s*");
    clustersToSyncImmediately.addAll(Arrays.asList(clustersToSync));

    installProfilerHooks();

    long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
    long writeCacheSize = (long) Math.floor((((double) OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0)
        * diskCacheSize);
    long readCacheSize = diskCacheSize - writeCacheSize;

    diskCache = new OReadWriteDiskCache(readCacheSize, writeCacheSize,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_TTL.getValueAsLong() * 1000,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL.getValueAsInteger(), this, null, false, true);
  }

  public synchronized void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
    final long timer = Orient.instance().getProfiler().startChrono();

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
      pos = registerDataSegment(new OStorageDataConfiguration(configuration, OStorage.DATA_DEFAULT_NAME, 0, getStoragePath()));
      dataSegments[pos].open();

      addDefaultClusters();

      // REGISTER DATA SEGMENT
      for (int i = 0; i < configuration.dataSegments.size(); ++i) {
        final OStorageDataConfiguration dataConfig = configuration.dataSegments.get(i);

        if (dataConfig == null)
          continue;
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
              if (clusters[i] != null && clusters[i] instanceof OClusterLocal)
                clusters[i].close();

              clusters[i] = Orient.instance().getClusterFactory().createCluster(OClusterLocal.TYPE);
              clusters[i].configure(this, clusterConfig);
              clusterMap.put(clusters[i].getName(), clusters[i]);
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

      if (OGlobalConfiguration.USE_WAL.getValueAsBoolean())
        writeAheadLog = new OWriteAheadLog(this);

      txManager.open();

    } catch (Exception e) {
      close(true);
      throw new OStorageException("Cannot open local storage '" + url + "' with mode=" + mode, e);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".open", "Open a database", timer, "db.*.open");
    }
  }

  public ODiskCache getDiskCache() {
    return diskCache;
  }

  private void addDefaultClusters() throws IOException {
    createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, 0,
        OMetadataDefault.CLUSTER_INTERNAL_NAME));
    configuration.load();

    createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, 0,
        OMetadataDefault.CLUSTER_INDEX_NAME));

    createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, 0,
        OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME));

    defaultClusterId = createClusterFromConfig(new OStoragePhysicalClusterConfigurationLocal(configuration, clusters.length, 0,
        CLUSTER_DEFAULT_NAME));
  }

  public void create(final Map<String, Object> iProperties) {
    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {

      if (status != STATUS.CLOSED)
        throw new OStorageException("Cannot create new storage '" + name + "' because it is not closed (status:" + status + ")");

      addUser();

      final File storageFolder = new File(storagePath);
      if (!storageFolder.exists())
        storageFolder.mkdir();

      if (exists())
        throw new OStorageException("Cannot create new storage '" + name + "' because it already exists");

      status = STATUS.OPEN;

      addDataSegment(OStorage.DATA_DEFAULT_NAME);
      addDataSegment(OMetadataDefault.DATASEGMENT_INDEX_NAME);

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_INTERNAL_NAME, null, null, true);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING IN THE INDEX DATA SEGMENT
      addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_INDEX_NAME, null,
          OMetadataDefault.DATASEGMENT_INDEX_NAME, true);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
      addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null, null, true);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), CLUSTER_DEFAULT_NAME, null, null, false);

      configuration.create();

      writeAheadLog = new OWriteAheadLog(this);

      txManager.create();
    } catch (OStorageException e) {
      close();
      throw e;
    } catch (IOException e) {
      close();
      throw new OStorageException("Error on creation of storage '" + name + "'", e);

    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".create", "Create a database", timer, "db.*.create");
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
    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {

      if (!checkForClose(iForce))
        return;

      status = STATUS.CLOSING;

      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.close();
      clusters = new OCluster[0];
      clusterMap.clear();

      for (ODataLocal data : dataSegments)
        if (data != null)
          data.close();
      dataSegments = new ODataLocal[0];

      txManager.close();

      if (configuration != null)
        configuration.close();

      level2Cache.shutdown();

      OMMapManagerLocator.getInstance().flush();

      super.close(iForce);
      uninstallProfilerHooks();

      if (diskCache != null)
        diskCache.close();

      if (writeAheadLog != null) {
        writeAheadLog.shrinkTill(writeAheadLog.end());
        writeAheadLog.close();
      }

      Orient.instance().unregisterStorage(this);
      status = STATUS.CLOSED;
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on closing of storage '" + name, e, OStorageException.class);

    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".close", "Close a database", timer, "db.*.close");
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

    final long timer = Orient.instance().getProfiler().startChrono();

    // GET REAL DIRECTORY
    File dbDir = new File(OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url)));
    if (!dbDir.exists() || !dbDir.isDirectory())
      dbDir = dbDir.getParentFile();

    lock.acquireExclusiveLock();
    try {

      if (diskCache != null)
        diskCache.delete();

      // RETRIES
      for (int i = 0; i < DELETE_MAX_RETRIES; ++i) {
        if (dbDir.exists() && dbDir.isDirectory()) {
          int notDeletedFiles = 0;

          // TRY TO DELETE ALL THE FILES
          File[] files = dbDir.listFiles();
          if (files != null) {
            for (File f : files) {
              // DELETE ONLY THE SUPPORTED FILES
              for (String ext : ALL_FILE_EXTENSIONS)
                if (f.getPath().endsWith(ext)) {
                  if (!f.delete()) {
                    notDeletedFiles++;
                  }
                  break;
                }
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

    } catch (IOException ioe) {
      throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ".", ioe);
    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".drop", "Drop a database", timer, "db.*.drop");
    }
  }

  public boolean check(final boolean iVerbose, final OCommandOutputListener iListener) {
    int errors = 0;
    int warnings = 0;

    lock.acquireSharedLock();
    try {
      long totalRecors = 0;
      final long start = System.currentTimeMillis();

      formatMessage(iVerbose, iListener, "\nChecking database '" + getName() + "'...\n");

      formatMessage(iVerbose, iListener, "\n(1) Checking data-clusters. This activity checks if pointers to data are coherent.");

      final OPhysicalPosition ppos = new OPhysicalPosition();

      // BROWSE ALL THE CLUSTERS
      for (OCluster c : clusters) {
        if (!(c instanceof OClusterLocal))
          continue;

        formatMessage(iVerbose, iListener, "\n- data-cluster #%-5d %s -> ", c.getId(), c.getName());

        // BROWSE ALL THE RECORDS
        for (final OClusterEntryIterator it = c.absoluteIterator(); it.hasNext();) {
          final OPhysicalPosition physicalPosition = it.next();
          totalRecors++;
          try {

            if (physicalPosition.dataSegmentId >= dataSegments.length) {
              formatMessage(iVerbose, iListener, "WARN: Found wrong data segment %d ", physicalPosition.dataSegmentId);
              warnings++;
            }

            if (physicalPosition.recordSize < 0) {
              formatMessage(iVerbose, iListener, "WARN: Found wrong record size %d ", physicalPosition.recordSize);
              warnings++;
            }

            if (physicalPosition.recordSize >= 1000000) {
              formatMessage(iVerbose, iListener, "WARN: Found suspected big record size %d. Is it corrupted? ",
                  physicalPosition.recordSize);
              warnings++;
            }

            if (physicalPosition.dataSegmentPos > dataSegments[physicalPosition.dataSegmentId].getFilledUpTo()) {
              formatMessage(iVerbose, iListener, "WARN: Found wrong pointer to data chunk %d out of data segment size (%d) ",
                  physicalPosition.dataSegmentPos, dataSegments[physicalPosition.dataSegmentId].getFilledUpTo());
              warnings++;
            }

            if (physicalPosition.recordVersion.isTombstone() && (c instanceof OClusterLocal)) {
              // CHECK IF THE HOLE EXISTS
              boolean found = false;
              int tot = ((OClusterLocal) c).holeSegment.getHoles();
              for (int i = 0; i < tot; ++i) {
                final long recycledPosition = ((OClusterLocal) c).holeSegment.getEntryPosition(i) / OClusterLocal.RECORD_SIZE;
                if (recycledPosition == physicalPosition.clusterPosition.longValue()) {
                  // FOUND
                  found = true;
                  break;
                }
              }

              if (!found) {
                formatMessage(iVerbose, iListener, "WARN: Cannot find hole for deleted record %d:%d ", c.getId(),
                    physicalPosition.clusterPosition);
                warnings++;
              }
            }
          } catch (IOException e) {
            formatMessage(iVerbose, iListener, "WARN: Error while reading record #%d:%d ", e, c.getId(), ppos.clusterPosition);
            warnings++;
          }
        }

        if (c instanceof OClusterLocal) {
          final int totalHoles = ((OClusterLocal) c).holeSegment.getHoles();
          if (totalHoles > 0) {
            formatMessage(iVerbose, iListener, " [found " + totalHoles + " hole(s)]");
            // CHECK HOLES
            for (int i = 0; i < totalHoles; ++i) {
              long recycledPosition = -1;
              try {
                ppos.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(((OClusterLocal) c).holeSegment.getEntryPosition(i)
                    / OClusterLocal.RECORD_SIZE);
                OPhysicalPosition physicalPosition = c.getPhysicalPosition(ppos);

                if (physicalPosition != null && !physicalPosition.recordVersion.isTombstone()) {
                  formatMessage(iVerbose, iListener,
                      "WARN: Found wrong hole %d/%d for deleted record %d:%d. The record seems good ", i, totalHoles - 1,
                      c.getId(), recycledPosition);
                  warnings++;
                }
              } catch (Exception e) {
                formatMessage(iVerbose, iListener, "WARN: Found wrong hole %d/%d for deleted record %d:%d. The record not exists ",
                    i, totalHoles - 1, c.getId(), recycledPosition);
                warnings++;
              }
            }
          }
        }

        formatMessage(iVerbose, iListener, "OK");
      }

      int totalChunks = 0;
      formatMessage(iVerbose, iListener,
          "\n\n(2) Checking data chunks integrity. In this phase data segments are scanned to check the back reference into the clusters.");

      for (ODataLocal d : dataSegments) {
        if (d == null)
          continue;

        formatMessage(iVerbose, iListener, "\n- data-segment %s (id=%d) size=%d/%d...", d.getName(), d.getId(), d.getFilledUpTo(),
            d.getSize(), d.getHoles());

        int nextPos = 0;

        // GET DATA-SEGMENT HOLES
        final List<ODataHoleInfo> holes = d.getHolesList();
        if (iVerbose) {
          formatMessage(iVerbose, iListener, "\n-- found %d holes:", holes.size());

          for (ODataHoleInfo hole : holes)
            formatMessage(iVerbose, iListener, "\n--- hole #%-7d offset=%-10d size=%-7d", hole.holeOffset, hole.dataOffset,
                hole.size);
        }

        // CHECK CHUNKS
        formatMessage(iVerbose, iListener, "\n-- checking chunks:");

        int pos;
        do {
          try {
            pos = nextPos;

            // SEARCH IF THE RECORD IT'S BETWEEN HOLES
            ODataHoleInfo foundHole = null;
            for (ODataHoleInfo hole : holes) {
              if (hole.dataOffset == pos) {
                // HOLE FOUND!
                foundHole = hole;
                break;
              }
            }

            int recordSize = d.getRecordSize(pos);

            formatMessage(iVerbose, iListener, "\n--- chunk #%-7d offset=%-10d size=%-7d -> ", totalChunks, pos, recordSize);

            if (recordSize < 0) {
              recordSize *= -1;

              // HOLE: CHECK HOLE PRESENCE
              if (foundHole != null) {
                if (foundHole.size != recordSize) {
                  formatMessage(iVerbose, iListener,
                      "WARN: Chunk %s:%d (offset=%d size=%d) differs in size with the hole size %d ", d.getName(), totalChunks,
                      pos, recordSize, foundHole.size);
                  warnings++;
                }

                nextPos = pos + foundHole.size;
              } else {
                formatMessage(iVerbose, iListener, "WARN: Chunk %s:%d (offset=%d size=%d) has no hole for deleted chunk ",
                    d.getName(), totalChunks, pos, recordSize);
                warnings++;

                nextPos = pos + recordSize;
              }
            } else {

              if (foundHole != null) {
                formatMessage(
                    iVerbose,
                    iListener,
                    "WARN: Chunk %s:%d (offset=%d size=%d) it's between the holes (hole #%d) even if has no negative recordSize. Jump the content ",
                    d.getName(), totalChunks, pos, recordSize, foundHole.holeOffset);
                warnings++;
                nextPos = pos + foundHole.size;
              } else {
                // REGULAR DATA CHUNK
                nextPos = pos + ODataLocal.RECORD_FIX_SIZE + recordSize;

                final byte[] buffer = d.getRecord(pos);
                if (buffer.length != recordSize) {
                  formatMessage(iVerbose, iListener,
                      "WARN: Chunk %s:%d (offset=%d size=%d) has wrong record size because the record length is %d ", d.getName(),
                      totalChunks, pos, recordSize, buffer.length);
                  warnings++;
                }

                final ORecordId rid = d.getRecordRid(pos);
                if (!rid.isValid()) {
                  formatMessage(iVerbose, iListener, "WARN: Chunk %s:%d (offset=%d size=%d) points to invalid RID %s ",
                      d.getName(), totalChunks, pos, recordSize, rid);
                  warnings++;
                } else {
                  if (rid.clusterId >= clusters.length) {
                    formatMessage(
                        iVerbose,
                        iListener,
                        "WARN: Chunk %s:%d (offset=%d size=%d) has invalid RID because points to %s but configured clusters are %d in total ",
                        d.getName(), totalChunks, pos, recordSize, rid, clusters.length);
                    warnings++;

                  } else if (clusters[rid.clusterId] == null) {
                    formatMessage(
                        iVerbose,
                        iListener,
                        "WARN: Chunk %s:%d (offset=%d size=%d) has invalid RID because points to %s but the cluster %d not exists ",
                        d.getName(), totalChunks, pos, recordSize, rid, rid.clusterId);
                    warnings++;
                  } else {
                    ppos.clusterPosition = rid.clusterPosition;
                    clusters[rid.clusterId].getPhysicalPosition(ppos);

                    if (ppos.dataSegmentId != d.getId()) {
                      formatMessage(
                          iVerbose,
                          iListener,
                          "WARN: Chunk %s:%d (offset=%d size=%d) point to the RID %d but it doesn't point to current data segment %d but to %d ",
                          d.getName(), totalChunks, pos, recordSize, rid, d.getId(), ppos.dataSegmentId);
                      warnings++;
                    }

                    if (ppos.dataSegmentPos != pos) {
                      formatMessage(
                          iVerbose,
                          iListener,
                          "WARN: Chunk %s:%d (offset=%d size=%d) point to the RID %d but it doesn't point to current chunk %d but to %d ",
                          d.getName(), totalChunks, pos, recordSize, rid, ppos.dataSegmentPos, pos);
                      warnings++;
                    }
                  }
                }
              }
            }
            totalChunks++;

            formatMessage(iVerbose, iListener, "OK");

          } catch (Exception e) {
            iListener.onMessage("ERROR: " + e.toString());
            // OLogManager.instance().warn(this, "ERROR: Chunk %s:%d (offset=%d) error: %s", e, d.getName(),
            // totalChunks, pos, e.toString());
            errors++;
          }
        } while (nextPos < d.getFilledUpTo());
        formatMessage(iVerbose, iListener, "\n");
      }

      iListener.onMessage("\nCheck of database completed in " + (System.currentTimeMillis() - start)
          + "ms:\n- Total records checked: " + totalRecors + "\n- Total chunks checked.: " + totalChunks
          + "\n- Warnings.............: " + warnings + "\n- Errors...............: " + errors + "\n");

    } finally {
      lock.releaseSharedLock();
    }

    return errors == 0;
  }

  public ODataLocal getDataSegmentById(final int iDataSegmentId) {
    checkOpeness();

    lock.acquireSharedLock();
    try {

      if (iDataSegmentId >= dataSegments.length)
        throw new IllegalArgumentException("Data segment #" + iDataSegmentId + " does not exist in database '" + name + "'");

      return dataSegments[iDataSegmentId];

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    if (iDataSegmentName == null)
      return 0;

    checkOpeness();

    lock.acquireSharedLock();
    try {

      for (ODataLocal d : dataSegments) {
        if (d != null && d.getName().equalsIgnoreCase(iDataSegmentName))
          return d.getId();
      }
      throw new IllegalArgumentException("Data segment '" + iDataSegmentName + "' does not exist in database '" + name + "'");

    } finally {
      lock.releaseSharedLock();
    }
  }

  /**
   * Add a new data segment in the default segment directory and with filename equals to the cluster name.
   */
  public int addDataSegment(final String iDataSegmentName) {
    return addDataSegment(iDataSegmentName, null);
  }

  public int addDataSegment(String iSegmentName, final String iDirectory) {
    checkOpeness();

    iSegmentName = iSegmentName.toLowerCase();

    lock.acquireExclusiveLock();
    try {

      final OStorageDataConfiguration conf = new OStorageDataConfiguration(configuration, iSegmentName, -1, iDirectory);

      final int pos = registerDataSegment(conf);

      if (pos == -1)
        throw new OConfigurationException("Cannot add segment " + conf.name + " because it is already part of storage '" + name
            + "'");

      dataSegments[pos].create(-1);

      // UPDATE CONFIGURATION
      conf.id = pos;
      if (pos == configuration.dataSegments.size())
        configuration.dataSegments.add(conf);
      else
        configuration.dataSegments.set(pos, conf);
      configuration.update();

      return pos;
    } catch (Throwable e) {
      OLogManager.instance().error(this, "Error on creation of new data segment '" + iSegmentName + "' in: " + iDirectory, e,
          OStorageException.class);
      return -1;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Add a new cluster into the storage. Type can be: "physical" or "logical".
   */
  public int addCluster(final String iClusterType, String iClusterName, final String iLocation, final String iDataSegmentName,
      boolean forceListBased, final Object... iParameters) {
    checkOpeness();

    lock.acquireExclusiveLock();
    try {
      final OCluster cluster;
      if (iClusterName != null) {
        iClusterName = iClusterName.toLowerCase();

        // FIND THE FIRST AVAILABLE CLUSTER ID
        int clusterPos = clusters.length;
        for (int i = 0; i < clusters.length; ++i)
          if (clusters[i] == null) {
            clusterPos = i;
            break;
          }

        cluster = Orient.instance().getClusterFactory().createCluster(iClusterType);
        cluster.configure(this, clusterPos, iClusterName, iLocation, getDataSegmentIdByName(iDataSegmentName), iParameters);
      } else
        cluster = null;

      final int clusterId = registerCluster(cluster);

      if (cluster != null) {
        cluster.create(-1);
        configuration.update();
      }

      return clusterId;

    } catch (Exception e) {
      OLogManager.instance().exception("Error in creation of new cluster '" + iClusterName + "' of type: " + iClusterType, e,
          OStorageException.class);
    } finally {
      lock.releaseExclusiveLock();
    }

    return -1;
  }

  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters) {
    throw new UnsupportedOperationException("This operation is unsupported for " + getType()
        + " storage. If you are doing import please use parameter -preserveClusterIDs=false .");
  }

  public ODataLocal[] getDataSegments() {
    return dataSegments;
  }

  public OStorageLocalTxExecuter getTxManager() {
    return txManager;
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

      return true;
    } catch (Exception e) {
      OLogManager.instance().exception("Error while removing cluster '" + iClusterId + "'", e, OStorageException.class);

    } finally {
      lock.releaseExclusiveLock();
    }

    return false;
  }

  public boolean dropDataSegment(final String iName) {
    lock.acquireExclusiveLock();
    try {

      final int id = getDataSegmentIdByName(iName);
      final ODataLocal data = dataSegments[id];
      if (data == null)
        return false;

      data.drop();

      dataSegments[id] = null;

      // UPDATE CONFIGURATION
      configuration.dropDataSegment(id);

      return true;
    } catch (Exception e) {
      OLogManager.instance().exception("Error while removing data segment '" + iName + "'", e, OStorageException.class);

    } finally {
      lock.releaseExclusiveLock();
    }

    return false;
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

  public OStorageOperationResult<OPhysicalPosition> createRecord(int iDataSegmentId, final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iRecordVersion, final byte iRecordType, final int iMode, ORecordCallback<OClusterPosition> iCallback) {
    checkOpeness();

    final OCluster cluster = getClusterById(iRid.clusterId);
    final ODataLocal dataSegment = getDataSegmentById(iDataSegmentId);

    final OPhysicalPosition ppos;
    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        if (txManager.isCommitting()) {
          ppos = txManager.createRecord(txManager.getCurrentTransaction().getId(), dataSegment, cluster, iRid, iContent,
              iRecordVersion, iRecordType);
        } else {
          ppos = createRecord(dataSegment, cluster, iContent, iRecordType, iRid, iRecordVersion);
          if (OGlobalConfiguration.NON_TX_RECORD_UPDATE_SYNCH.getValueAsBoolean()
              || clustersToSyncImmediately.contains(cluster.getName()))
            synchRecordUpdate(cluster, ppos);
          if (iCallback != null)
            iCallback.call(iRid, ppos.clusterPosition);
        }
      } finally {
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }

    return new OStorageOperationResult<OPhysicalPosition>(ppos);
  }

  public boolean updateReplica(final int dataSegmentId, final ORecordId rid, final byte[] content,
      final ORecordVersion recordVersion, final byte recordType) throws IOException {
    if (rid.isNew())
      throw new OStorageException("Passed record with id " + rid + " is new and can not be treated as replica.");

    checkOpeness();

    final OCluster cluster = getClusterById(rid.clusterId);
    final ODataLocal dataSegment = getDataSegmentById(dataSegmentId);

    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        lockManager.acquireLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
        try {
          OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
          if (ppos == null) {
            if (!cluster.isHashBased())
              throw new OStorageException("Cluster with LH support is required.");

            ppos = new OPhysicalPosition(rid.clusterPosition, recordVersion);

            ppos.recordType = recordType;
            ppos.dataSegmentId = dataSegment.getId();

            if (!recordVersion.isTombstone()) {
              ppos.dataSegmentPos = dataSegment.addRecord(rid, content);
            }

            cluster.addPhysicalPosition(ppos);
            return true;
          } else {
            if (ppos.recordType != recordType)
              throw new OStorageException("Record types of provided and stored replicas are different " + recordType + ":"
                  + ppos.recordType + ".");

            if (ppos.recordVersion.compareTo(recordVersion) < 0) {
              cluster.updateVersion(ppos.clusterPosition, recordVersion);

              if (!recordVersion.isTombstone() && !ppos.recordVersion.isTombstone()) {
                ppos.dataSegmentPos = dataSegment.setRecord(ppos.dataSegmentPos, rid, content);
                cluster.updateDataSegmentPosition(ppos.clusterPosition, dataSegmentId, ppos.dataSegmentPos);
              } else if (!recordVersion.isTombstone() && ppos.recordVersion.isTombstone()) {
                ppos.dataSegmentPos = dataSegment.addRecord(rid, content);
                cluster.updateDataSegmentPosition(ppos.clusterPosition, dataSegmentId, ppos.dataSegmentPos);
              } else if (recordVersion.isTombstone() && !ppos.recordVersion.isTombstone()) {
                dataSegment.deleteRecord(ppos.dataSegmentPos);
              }

              return true;
            }
          }

        } finally {
          lockManager.releaseLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
        }
      } finally {
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }

    return false;
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
      if (exclusiveLock)
        lock.acquireExclusiveLock();
      else
        lock.acquireSharedLock();
      try {
        lockManager.acquireLock(Thread.currentThread(), rid, exclusiveLock ? LOCK.EXCLUSIVE : LOCK.SHARED);
        try {
          return callable.call();
        } finally {
          lockManager.releaseLock(Thread.currentThread(), rid, exclusiveLock ? LOCK.EXCLUSIVE : LOCK.SHARED);
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new OException("Error on nested call in lock", e);
      } finally {
        if (exclusiveLock)
          lock.releaseExclusiveLock();
        else
          lock.releaseSharedLock();
      }
    } finally {
      if (exclusiveLock)
        modificationLock.releaseModificationLock();
    }
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones) {
    checkOpeness();
    return new OStorageOperationResult<ORawBuffer>(readRecord(getClusterById(iRid.clusterId), iRid, true, loadTombstones));
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, final int iMode, ORecordCallback<ORecordVersion> iCallback) {
    checkOpeness();

    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        final OCluster cluster = getClusterById(iRid.clusterId);
        if (txManager.isCommitting()) {
          return new OStorageOperationResult<ORecordVersion>(txManager.updateRecord(txManager.getCurrentTransaction().getId(),
              cluster, iRid, iContent, iVersion, iRecordType));
        } else {
          final OPhysicalPosition ppos = updateRecord(cluster, iRid, iContent, iVersion, iRecordType);

          if (ppos != null
              && (OGlobalConfiguration.NON_TX_RECORD_UPDATE_SYNCH.getValueAsBoolean() || clustersToSyncImmediately.contains(cluster
                  .getName())))
            synchRecordUpdate(cluster, ppos);

          final ORecordVersion returnValue = (ppos != null ? ppos.recordVersion : OVersionFactory.instance()
              .createUntrackedVersion());

          if (iCallback != null)
            iCallback.call(iRid, returnValue);

          return new OStorageOperationResult<ORecordVersion>(returnValue);
        }
      } finally {
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final ORecordVersion iVersion, final int iMode,
      ORecordCallback<Boolean> iCallback) {
    checkOpeness();

    final OCluster cluster = getClusterById(iRid.clusterId);

    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        if (txManager.isCommitting()) {
          return new OStorageOperationResult<Boolean>(txManager.deleteRecord(txManager.getCurrentTransaction().getId(), cluster,
              iRid.clusterPosition, iVersion));
        } else {
          final OPhysicalPosition ppos = deleteRecord(cluster, iRid, iVersion,
              OGlobalConfiguration.STORAGE_USE_TOMBSTONES.getValueAsBoolean());

          if (ppos != null
              && (OGlobalConfiguration.NON_TX_RECORD_UPDATE_SYNCH.getValueAsBoolean() || clustersToSyncImmediately.contains(cluster
                  .getName())))
            synchRecordUpdate(cluster, ppos);

          final boolean returnValue = ppos != null;

          if (iCallback != null)
            iCallback.call(iRid, returnValue);

          return new OStorageOperationResult<Boolean>(returnValue);
        }
      } finally {
        lock.releaseExclusiveLock();
      }
    } finally {
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

  public void commit(final OTransaction iTx, Runnable callback) {
    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        try {
          startStorageTx(iTx);
          txManager.clearLogEntries(iTx);
          txManager.commitAllPendingRecords(iTx);

          if (callback != null)
            callback.run();

          if (OGlobalConfiguration.TX_COMMIT_SYNCH.getValueAsBoolean())
            synch();

          endStorageTx();
        } catch (Exception e) {
          // WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
          OLogManager.instance().debug(this, "Error during transaction commit, transaction will be rolled back (tx-id=%d)", e,
              iTx.getId());
          rollback(iTx);
          if (e instanceof OException)
            throw ((OException) e);
          else
            throw new OStorageException("Error during transaction commit.", e);
        } finally {
          try {
            txManager.clearLogEntries(iTx);
            if (writeAheadLog != null)
              writeAheadLog.shrinkTill(writeAheadLog.end());
          } catch (Exception e) {
            // XXX WHAT CAN WE DO HERE ? ROLLBACK IS NOT POSSIBLE
            // IF WE THROW EXCEPTION, A ROLLBACK WILL BE DONE AT DB LEVEL BUT NOT AT STORAGE LEVEL
            OLogManager.instance().error(this, "Clear tx log entries failed", e);
          }
        }
      } finally {
        transaction = null;
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public void rollback(final OTransaction iTx) {
    modificationLock.requestModificationLock();
    try {
      lock.acquireExclusiveLock();
      try {
        txManager.getTxSegment().rollback(iTx);
        rollbackStorageTx();

        if (OGlobalConfiguration.TX_COMMIT_SYNCH.getValueAsBoolean())
          synch();
      } catch (IOException ioe) {
        OLogManager.instance().error(this,
            "Error executing rollback for transaction with id '" + iTx.getId() + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        transaction = null;
        lock.releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public void synch() {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {
      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.synch();

      for (ODataLocal data : dataSegments)
        if (data != null)
          data.synch();

      if (configuration != null)
        configuration.synch();

    } catch (IOException e) {
      throw new OStorageException("Error on synch storage '" + name + "'", e);

    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler().stopChrono("db." + name + ".synch", "Synch a database", timer, "db.*.synch");
    }
  }

  protected void synchRecordUpdate(final OCluster cluster, final OPhysicalPosition ppos) {
    checkOpeness();

    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireExclusiveLock();
    try {
      cluster.synch();

      final ODataLocal data = getDataSegmentById(ppos.dataSegmentId);
      data.synch();

      if (configuration != null)
        configuration.synch();

    } catch (IOException e) {
      throw new OStorageException("Error on synch storage '" + name + "'", e);

    } finally {
      lock.releaseExclusiveLock();

      Orient.instance().getProfiler()
          .stopChrono("db." + name + "record.synch", "Synch a record to database", timer, "db.*.record.synch");
    }
  }

  /**
   * Returns the list of holes as pair of position & ODataHoleInfo
   * 
   */
  public List<ODataHoleInfo> getHolesList() {
    final List<ODataHoleInfo> holes = new ArrayList<ODataHoleInfo>();

    lock.acquireSharedLock();
    try {

      for (ODataLocal d : dataSegments)
        if (d != null)
          holes.addAll(d.getHolesList());

      return holes;

    } finally {
      lock.releaseSharedLock();
    }
  }

  /**
   * Returns the total number of holes.
   * 
   */
  public long getHoles() {
    lock.acquireSharedLock();
    try {

      long holes = 0;
      for (ODataLocal d : dataSegments)
        if (d != null)
          holes += d.getHoles();
      return holes;

    } finally {
      lock.releaseSharedLock();
    }
  }

  /**
   * Returns the total size used by holes
   * 
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

  public void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    checkOpeness();

    if (iClusterId < 0)
      return null;

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
        throw new IllegalArgumentException("Cluster " + iClusterName + " does not exist in database '" + name + "'");
      return cluster;

    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public String getURL() {
    return OEngineLocal.NAME + ":" + url;
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

  protected int registerDataSegment(final OStorageDataConfiguration iConfig) throws IOException {
    checkOpeness();

    // CHECK FOR DUPLICATION OF NAMES
    for (ODataLocal data : dataSegments)
      if (data != null && data.getName().equals(iConfig.name)) {
        // OVERWRITE CONFIG
        data.config = iConfig;
        return -1;
      }

    int pos = -1;

    for (int i = 0; i < dataSegments.length; ++i)
      if (dataSegments[i] == null) {
        // RECYCLE POSITION
        pos = i;
        break;
      }

    if (pos == -1)
      // ASSIGN LATEST
      pos = dataSegments.length;

    // CREATE AND ADD THE NEW REF SEGMENT
    final ODataLocal segment = new ODataLocal(this, iConfig, pos);

    if (pos == dataSegments.length)
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
   */
  private int createClusterFromConfig(final OStorageClusterConfiguration iConfig) throws IOException {
    OCluster cluster = clusterMap.get(iConfig.getName());

    if (cluster instanceof OClusterLocal && iConfig instanceof OStorageEHClusterConfiguration)
      clusterMap.remove(iConfig.getName());
    else if (cluster != null) {
      if (cluster instanceof OClusterLocal) {
        // ALREADY CONFIGURED, JUST OVERWRITE CONFIG
        cluster.configure(this, iConfig);
      }
      return -1;
    }

    cluster = Orient.instance().getClusterFactory().createCluster(iConfig);
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
  private int registerCluster(final OCluster iCluster) throws IOException {
    final int id;

    if (iCluster != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (clusterMap.containsKey(iCluster.getName()))
        throw new OConfigurationException("Cannot add segment '" + iCluster.getName()
            + "' because it is already registered in database '" + name + "'");
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
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  protected OPhysicalPosition createRecord(final ODataLocal dataSegment, final OCluster cluster, final byte[] content,
      final byte recordType, final ORecordId rid, final ORecordVersion recordVersion) {
    assert (lock.assertExclusiveLockHold());
    checkOpeness();

    if (content == null)
      throw new IllegalArgumentException("Record is null");

    final long timer = Orient.instance().getProfiler().startChrono();

    final OPhysicalPosition ppos = new OPhysicalPosition(-1, -1, recordType);
    if (cluster.isHashBased()) {
      if (rid.isNew()) {
        if (OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.getValueAsBoolean()) {
          ppos.clusterPosition = OClusterPositionFactory.INSTANCE.generateUniqueClusterPosition();
        } else {
          ppos.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(positionGenerator++);
        }
      } else {
        ppos.clusterPosition = rid.clusterPosition;
      }
    }

    try {
      if (!cluster.addPhysicalPosition(ppos))
        throw new OStorageException("Record with given id " + new ORecordId(rid.clusterId, ppos.clusterPosition)
            + " already exists.");

      rid.clusterPosition = ppos.clusterPosition;

      lockManager.acquireLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
      try {
        ppos.dataSegmentId = dataSegment.getId();
        ppos.dataSegmentPos = dataSegment.addRecord(rid, content);

        cluster.updateDataSegmentPosition(ppos.clusterPosition, ppos.dataSegmentId, ppos.dataSegmentPos);

        if (recordVersion.getCounter() > 0 && recordVersion.compareTo(ppos.recordVersion) != 0) {
          // OVERWRITE THE VERSION
          cluster.updateVersion(rid.clusterPosition, recordVersion);
          ppos.recordVersion = recordVersion;
        }

        return ppos;
      } finally {
        lockManager.releaseLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
      }
    } catch (IOException ioe) {
      try {
        if (ppos.clusterPosition != null && ppos.clusterPosition.compareTo(OClusterPosition.INVALID_POSITION) != 0)
          cluster.removePhysicalPosition(ppos.clusterPosition);
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on removing physical position in cluster: " + cluster, e);
      }

      OLogManager.instance().error(this, "Error on creating record in cluster: " + cluster, ioe);
      return null;
    } finally {
      Orient.instance().getProfiler().stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer, "db.*.createRecord");
    }
  }

  @Override
  protected ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, boolean iAtomicLock, boolean loadTombstones) {
    if (!iRid.isPersistent())
      throw new IllegalArgumentException("Cannot read record " + iRid + " since the position is invalid in database '" + name
          + '\'');

    // NOT FOUND: SEARCH IT IN THE STORAGE
    final long timer = Orient.instance().getProfiler().startChrono();

    // GET LOCK ONLY IF IT'S IN ATOMIC-MODE (SEE THE PARAMETER iAtomicLock)
    // USUALLY BROWSING OPERATIONS (QUERY) AVOID ATOMIC LOCKING
    // TO IMPROVE PERFORMANCES BY LOCKING THE ENTIRE CLUSTER FROM THE
    // OUTSIDE.
    if (iAtomicLock)
      lock.acquireSharedLock();

    try {
      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.SHARED);
      try {
        final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));

        if (ppos != null && loadTombstones && ppos.recordVersion.isTombstone())
          return new ORawBuffer(null, ppos.recordVersion, ppos.recordType);

        if (ppos == null || !checkForRecordValidity(ppos))
          // DELETED
          return null;

        final ODataLocal data = getDataSegmentById(ppos.dataSegmentId);
        return new ORawBuffer(data.getRecord(ppos.dataSegmentPos), ppos.recordVersion, ppos.recordType);

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.SHARED);
      }

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on reading record " + iRid + " (cluster: " + iClusterSegment + ')', e);
      return null;
    } finally {
      if (iAtomicLock)
        lock.releaseSharedLock();

      Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
    }
  }

  protected OPhysicalPosition updateRecord(final OCluster iClusterSegment, final ORecordId rid, final byte[] recordContent,
      final ORecordVersion recordVersion, final byte iRecordType) {
    assert (lock.assertExclusiveLockHold());
    if (iClusterSegment == null)
      throw new OStorageException("Cluster not defined for record: " + rid);

    final long timer = Orient.instance().getProfiler().startChrono();

    try {

      // GET THE SHARED LOCK AND GET AN EXCLUSIVE LOCK AGAINST THE RECORD
      lockManager.acquireLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
      try {

        // UPDATE IT
        final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
        if (!checkForRecordValidity(ppos))
          return null;

        // VERSION CONTROL CHECK
        switch (recordVersion.getCounter()) {
        // DOCUMENT UPDATE, NO VERSION CONTROL
        case -1:
          ppos.recordVersion.increment();
          iClusterSegment.updateVersion(rid.clusterPosition, ppos.recordVersion);
          break;

        // DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION UPDATE
        case -2:
          break;

        default:
          // MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
          if (recordVersion.getCounter() > -1) {
            // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
            if (!recordVersion.equals(ppos.recordVersion))
              if (OFastConcurrentModificationException.enabled())
                throw OFastConcurrentModificationException.instance();
              else
                throw new OConcurrentModificationException(rid, ppos.recordVersion, recordVersion, ORecordOperation.UPDATED);
            ppos.recordVersion.increment();
            iClusterSegment.updateVersion(rid.clusterPosition, ppos.recordVersion);
          } else {
            // DOCUMENT ROLLBACKED
            recordVersion.clearRollbackMode();
            ppos.recordVersion.copyFrom(recordVersion);
            iClusterSegment.updateVersion(rid.clusterPosition, ppos.recordVersion);
          }
        }

        if (ppos.recordType != iRecordType)
          iClusterSegment.updateRecordType(rid.clusterPosition, iRecordType);

        final long newDataSegmentOffset;

        if (ppos.dataSegmentPos == -1)
          // WAS EMPTY FIRST TIME, CREATE IT NOW
          newDataSegmentOffset = getDataSegmentById(ppos.dataSegmentId).addRecord(rid, recordContent);
        else
          newDataSegmentOffset = getDataSegmentById(ppos.dataSegmentId).setRecord(ppos.dataSegmentPos, rid, recordContent);

        if (newDataSegmentOffset != ppos.dataSegmentPos) {
          // UPDATE DATA SEGMENT OFFSET WITH THE NEW PHYSICAL POSITION
          iClusterSegment.updateDataSegmentPosition(ppos.clusterPosition, ppos.dataSegmentId, newDataSegmentOffset);
          ppos.dataSegmentPos = newDataSegmentOffset;
        }

        return ppos;

      } finally {
        lockManager.releaseLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
      }

    } catch (IOException e) {

      OLogManager.instance().error(this, "Error on updating record " + rid + " (cluster: " + iClusterSegment + ")", e);

    } finally {
      Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer, "db.*.updateRecord");
    }

    return null;
  }

  protected OPhysicalPosition deleteRecord(final OCluster iClusterSegment, final ORecordId iRid, final ORecordVersion iVersion,
      boolean useTombstones) {
    assert (lock.assertExclusiveLockHold());
    final long timer = Orient.instance().getProfiler().startChrono();

    try {
      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      try {

        final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));

        if (ppos == null || ppos.dataSegmentId < 0 || (useTombstones && ppos.recordVersion.isTombstone()))
          // ALREADY DELETED
          return null;

        // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
        if (iVersion.getCounter() > -1 && !ppos.recordVersion.equals(iVersion))
          if (OFastConcurrentModificationException.enabled())
            throw OFastConcurrentModificationException.instance();
          else
            throw new OConcurrentModificationException(iRid, ppos.recordVersion, iVersion, ORecordOperation.DELETED);

        if (!ppos.recordVersion.isTombstone() && ppos.dataSegmentPos > -1) {
          try {
            getDataSegmentById(ppos.dataSegmentId).deleteRecord(ppos.dataSegmentPos);
          } catch (OIOException e) {
            OLogManager.instance().error(this, "Cannot remove the record in data segment, however remove it from cluster", e);
          }
        }

        if (useTombstones && iClusterSegment.hasTombstonesSupport())
          iClusterSegment.convertToTombstone(iRid.clusterPosition);
        else
          iClusterSegment.removePhysicalPosition(iRid.clusterPosition);

        return ppos;

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      }
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on deleting record " + iRid + "( cluster: " + iClusterSegment + ")", e);
    } finally {
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
    }

    return null;
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    final OCluster cluster = getClusterById(recordId.clusterId);

    modificationLock.requestModificationLock();
    try {
      final OPhysicalPosition ppos = deleteRecord(cluster, recordId, recordVersion, false);

      if (ppos != null
          && (OGlobalConfiguration.NON_TX_RECORD_UPDATE_SYNCH.getValueAsBoolean() || clustersToSyncImmediately.contains(cluster
              .getName())))
        synchRecordUpdate(cluster, ppos);

      final boolean returnValue = ppos != null;

      if (callback != null)
        callback.call(recordId, returnValue);

      return returnValue;
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  private void installProfilerHooks() {
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("db." + name + ".data.holes", "Number of the holes in database", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return getHoles();
              }
            }, "db.*.data.holes");
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("db." + name + ".data.holeSize", "Size of the holes in database", METRIC_TYPE.SIZE,
            new OProfilerHookValue() {
              public Object getValue() {
                return getHoleSize();
              }
            }, "db.*.data.holeSize");
  }

  private void uninstallProfilerHooks() {
    Orient.instance().getProfiler().unregisterHookValue("db." + name + ".data.holes");
    Orient.instance().getProfiler().unregisterHookValue("db." + name + ".data.holeSize");
  }

  private void formatMessage(final boolean iVerbose, final OCommandOutputListener iListener, final String iMessage,
      final Object... iArgs) {
    if (iVerbose)
      iListener.onMessage(String.format(iMessage, iArgs));
  }

  public void freeze(boolean throwException) {
    modificationLock.prohibitModifications(throwException);
    synch();

    try {
      for (OCluster cluster : clusters)
        if (cluster != null)
          cluster.setSoftlyClosed(true);

      for (ODataLocal data : dataSegments)
        if (data != null)
          data.setSoftlyClosed(true);

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

      for (ODataLocal data : dataSegments)
        if (data != null)
          data.setSoftlyClosed(false);

      if (configuration != null)
        configuration.setSoftlyClosed(false);

    } catch (IOException e) {
      throw new OStorageException("Error on release storage '" + name + "'", e);
    }

    modificationLock.allowModifications();
  }

  public boolean wasClusterSoftlyClosed(String clusterName) {
    final OCluster indexCluster = clusterMap.get(clusterName);
    return !(indexCluster instanceof OClusterLocal) || ((OClusterLocal) indexCluster).isSoftlyClosed();
  }

  @Override
  public String getType() {
    return OEngineLocal.NAME;
  }

}
