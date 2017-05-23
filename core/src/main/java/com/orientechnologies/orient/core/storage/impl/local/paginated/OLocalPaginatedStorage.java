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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.local.twoq.O2QCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OAbstractPaginatedStorage implements OFreezableStorageComponent {

  private static String[] ALL_FILE_EXTENSIONS = { ".ocf", ".pls", ".pcl", ".oda", ".odh", ".otx", ".ocs", ".oef", ".oem", ".oet",
      ".fl", ".json", ".DS_Store", ODiskWriteAheadLog.WAL_SEGMENT_EXTENSION, ODiskWriteAheadLog.MASTER_RECORD_EXTENSION,
      OHashTableIndexEngine.BUCKET_FILE_EXTENSION, OHashTableIndexEngine.METADATA_FILE_EXTENSION,
      OHashTableIndexEngine.TREE_FILE_EXTENSION, OHashTableIndexEngine.NULL_BUCKET_FILE_EXTENSION,
      OClusterPositionMap.DEF_EXTENSION, OSBTreeIndexEngine.DATA_FILE_EXTENSION, OWOWCache.NAME_ID_MAP_EXTENSION,
      OIndexRIDContainer.INDEX_FILE_EXTENSION, OSBTreeCollectionManagerShared.DEFAULT_EXTENSION,
      OSBTreeIndexEngine.NULL_BUCKET_FILE_EXTENSION, O2QCache.CACHE_STATISTIC_FILE_EXTENSION };

  private static final int ONE_KB = 1024;

  private final int DELETE_MAX_RETRIES;
  private final int DELETE_WAIT_TIME;

  private final OStorageVariableParser     variableParser;
  private final OPaginatedStorageDirtyFlag dirtyFlag;

  private final String          storagePath;
  private       ExecutorService checkpointExecutor;

  private final OClosableLinkedContainer<Long, OFileClassic> files;

  public OLocalPaginatedStorage(final String name, final String filePath, final String mode, final int id, OReadCache readCache,
      OClosableLinkedContainer<Long, OFileClassic> files) throws IOException {
    super(name, filePath, mode, id);

    this.readCache = readCache;
    this.files = files;

    File f = new File(url);

    String sp;
    if (f.exists() || !exists(f.getParent())) {
      // ALREADY EXISTS OR NOT LEGACY
      sp = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getPath()));
    } else {
      // LEGACY DB
      sp = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getParent()));
    }

    storagePath = OIOUtils.getPathFromDatabaseName(sp);
    variableParser = new OStorageVariableParser(storagePath);

    dirtyFlag = new OPaginatedStorageDirtyFlag(storagePath + File.separator + "dirty.fl");

    configuration = new OStorageConfigurationSegment(this);

    DELETE_MAX_RETRIES = OGlobalConfiguration.FILE_DELETE_RETRY.getValueAsInteger();
    DELETE_WAIT_TIME = OGlobalConfiguration.FILE_DELETE_DELAY.getValueAsInteger();
  }

  @Override
  public void create(final Map<String, Object> iProperties) {
    try {
      stateLock.acquireWriteLock();
      try {
        final File storageFolder = new File(storagePath);
        if (!storageFolder.exists())
          if (!storageFolder.mkdirs())
            throw new OStorageException("Cannot create folders in storage with path " + storagePath);

        super.create(iProperties);
      } finally {
        stateLock.releaseWriteLock();
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
  protected String normalizeName(String name) {
    try {
      final int firstIndexOf = name.lastIndexOf('/');
      final int secondIndexOf = name.lastIndexOf(File.separator);

      if (firstIndexOf >= 0 || secondIndexOf >= 0)
        return name.substring(Math.max(firstIndexOf, secondIndexOf) + 1);
      else
        return name;
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public boolean exists() {
    try {
      if (status == STATUS.OPEN)
        return true;

      return exists(storagePath);
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public String getURL() {
    try {
      return OEngineLocalPaginated.NAME + ":" + url;
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  public String getStoragePath() {
    return storagePath;
  }

  public OStorageVariableParser getVariableParser() {
    return variableParser;
  }

  @Override
  public String getType() {
    try {
      return OEngineLocalPaginated.NAME;
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iOutput, final int compressionLevel, final int bufferSize) throws IOException {
    try {
      if (out == null)
        throw new IllegalArgumentException("Backup output is null");

      freeze(false);
      try {
        if (callable != null)
          try {
            callable.call();
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on callback invocation during backup", e);
          }

        final OutputStream bo = bufferSize > 0 ? new BufferedOutputStream(out, bufferSize) : out;
        try {
          return OZIPCompressionUtil
              .compressDirectory(new File(getStoragePath()).getAbsolutePath(), bo, new String[] { ".wal", ".fl" }, iOutput,
                  compressionLevel);
        } finally {
          if (bufferSize > 0) {
            bo.flush();
            bo.close();
          }
        }
      } finally {
        release();
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
  public void restore(InputStream in, Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    try {
      if (!isClosed())
        close(true, false);

      OZIPCompressionUtil.uncompressDirectory(in, getStoragePath(), iListener);

      if (callable != null)
        try {
          callable.call();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on calling callback on database restore");
        }

      open(null, null, null);
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OStorageConfiguration getConfiguration() {
    try {
      stateLock.acquireReadLock();
      try {
        return super.getConfiguration();
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
  protected OLogSequenceNumber copyWALToIncrementalBackup(ZipOutputStream zipOutputStream, long startSegment) throws IOException {

    File[] nonActiveSegments;

    OLogSequenceNumber lastLSN;
    long freezeId = getAtomicOperationsManager().freezeAtomicOperations(null, null);
    try {
      lastLSN = writeAheadLog.end();
      writeAheadLog.newSegment();
      nonActiveSegments = writeAheadLog.nonActiveSegments(startSegment);
    } finally {
      getAtomicOperationsManager().releaseAtomicOperations(freezeId);
    }

    for (final File nonActiveSegment : nonActiveSegments) {
      final FileInputStream fileInputStream = new FileInputStream(nonActiveSegment);
      try {
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        try {
          final ZipEntry entry = new ZipEntry(nonActiveSegment.getName());
          zipOutputStream.putNextEntry(entry);
          try {
            final byte[] buffer = new byte[4096];

            int br = 0;

            while ((br = bufferedInputStream.read(buffer)) >= 0) {
              zipOutputStream.write(buffer, 0, br);
            }
          } finally {
            zipOutputStream.closeEntry();
          }
        } finally {
          bufferedInputStream.close();
        }
      } finally {
        fileInputStream.close();
      }
    }

    return lastLSN;
  }

  @Override
  protected File createWalTempDirectory() {
    final File walDirectory = new File(getStoragePath(), "walIncrementalBackupRestoreDirectory");

    if (walDirectory.exists()) {
      OFileUtils.deleteRecursively(walDirectory);
    }

    if (!walDirectory.mkdirs())
      throw new OStorageException("Can not create temporary directory to store files created during incremental backup");

    return walDirectory;
  }

  @Override
  protected void addFileToDirectory(String name, InputStream stream, File directory) throws IOException {
    final byte[] buffer = new byte[4096];

    int rb = -1;
    int bl = 0;

    final File walBackupFile = new File(directory, name);
    final FileOutputStream outputStream = new FileOutputStream(walBackupFile);
    try {
      final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
      try {
        while (true) {
          while (bl < buffer.length && (rb = stream.read(buffer, bl, buffer.length - bl)) > -1) {
            bl += rb;
          }

          bufferedOutputStream.write(buffer, 0, bl);
          bl = 0;

          if (rb < 0) {
            break;
          }
        }
      } finally {
        bufferedOutputStream.close();
      }
    } finally {
      outputStream.close();
    }
  }

  @Override
  protected OWriteAheadLog createWalFromIBUFiles(File directory) throws IOException {
    final OWriteAheadLog restoreWAL = new ODiskWriteAheadLog(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger(),
        OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger(),
        ((long) OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger()) * ONE_KB * ONE_KB, directory.getAbsolutePath(),
        false, this, OGlobalConfiguration.WAL_FILE_AUTOCLOSE_INTERVAL.getValueAsInteger());

    return restoreWAL;
  }

  @Override
  protected void preOpenSteps() throws IOException {
    if (configuration.binaryFormatVersion >= 11) {
      if (dirtyFlag.exists())
        dirtyFlag.open();
      else {
        dirtyFlag.create();
        dirtyFlag.makeDirty();
      }
    } else {
      if (dirtyFlag.exists())
        dirtyFlag.open();
      else {
        dirtyFlag.create();
        dirtyFlag.clearDirty();
      }
    }
  }

  @Override
  protected void preCreateSteps() throws IOException {
    dirtyFlag.create();
  }

  @Override
  protected void postCloseSteps(boolean onDelete) throws IOException {
    if (onDelete)
      dirtyFlag.delete();
    else {
      dirtyFlag.clearDirty();
      dirtyFlag.close();
    }
  }

  @Override
  protected void preCloseSteps() throws IOException {
    try {
      if (writeAheadLog != null) {
        checkpointExecutor.shutdown();
        if (!checkpointExecutor
            .awaitTermination(OGlobalConfiguration.WAL_FULL_CHECKPOINT_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.SECONDS))
          throw new OStorageException("Cannot terminate full checkpoint task");
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw OException.wrapException(new OStorageException("Error on closing of storage '" + name), e);
    }
  }

  @Override
  protected void postDeleteSteps() {
    File dbDir;// GET REAL DIRECTORY
    dbDir = new File(OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url)));
    if (!dbDir.exists() || !dbDir.isDirectory())
      return;

    // RETRIES
    for (int i = 0; i < DELETE_MAX_RETRIES; ++i) {
      if (dbDir != null && dbDir.exists() && dbDir.isDirectory()) {
        int notDeletedFiles = 0;

        final File[] storageFiles = dbDir.listFiles();
        if (storageFiles == null)
          continue;

        // TRY TO DELETE ALL THE FILES
        for (File f : storageFiles) {
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
          if (!dbDir.delete())
            OLogManager.instance().error(this,
                "Cannot delete storage directory with path " + dbDir.getAbsolutePath() + " because directory is not empty. Files: "
                    + Arrays.toString(dbDir.listFiles()));
          return;
        }
      } else
        return;

      OLogManager.instance().debug(this,
          "Cannot delete database files because they are still locked by the OrientDB process: waiting %d ms and retrying %d/%d...",
          DELETE_WAIT_TIME, i, DELETE_MAX_RETRIES);
    }

    throw new OStorageException("Cannot delete database '" + name + "' located in: " + dbDir + ". Database files seem locked");
  }

  protected void makeStorageDirty() throws IOException {
    dirtyFlag.makeDirty();
  }

  protected void clearStorageDirty() throws IOException {
    dirtyFlag.clearDirty();
  }

  @Override
  protected boolean isDirty() throws IOException {
    return dirtyFlag.isDirty();
  }

  @Override
  public boolean isIndexRebuildScheduled() {
    checkOpeness();

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      return dirtyFlag.isIndexRebuildScheduled();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  protected boolean isIndexRebuildScheduledInternal() {
    return dirtyFlag.isIndexRebuildScheduled();
  }

  @Override
  protected void scheduleIndexRebuild() throws IOException {
    dirtyFlag.scheduleIndexRebuild();
  }

  @Override
  public void cancelIndexRebuild() throws IOException {
    try {
      checkOpeness();

      stateLock.acquireReadLock();
      try {
        checkOpeness();

        dirtyFlag.clearIndexRebuild();
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
  protected boolean isWriteAllowedDuringIncrementalBackup() {
    return true;
  }

  protected void initWalAndDiskCache() throws IOException {
    if (configuration.getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.USE_WAL)) {
      checkpointExecutor = Executors.newSingleThreadExecutor(new FullCheckpointThreadFactory());

      final ODiskWriteAheadLog diskWriteAheadLog = new ODiskWriteAheadLog(this);
      diskWriteAheadLog.addLowDiskSpaceListener(this);
      diskWriteAheadLog.checkFreeSpace();
      writeAheadLog = diskWriteAheadLog;
      writeAheadLog.addFullCheckpointListener(this);
    } else
      writeAheadLog = null;

    long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
    long writeCacheSize = (long) Math
        .floor((((double) OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0) * diskCacheSize);

    final OWOWCache wowCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB,
        OByteBufferPool.instance(), OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_TTL.getValueAsLong() * 1000, writeAheadLog,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL.getValueAsInteger(), writeCacheSize, diskCacheSize, this, true,
        files, getId());
    wowCache.loadRegisteredFiles();
    wowCache.addLowDiskSpaceListener(this);
    wowCache.addBackgroundExceptionListener(this);

    writeCache = wowCache;
  }

  public static boolean exists(final String path) {
    return new File(path + "/" + OMetadataDefault.CLUSTER_INTERNAL_NAME + OPaginatedCluster.DEF_EXTENSION).exists();
  }

  private static class FullCheckpointThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      return thread;
    }
  }
}
