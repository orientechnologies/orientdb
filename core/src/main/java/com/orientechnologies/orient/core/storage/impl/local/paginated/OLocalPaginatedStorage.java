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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.local.twoq.O2QCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OAbstractPaginatedStorage {

  private static final String[] ALL_FILE_EXTENSIONS = { ".ocf", ".pls", ".pcl", ".oda", ".odh", ".otx", ".ocs", ".oef", ".oem",
      ".oet", ".fl", ODiskWriteAheadLog.WAL_SEGMENT_EXTENSION, ODiskWriteAheadLog.MASTER_RECORD_EXTENSION,
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

  private final Path                                         storagePath;
  private final OClosableLinkedContainer<Long, OFileClassic> files;

  public OLocalPaginatedStorage(final String name, final String filePath, final String mode, final int id, OReadCache readCache,
      OClosableLinkedContainer<Long, OFileClassic> files) throws IOException {
    super(name, filePath, mode, id);

    this.files = files;
    this.readCache = readCache;

    File f = new File(url);

    String sp;
    if (f.exists() || !exists(Paths.get(f.getParent()))) {
      // ALREADY EXISTS OR NOT LEGACY
      sp = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getPath()));
    } else {
      // LEGACY DB
      sp = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(url).getParent()));
    }

    storagePath = Paths.get(OIOUtils.getPathFromDatabaseName(sp));
    variableParser = new OStorageVariableParser(storagePath);

    configuration = new OStorageConfigurationSegment(this);

    DELETE_MAX_RETRIES = getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.FILE_DELETE_RETRY);
    DELETE_WAIT_TIME = getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.FILE_DELETE_DELAY);

    dirtyFlag = new OPaginatedStorageDirtyFlag(storagePath.resolve("dirty.fl"));
  }

  @Override
  public void create(OContextConfiguration contextConfiguration) throws IOException {
    try {
      stateLock.acquireWriteLock();
      try {
        final Path storageFolder = storagePath;
        if (!Files.exists(storageFolder))
          Files.createDirectories(storageFolder);

        super.create(contextConfiguration);
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
    final int firstIndexOf = name.lastIndexOf('/');
    final int secondIndexOf = name.lastIndexOf(File.separator);

    if (firstIndexOf >= 0 || secondIndexOf >= 0)
      return name.substring(Math.max(firstIndexOf, secondIndexOf) + 1);
    else
      return name;
  }

  @Override
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
    return OEngineLocalPaginated.NAME + ":" + url;
  }

  public Path getStoragePath() {
    return storagePath;
  }

  public OStorageVariableParser getVariableParser() {
    return variableParser;
  }

  @Override
  public String getType() {
    return OEngineLocalPaginated.NAME;
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
              .compressDirectory(getStoragePath().toString(), bo, new String[] { ".fl", O2QCache.CACHE_STATISTIC_FILE_EXTENSION },
                  iOutput, compressionLevel);
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

      OZIPCompressionUtil.uncompressDirectory(in, getStoragePath().toString(), iListener);

      final Path cacheStateFile = getStoragePath().resolve(O2QCache.CACHE_STATE_FILE);
      if (Files.exists(cacheStateFile)) {
        String message = "the cache state file (" + O2QCache.CACHE_STATE_FILE + ") is found in the backup, deleting the file";
        OLogManager.instance().warn(this, message);
        if (iListener != null)
          iListener.onMessage('\n' + message);

        try {
          Files.deleteIfExists(cacheStateFile); // delete it, if it still exists
        } catch (IOException e) {
          message =
              "unable to delete the backed up cache state file (" + O2QCache.CACHE_STATE_FILE + "), please delete it manually";
          OLogManager.instance().warn(this, message, e);
          if (iListener != null)
            iListener.onMessage('\n' + message);
        }
      }

      if (callable != null)
        try {
          callable.call();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on calling callback on database restore", e);
        }

      open(null, null, new OContextConfiguration());
    } catch (RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OStorageConfigurationImpl getConfiguration() {
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
      writeAheadLog.appendNewSegment();
      nonActiveSegments = writeAheadLog.nonActiveSegments(startSegment);
    } finally {
      getAtomicOperationsManager().releaseAtomicOperations(freezeId);
    }

    for (final File nonActiveSegment : nonActiveSegments) {
      try (FileInputStream fileInputStream = new FileInputStream(nonActiveSegment)) {
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
          final ZipEntry entry = new ZipEntry(nonActiveSegment.getName());
          zipOutputStream.putNextEntry(entry);
          try {
            final byte[] buffer = new byte[4096];

            int br;

            while ((br = bufferedInputStream.read(buffer)) >= 0) {
              zipOutputStream.write(buffer, 0, br);
            }
          } finally {
            zipOutputStream.closeEntry();
          }
        }
      }
    }

    return lastLSN;
  }

  @Override
  protected File createWalTempDirectory() {
    final File walDirectory = new File(getStoragePath().toFile(), "walIncrementalBackupRestoreDirectory");

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
    try (FileOutputStream outputStream = new FileOutputStream(walBackupFile)) {
      try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream)) {
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
      }
    }
  }

  @Override
  protected OWriteAheadLog createWalFromIBUFiles(File directory) throws IOException {
    final OWriteAheadLog restoreWAL = new ODiskWriteAheadLog(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger(),
        OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger(),
        ((long) OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger()) * ONE_KB * ONE_KB, directory.getAbsolutePath(),
        false, this, OGlobalConfiguration.WAL_SEGMENT_BUFFER_SIZE.getValueAsInteger() * 1024 * 1024,
        OGlobalConfiguration.WAL_FILE_AUTOCLOSE_INTERVAL.getValueAsInteger());

    return restoreWAL;
  }

  @Override
  protected void preOpenSteps() throws IOException {
    if (getConfiguration().binaryFormatVersion >= 11) {
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
  protected void postCloseSteps(boolean onDelete, boolean jvmError) throws IOException {
    if (onDelete) {
      dirtyFlag.delete();
    } else {
      if (!jvmError) {
        dirtyFlag.clearDirty();
      }
      dirtyFlag.close();
    }
  }

  @Override
  protected void postDeleteSteps() {
    File dbDir;// GET REAL DIRECTORY
    dbDir = new File(OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url)));
    if (!dbDir.exists() || !dbDir.isDirectory())
      dbDir = dbDir.getParentFile();

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
                    + Arrays.toString(dbDir.listFiles()), null);
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

  @Override
  protected void makeStorageDirty() throws IOException {
    dirtyFlag.makeDirty();
  }

  @Override
  protected void clearStorageDirty() throws IOException {
    dirtyFlag.clearDirty();
  }

  @Override
  protected boolean isDirty() {
    return dirtyFlag.isDirty();
  }

  @Override
  protected boolean isWriteAllowedDuringIncrementalBackup() {
    return true;
  }

  @Override
  protected void initWalAndDiskCache(OContextConfiguration contextConfiguration) throws IOException, InterruptedException {
    if (getConfiguration().getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.USE_WAL)) {
      fuzzyCheckpointExecutor.scheduleWithFixedDelay(new PeriodicFuzzyCheckpoint(),
          OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger(),
          OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger(), TimeUnit.SECONDS);

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

    final OWOWCache wowCache = new OWOWCache(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB,
        OByteBufferPool.instance(), writeAheadLog, OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL.getValueAsInteger(),
        writeCacheSize, this, true, files, getId(),
        contextConfiguration.getValueAsEnum(OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.class));

    wowCache.addLowDiskSpaceListener(this);
    wowCache.loadRegisteredFiles();
    wowCache.addBackgroundExceptionListener(this);
    wowCache.addPageIsBrokenListener(this);

    writeCache = wowCache;
  }

  public static boolean exists(final Path path) {
    return Files.exists(path.resolve("database.ocf"));
  }

  private class PeriodicFuzzyCheckpoint implements Runnable {
    @Override
    public void run() {
      try {
        makeFuzzyCheckpoint();
      } catch (RuntimeException e) {
        OLogManager.instance().error(this, "Error during fuzzy checkpoint", e);
      }
    }
  }
}
