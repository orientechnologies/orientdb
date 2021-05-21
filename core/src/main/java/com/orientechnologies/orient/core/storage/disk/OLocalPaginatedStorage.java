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

package com.orientechnologies.orient.core.storage.disk;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog.MASTER_RECORD_EXTENSION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog.WAL_SEGMENT_EXTENSION;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.thread.OThreadPoolExecutorWithLogging;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeMultiValueIndexEngine;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLog;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLogGL;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLogNoOP;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.v2.FreeSpaceMap;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.paginated.StorageStartupMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OAbstractPaginatedStorage {
  @SuppressWarnings("WeakerAccess")
  protected static final long IV_SEED = 234120934;

  private static final String IV_EXT = ".iv";

  @SuppressWarnings("WeakerAccess")
  protected static final String IV_NAME = "data" + IV_EXT;

  private static final String[] ALL_FILE_EXTENSIONS = {
    ".cm",
    ".ocf",
    ".pls",
    ".pcl",
    ".oda",
    ".odh",
    ".otx",
    ".ocs",
    ".oef",
    ".oem",
    ".oet",
    ".fl",
    ".flb",
    IV_EXT,
    CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION,
    CASDiskWriteAheadLog.MASTER_RECORD_EXTENSION,
    OHashTableIndexEngine.BUCKET_FILE_EXTENSION,
    OHashTableIndexEngine.METADATA_FILE_EXTENSION,
    OHashTableIndexEngine.TREE_FILE_EXTENSION,
    OHashTableIndexEngine.NULL_BUCKET_FILE_EXTENSION,
    OClusterPositionMap.DEF_EXTENSION,
    OSBTreeIndexEngine.DATA_FILE_EXTENSION,
    OIndexRIDContainer.INDEX_FILE_EXTENSION,
    OSBTreeCollectionManagerShared.DEFAULT_EXTENSION,
    OSBTreeIndexEngine.NULL_BUCKET_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.MAP_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.DATA_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.TREE_DATA_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.TREE_NULL_FILE_EXTENSION,
    OCellBTreeMultiValueIndexEngine.DATA_FILE_EXTENSION,
    OCellBTreeMultiValueIndexEngine.M_CONTAINER_EXTENSION,
    DoubleWriteLogGL.EXTENSION,
    FreeSpaceMap.DEF_EXTENSION
  };

  private static final int ONE_KB = 1024;

  private static final OThreadPoolExecutorWithLogging segmentAdderExecutor;

  static {
    segmentAdderExecutor =
        new OThreadPoolExecutorWithLogging(
            0, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new SegmentAppenderFactory());
  }

  private final int deleteMaxRetries;
  private final int deleteWaitTime;

  private final StorageStartupMetadata startupMetadata;

  private final Path storagePath;
  private final OClosableLinkedContainer<Long, OFile> files;

  private Future<?> fuzzyCheckpointTask;

  private final long walMaxSegSize;
  private final long doubleWriteLogMaxSegSize;

  private final AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

  protected volatile byte[] iv;

  public OLocalPaginatedStorage(
      final String name,
      final String filePath,
      final String mode,
      final int id,
      final OReadCache readCache,
      final OClosableLinkedContainer<Long, OFile> files,
      final long walMaxSegSize,
      long doubleWriteLogMaxSegSize) {
    super(name, filePath, mode, id);

    this.walMaxSegSize = walMaxSegSize;
    this.files = files;
    this.doubleWriteLogMaxSegSize = doubleWriteLogMaxSegSize;
    this.readCache = readCache;

    final String sp =
        OSystemVariableResolver.resolveSystemVariables(
            OFileUtils.getPath(new java.io.File(url).getPath()));

    storagePath = Paths.get(OIOUtils.getPathFromDatabaseName(sp));

    deleteMaxRetries = OGlobalConfiguration.FILE_DELETE_RETRY.getValueAsInteger();
    deleteWaitTime = OGlobalConfiguration.FILE_DELETE_DELAY.getValueAsInteger();

    startupMetadata =
        new StorageStartupMetadata(
            storagePath.resolve("dirty.fl"), storagePath.resolve("dirty.flb"));
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public void create(final OContextConfiguration contextConfiguration) {
    try {
      stateLock.acquireWriteLock();
      try {
        final Path storageFolder = storagePath;
        if (!Files.exists(storageFolder)) {
          Files.createDirectories(storageFolder);
        }

        super.create(contextConfiguration);
      } finally {
        stateLock.releaseWriteLock();
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
  protected final String normalizeName(final String name) {
    final int firstIndexOf = name.lastIndexOf('/');
    final int secondIndexOf = name.lastIndexOf(java.io.File.separator);

    if (firstIndexOf >= 0 || secondIndexOf >= 0)
      return name.substring(Math.max(firstIndexOf, secondIndexOf) + 1);
    else return name;
  }

  @Override
  public final boolean exists() {
    try {
      if (status == STATUS.OPEN || status == STATUS.INTERNAL_ERROR) return true;

      return exists(storagePath);
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public String getURL() {
    return OEngineLocalPaginated.NAME + ":" + url;
  }

  public final Path getStoragePath() {
    return storagePath;
  }

  @Override
  public String getType() {
    return OEngineLocalPaginated.NAME;
  }

  @Override
  public final List<String> backup(
      final OutputStream out,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iOutput,
      final int compressionLevel,
      final int bufferSize) {
    try {
      if (out == null) throw new IllegalArgumentException("Backup output is null");

      freeze(false);
      try {
        if (callable != null)
          try {
            callable.call();
          } catch (final Exception e) {
            OLogManager.instance().error(this, "Error on callback invocation during backup", e);
          }
        OLogSequenceNumber freezeLSN = null;
        if (writeAheadLog != null) {
          freezeLSN = writeAheadLog.begin();
          writeAheadLog.addCutTillLimit(freezeLSN);
        }

        startupMetadata.setTxMetadata(getLastMetadata().orElse(null));
        try {
          final OutputStream bo = bufferSize > 0 ? new BufferedOutputStream(out, bufferSize) : out;
          try {
            try (final ZipOutputStream zos = new ZipOutputStream(bo)) {
              zos.setComment("OrientDB Backup executed on " + new Date());
              zos.setLevel(compressionLevel);

              final List<String> names =
                  OZIPCompressionUtil.compressDirectory(
                      storagePath.toString(),
                      zos,
                      new String[] {".fl", ".lock", DoubleWriteLogGL.EXTENSION},
                      iOutput);
              startupMetadata.addFileToArchive(zos, "dirty.fl");
              names.add("dirty.fl");
              return names;
            }
          } finally {
            if (bufferSize > 0) {
              bo.flush();
              bo.close();
            }
          }
        } finally {
          if (freezeLSN != null) {
            writeAheadLog.removeCutTillLimit(freezeLSN);
          }
        }

      } finally {
        release();
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
  public final void restore(
      final InputStream in,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iListener) {
    try {
      if (!isClosed()) close(true, false);
      try {
        stateLock.acquireWriteLock();
        final java.io.File dbDir =
            new java.io.File(
                OIOUtils.getPathFromDatabaseName(
                    OSystemVariableResolver.resolveSystemVariables(url)));
        final java.io.File[] storageFiles = dbDir.listFiles();
        if (storageFiles != null) {
          // TRY TO DELETE ALL THE FILES
          for (final java.io.File f : storageFiles) {
            // DELETE ONLY THE SUPPORTED FILES
            for (final String ext : ALL_FILE_EXTENSIONS)
              if (f.getPath().endsWith(ext)) {
                //noinspection ResultOfMethodCallIgnored
                f.delete();
                break;
              }
          }
        }

        OZIPCompressionUtil.uncompressDirectory(in, storagePath.toString(), iListener);

        final java.io.File[] newStorageFiles = dbDir.listFiles();
        if (newStorageFiles != null) {
          // TRY TO DELETE ALL THE FILES
          for (final java.io.File f : newStorageFiles) {
            if (f.getPath().endsWith(MASTER_RECORD_EXTENSION)) {
              final boolean renamed =
                  f.renameTo(new File(f.getParent(), getName() + MASTER_RECORD_EXTENSION));
              assert renamed;
            }
            if (f.getPath().endsWith(WAL_SEGMENT_EXTENSION)) {
              String walName = f.getName();
              final int segmentIndex =
                  walName.lastIndexOf(".", walName.length() - WAL_SEGMENT_EXTENSION.length() - 1);
              String ending = walName.substring(segmentIndex);
              final boolean renamed = f.renameTo(new File(f.getParent(), getName() + ending));
              assert renamed;
            }
          }
        }

        if (callable != null)
          try {
            callable.call();
          } catch (final Exception e) {
            OLogManager.instance().error(this, "Error on calling callback on database restore", e);
          }
      } finally {
        stateLock.releaseWriteLock();
      }

      open(null, null, new OContextConfiguration());
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  protected OLogSequenceNumber copyWALToIncrementalBackup(
      final ZipOutputStream zipOutputStream, final long startSegment) throws IOException {

    java.io.File[] nonActiveSegments;

    OLogSequenceNumber lastLSN;
    final long freezeId = getAtomicOperationsManager().freezeAtomicOperations(null, null);
    try {
      lastLSN = writeAheadLog.end();
      writeAheadLog.flush();
      writeAheadLog.appendNewSegment();
      nonActiveSegments = writeAheadLog.nonActiveSegments(startSegment);
    } finally {
      getAtomicOperationsManager().releaseAtomicOperations(freezeId);
    }

    for (final java.io.File nonActiveSegment : nonActiveSegments) {
      try (final FileInputStream fileInputStream = new FileInputStream(nonActiveSegment)) {
        try (final BufferedInputStream bufferedInputStream =
            new BufferedInputStream(fileInputStream)) {
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
  protected java.io.File createWalTempDirectory() {
    final java.io.File walDirectory =
        new java.io.File(storagePath.toFile(), "walIncrementalBackupRestoreDirectory");

    if (walDirectory.exists()) {
      OFileUtils.deleteRecursively(walDirectory);
    }

    if (!walDirectory.mkdirs())
      throw new OStorageException(
          "Can not create temporary directory to store files created during incremental backup");

    return walDirectory;
  }

  @Override
  protected void addFileToDirectory(
      final String name, final InputStream stream, final java.io.File directory)
      throws IOException {
    final byte[] buffer = new byte[4096];

    int rb = -1;
    int bl = 0;

    final java.io.File walBackupFile = new java.io.File(directory, name);
    try (final FileOutputStream outputStream = new FileOutputStream(walBackupFile)) {
      try (final BufferedOutputStream bufferedOutputStream =
          new BufferedOutputStream(outputStream)) {
        do {
          while (bl < buffer.length && (rb = stream.read(buffer, bl, buffer.length - bl)) > -1) {
            bl += rb;
          }

          bufferedOutputStream.write(buffer, 0, bl);
          bl = 0;

        } while (rb >= 0);
      }
    }
  }

  @Override
  protected OWriteAheadLog createWalFromIBUFiles(
      final java.io.File directory,
      final OContextConfiguration contextConfiguration,
      final Locale locale,
      byte[] iv)
      throws IOException {
    final String aesKeyEncoded =
        contextConfiguration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    final byte[] aesKey =
        Optional.ofNullable(aesKeyEncoded)
            .map(keyEncoded -> Base64.getDecoder().decode(keyEncoded))
            .orElse(null);

    return new CASDiskWriteAheadLog(
        name,
        storagePath,
        directory.toPath(),
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_CACHE_SIZE),
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_BUFFER_SIZE),
        aesKey,
        iv,
        contextConfiguration.getValueAsLong(OGlobalConfiguration.WAL_SEGMENTS_INTERVAL)
            * 60
            * 1_000_000_000L,
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE)
            * 1024
            * 1024L,
        10,
        true,
        locale,
        OGlobalConfiguration.WAL_MAX_SIZE.getValueAsLong() * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024 * 1024,
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_COMMIT_TIMEOUT),
        contextConfiguration.getValueAsBoolean(OGlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT),
        contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC),
        contextConfiguration.getValueAsBoolean(
            OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS),
        contextConfiguration.getValueAsInteger(
            OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL));
  }

  @Override
  protected StartupMetadata checkIfStorageDirty() throws IOException {
    if (startupMetadata.exists()) startupMetadata.open();
    else {
      startupMetadata.create();
      startupMetadata.makeDirty();
    }

    return new StartupMetadata(startupMetadata.getLastTxId(), startupMetadata.getTxMetadata());
  }

  @Override
  protected void initConfiguration(
      OAtomicOperation atomicOperation, final OContextConfiguration contextConfiguration)
      throws IOException {
    if (!OClusterBasedStorageConfiguration.exists(writeCache)
        && Files.exists(storagePath.resolve("database.ocf"))) {
      final OStorageConfigurationSegment oldConfig = new OStorageConfigurationSegment(this);
      oldConfig.load(contextConfiguration);

      final OClusterBasedStorageConfiguration atomicConfiguration =
          new OClusterBasedStorageConfiguration(this);
      atomicConfiguration.create(atomicOperation, contextConfiguration, oldConfig);
      configuration = atomicConfiguration;

      oldConfig.close();
      Files.deleteIfExists(storagePath.resolve("database.ocf"));
    }

    if (configuration == null) {
      configuration = new OClusterBasedStorageConfiguration(this);
      ((OClusterBasedStorageConfiguration) configuration)
          .load(contextConfiguration, atomicOperation);
    }
  }

  @Override
  protected Map<String, Object> preCloseSteps() {
    final Map<String, Object> params = super.preCloseSteps();

    if (fuzzyCheckpointTask != null) {
      fuzzyCheckpointTask.cancel(false);
    }

    return params;
  }

  @Override
  protected void postCloseStepsAfterLock(final Map<String, Object> params) {
    super.postCloseStepsAfterLock(params);
  }

  @Override
  protected void preCreateSteps() throws IOException {
    startupMetadata.create();
  }

  @Override
  protected void postCloseSteps(
      final boolean onDelete, final boolean internalError, final long lastTxId) throws IOException {
    if (onDelete) {
      startupMetadata.delete();
    } else {
      if (!internalError) {
        startupMetadata.setLastTxId(lastTxId);
        startupMetadata.setTxMetadata(getLastMetadata().orElse(null));

        startupMetadata.clearDirty();
      }
      startupMetadata.close();
    }
  }

  @Override
  protected void postDeleteSteps() {
    String databasePath =
        OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url));
    deleteFilesFromDisc(name, deleteMaxRetries, deleteWaitTime, databasePath);
  }

  public static void deleteFilesFromDisc(
      String name, int maxRetries, int waitTime, String databaseDirectory) {
    java.io.File dbDir; // GET REAL DIRECTORY
    dbDir = new java.io.File(databaseDirectory);
    if (!dbDir.exists() || !dbDir.isDirectory()) dbDir = dbDir.getParentFile();

    // RETRIES
    for (int i = 0; i < maxRetries; ++i) {
      if (dbDir != null && dbDir.exists() && dbDir.isDirectory()) {
        int notDeletedFiles = 0;

        final java.io.File[] storageFiles = dbDir.listFiles();
        if (storageFiles == null) continue;

        // TRY TO DELETE ALL THE FILES
        for (final java.io.File f : storageFiles) {
          // DELETE ONLY THE SUPPORTED FILES
          for (final String ext : ALL_FILE_EXTENSIONS)
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
            OLogManager.instance()
                .error(
                    OLocalPaginatedStorage.class,
                    "Cannot delete storage directory with path "
                        + dbDir.getAbsolutePath()
                        + " because directory is not empty. Files: "
                        + Arrays.toString(dbDir.listFiles()),
                    null);
          return;
        }
      } else return;

      OLogManager.instance()
          .debug(
              OLocalPaginatedStorage.class,
              "Cannot delete database files because they are still locked by the OrientDB process: waiting %d ms and retrying %d/%d...",
              waitTime,
              i,
              maxRetries);
    }

    throw new OStorageException(
        "Cannot delete database '"
            + name
            + "' located in: "
            + dbDir
            + ". Database files seem locked");
  }

  @Override
  protected void makeStorageDirty() throws IOException {
    startupMetadata.makeDirty();
  }

  @Override
  protected void clearStorageDirty() throws IOException {
    if (status != STATUS.INTERNAL_ERROR) {
      startupMetadata.clearDirty();
    }
  }

  @Override
  protected boolean isDirty() {
    return startupMetadata.isDirty();
  }

  @Override
  protected boolean isWriteAllowedDuringIncrementalBackup() {
    return true;
  }

  @Override
  protected void initIv() throws IOException {
    try (final RandomAccessFile ivFile =
        new RandomAccessFile(storagePath.resolve(IV_NAME).toAbsolutePath().toFile(), "rw")) {
      final byte[] iv = new byte[16];

      final SecureRandom random = new SecureRandom();
      random.nextBytes(iv);

      final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
      final XXHash64 hash64 = hashFactory.hash64();

      final long hash = hash64.hash(iv, 0, iv.length, IV_SEED);
      ivFile.write(iv);
      ivFile.writeLong(hash);
      ivFile.getFD().sync();

      this.iv = iv;
    }
  }

  @Override
  protected void readIv() throws IOException {
    final Path ivPath = storagePath.resolve(IV_NAME).toAbsolutePath();
    if (!Files.exists(ivPath)) {
      OLogManager.instance().info(this, "IV file is absent, will create new one.");
      initIv();
      return;
    }

    try (final RandomAccessFile ivFile = new RandomAccessFile(ivPath.toFile(), "r")) {
      final byte[] iv = new byte[16];
      ivFile.readFully(iv);

      final long storedHash = ivFile.readLong();

      final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
      final XXHash64 hash64 = hashFactory.hash64();

      final long expectedHash = hash64.hash(iv, 0, iv.length, IV_SEED);
      if (storedHash != expectedHash) {
        throw new OStorageException("iv data are broken");
      }

      this.iv = iv;
    }
  }

  @Override
  protected byte[] getIv() {
    return iv;
  }

  @Override
  protected void initWalAndDiskCache(final OContextConfiguration contextConfiguration)
      throws IOException, InterruptedException {
    final String aesKeyEncoded =
        contextConfiguration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    final byte[] aesKey =
        Optional.ofNullable(aesKeyEncoded)
            .map(keyEncoded -> Base64.getDecoder().decode(keyEncoded))
            .orElse(null);

    fuzzyCheckpointTask =
        fuzzyCheckpointExecutor.scheduleWithFixedDelay(
            new PeriodicFuzzyCheckpoint(),
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL),
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL),
            TimeUnit.SECONDS);

    final String configWalPath =
        contextConfiguration.getValueAsString(OGlobalConfiguration.WAL_LOCATION);
    final Path walPath;
    if (configWalPath == null) {
      walPath = null;
    } else {
      walPath = Paths.get(configWalPath);
    }

    final CASDiskWriteAheadLog diskWriteAheadLog =
        new CASDiskWriteAheadLog(
            name,
            storagePath,
            walPath,
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_CACHE_SIZE),
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_BUFFER_SIZE),
            aesKey,
            iv,
            contextConfiguration.getValueAsLong(OGlobalConfiguration.WAL_SEGMENTS_INTERVAL)
                * 60
                * 1_000_000_000L,
            walMaxSegSize,
            10,
            true,
            Locale.getDefault(),
            contextConfiguration.getValueAsLong(OGlobalConfiguration.WAL_MAX_SIZE) * 1024 * 1024,
            contextConfiguration.getValueAsLong(OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT)
                * 1024
                * 1024,
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_COMMIT_TIMEOUT),
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT),
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC),
            contextConfiguration.getValueAsBoolean(
                OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS),
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL));

    diskWriteAheadLog.addLowDiskSpaceListener(this);
    writeAheadLog = diskWriteAheadLog;
    writeAheadLog.addFullCheckpointListener(this);

    diskWriteAheadLog.addSegmentOverflowListener(
        (segment) -> {
          if (status != STATUS.OPEN) {
            return;
          }

          final Future<Void> oldAppender = segmentAppender.get();
          if (oldAppender == null || oldAppender.isDone()) {
            final Future<Void> appender =
                segmentAdderExecutor.submit(new SegmentAdder(segment, diskWriteAheadLog));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              return;
            }

            appender.cancel(false);
          }
        });

    final int pageSize =
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE) * ONE_KB;
    final long diskCacheSize =
        contextConfiguration.getValueAsLong(OGlobalConfiguration.DISK_CACHE_SIZE) * 1024 * 1024;
    final long writeCacheSize =
        (long)
            (contextConfiguration.getValueAsInteger(OGlobalConfiguration.DISK_WRITE_CACHE_PART)
                / 100.0
                * diskCacheSize);

    final DoubleWriteLog doubleWriteLog;
    if (contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_USE_DOUBLE_WRITE_LOG)) {
      doubleWriteLog = new DoubleWriteLogGL(doubleWriteLogMaxSegSize);
    } else {
      doubleWriteLog = new DoubleWriteLogNoOP();
    }

    final OWOWCache wowCache =
        new OWOWCache(
            pageSize,
            OByteBufferPool.instance(null),
            writeAheadLog,
            doubleWriteLog,
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL),
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT),
            writeCacheSize,
            storagePath,
            getName(),
            OStringSerializer.INSTANCE,
            files,
            getId(),
            contextConfiguration.getValueAsEnum(
                OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.class),
            iv,
            aesKey,
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC),
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.DISK_USE_NATIVE_OS_API));

    wowCache.addLowDiskSpaceListener(this);
    wowCache.loadRegisteredFiles();
    wowCache.addBackgroundExceptionListener(this);
    wowCache.addPageIsBrokenListener(this);

    writeCache = wowCache;
  }

  public static boolean exists(final Path path) {
    try {
      final boolean[] exists = new boolean[1];
      if (Files.exists(path)) {
        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
          stream.forEach(
              (p) -> {
                final String fileName = p.getFileName().toString();
                if (fileName.equals("database.ocf")
                    || (fileName.startsWith("config") && fileName.endsWith(".bd"))
                    || fileName.startsWith("dirty.fl")
                    || fileName.startsWith("dirty.flb")) {
                  exists[0] = true;
                }
              });
        }
        return exists[0];
      }

      return false;
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during fetching list of files"), e);
    }
  }

  private class PeriodicFuzzyCheckpoint implements Runnable {
    @Override
    public final void run() {
      try {
        makeFuzzyCheckpoint();
      } catch (final RuntimeException e) {
        OLogManager.instance().error(this, "Error during fuzzy checkpoint", e);
      }
    }
  }

  private final class SegmentAdder implements Callable<Void> {
    private final long segment;
    private final CASDiskWriteAheadLog wal;

    private SegmentAdder(final long segment, final CASDiskWriteAheadLog wal) {
      this.segment = segment;
      this.wal = wal;
    }

    @Override
    public Void call() {
      try {
        if (status != STATUS.OPEN) {
          return null;
        }

        stateLock.acquireReadLock();
        try {
          if (status != STATUS.OPEN) {
            return null;
          }

          final long freezeId = atomicOperationsManager.freezeComponentOperations();
          try {
            wal.appendSegment(segment + 1);
          } finally {
            atomicOperationsManager.releaseComponentOperations(freezeId);
          }

          atomicOperationsTable.compactTable();
        } finally {
          stateLock.releaseReadLock();
        }

      } catch (final Exception e) {
        OLogManager.instance()
            .errorNoDb(this, "Error during addition of new segment in storage %s.", e, getName());
        throw e;
      }

      return null;
    }
  }

  private static final class SegmentAppenderFactory implements ThreadFactory {
    private SegmentAppenderFactory() {}

    @Override
    public Thread newThread(final Runnable r) {
      return new Thread(OAbstractPaginatedStorage.storageThreadGroup, r, "Segment adder thread");
    }
  }
}
