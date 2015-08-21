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

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.*;
import java.lang.String;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OAbstractPaginatedStorage implements OFreezableStorage {
  private static final String              incrementalBackupExtension = ".ibu";

  private static String[]                  ALL_FILE_EXTENSIONS        = { incrementalBackupExtension, ".ocf", ".pls", ".pcl",
      ".oda", ".odh", ".otx", ".ocs", ".oef", ".oem", ".oet", ODiskWriteAheadLog.WAL_SEGMENT_EXTENSION,
      ODiskWriteAheadLog.MASTER_RECORD_EXTENSION, OHashTableIndexEngine.BUCKET_FILE_EXTENSION,
      OHashTableIndexEngine.METADATA_FILE_EXTENSION, OHashTableIndexEngine.TREE_FILE_EXTENSION,
      OHashTableIndexEngine.NULL_BUCKET_FILE_EXTENSION, OClusterPositionMap.DEF_EXTENSION, OSBTreeIndexEngine.DATA_FILE_EXTENSION,
      OWOWCache.NAME_ID_MAP_EXTENSION, OIndexRIDContainer.INDEX_FILE_EXTENSION, OSBTreeCollectionManagerShared.DEFAULT_EXTENSION,
      OSBTreeIndexEngine.NULL_BUCKET_FILE_EXTENSION                  };

  private static final int                 ONE_KB                     = 1024;

  private final int                        DELETE_MAX_RETRIES;
  private final int                        DELETE_WAIT_TIME;

  private final OStorageVariableParser     variableParser;
  private final OPaginatedStorageDirtyFlag dirtyFlag;

  private final String                     storagePath;
  private ExecutorService                  checkpointExecutor;

  public OLocalPaginatedStorage(final String name, final String filePath, final String mode, final int id, OReadCache readCache)
      throws IOException {
    super(name, filePath, mode, id);

    this.readCache = readCache;

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

    configuration = new OStorageConfigurationSegment(this);

    DELETE_MAX_RETRIES = OGlobalConfiguration.FILE_DELETE_RETRY.getValueAsInteger();
    DELETE_WAIT_TIME = OGlobalConfiguration.FILE_DELETE_DELAY.getValueAsInteger();

    dirtyFlag = new OPaginatedStorageDirtyFlag(storagePath + File.separator + "dirty.fl");
  }

  @Override
  public void create(final Map<String, Object> iProperties) {
    final File storageFolder = new File(storagePath);
    if (!storageFolder.exists())
      if (!storageFolder.mkdirs())
        throw new OStorageException("Cannot create folders in storage with path " + storagePath);

    super.create(iProperties);
  }

  public boolean exists() {
    if (status == STATUS.OPEN)
      return true;

    return exists(storagePath);
  }

  @Override
  public String getURL() {
    return OEngineLocalPaginated.NAME + ":" + url;
  }

  public String getStoragePath() {
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
        return OZIPCompressionUtil.compressDirectory(new File(getStoragePath()).getAbsolutePath(), bo, new String[] { ".wal" },
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
  }

  public void incrementalBackup(String backupDirectory) {
    incrementalBackup(new File(backupDirectory));
  }

  public void incrementalBackup(File backupDirectory) {
    if (!backupDirectory.exists()) {
      if (!backupDirectory.mkdirs()) {
        throw new OStorageException("Backup directory " + backupDirectory.getAbsolutePath()
            + " does not exist and can not be created");
      }
    }

    final String[] files = backupDirectory.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.toLowerCase(configuration.getLocaleInstance()).endsWith(incrementalBackupExtension);
      }
    });

    if (files == null)
      throw new OStorageException("Can not read list of backup files from directory " + backupDirectory.getAbsolutePath());

    Arrays.sort(files, new Comparator<String>() {
      @Override
      public int compare(String firsFile, String secondFile) {
        final long firstIndex = extractIndexFromIBUFile(firsFile);
        final long secondIndex = extractIndexFromIBUFile(secondFile);

        if (firstIndex > secondIndex)
          return 1;
        if (firstIndex < secondIndex)
          return -1;

        return 0;
      }
    });

    final OLogSequenceNumber lastLsn;
    final long nextIndex;

    if (files.length == 0) {
      lastLsn = null;
      nextIndex = 0;
    } else {
      lastLsn = extractIBULsn(backupDirectory, files[0]);
      nextIndex = extractIndexFromIBUFile(files[0]) + 1;
    }

    final SimpleDateFormat dateFormat = new SimpleDateFormat("HH_mm_ss_dd_MM_yyyy");
    final String fileName = nextIndex + "_" + dateFormat.format(new Date()) + incrementalBackupExtension;

    final File ibuFile = new File(backupDirectory, fileName);
    final FileOutputStream fileOutputStream;
    try {
      fileOutputStream = new FileOutputStream(ibuFile);
    } catch (IOException e) {
      throw new OStorageException("Error during incremental backup", e);
    }

    try {
      incrementalBackup(fileOutputStream, lastLsn);
    } catch (IOException e) {
      throw new OStorageException("Error during incremental backup", e);
    } finally {
      try {
        fileOutputStream.close();
      } catch (IOException e) {
        throw new OStorageException("Error during incremental backup", e);
      }
    }
  }

  private OLogSequenceNumber extractIBULsn(File backupDirectory, String file) {
    final File ibuFile = new File(backupDirectory, file);
    final FileInputStream fileInputStream;
    try {
      fileInputStream = new FileInputStream(ibuFile);
    } catch (FileNotFoundException e) {
      throw new OStorageException("Backup file was not found", e);
    }

    try {
      final ZipInputStream zipInputStream = new ZipInputStream(fileInputStream);
      try {
        ZipEntry zipEntry;
        while ((zipEntry = zipInputStream.getNextEntry()) != null) {
          if (zipEntry.getName().equals("last.lsn")) {
            final byte[] data = new byte[2 * OLongSerializer.LONG_SIZE];

            int bytesRead = 0;

            while (bytesRead < data.length) {
              final int rb = zipInputStream.read(data, bytesRead, data.length - bytesRead);

              if (rb >= 0)
                bytesRead += rb;
              else {
                throw new OStorageException("Can not read last backed up LSN during incremental backup");
              }
            }

            final long segment = OLongSerializer.INSTANCE.deserializeNative(data, 0);
            final long position = OLongSerializer.INSTANCE.deserializeNative(data, OLongSerializer.LONG_SIZE);

            return new OLogSequenceNumber(segment, position);
          }
        }
      } finally {
        zipInputStream.close();
      }

    } catch (IOException e) {
      throw new OStorageException("Error during read of backup file", e);
    } finally {
      try {
        fileInputStream.close();
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during read of backup file", e);
      }
    }

    throw new OStorageException("Can not find latest backed up LSN in last IBU file");
  }

  private long extractIndexFromIBUFile(String fileName) {
    final int indexPosition = fileName.indexOf('_');
    if (indexPosition < 1)
      throw new OStorageException("Invalid file name : " + fileName);

    final String indexStr = fileName.substring(0, indexPosition);
    final long index;
    try {
      index = Long.parseLong(indexStr);
    } catch (NumberFormatException e) {
      throw new OStorageException("Invalid file name : " + fileName);
    }

    return index;
  }

  public void incrementalBackup(final OutputStream stream, OLogSequenceNumber logSequenceNumber) throws IOException {
    checkOpeness();

    freeze(false);
    try {
      stateLock.acquireReadLock();
      try {
        checkOpeness();

        final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(stream);
        try {
          final ZipOutputStream zipOutputStream = new ZipOutputStream(bufferedOutputStream, Charset.forName(configuration
              .getCharset()));
          try {
            final Date startedOn = new Date();
            final OLogSequenceNumber lastLsn = ((OWOWCache) writeCache).backupPagesWithChanges(logSequenceNumber, zipOutputStream);
            final Date completedOn = new Date();

            final ZipEntry backupJson = new ZipEntry("backup.json");
            zipOutputStream.putNextEntry(backupJson);

            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh-mm-ss.SSS");

            final OutputStreamWriter writer = new OutputStreamWriter(zipOutputStream, configuration.getCharset());
            writer.append("{\r\n");
            writer.append("\t\"lsn\":").append(lastLsn.toString()).append(",\r\n");
            writer.append("\t\"startedOn\":").append(dateFormat.format(startedOn)).append(",\r\n");
            writer.append("\t\"completedOn\":").append(dateFormat.format(completedOn)).append("\r\n");
            writer.append("}\r\n");

            writer.flush();

            zipOutputStream.closeEntry();

            final ZipEntry lastLsnEntry = new ZipEntry("last.lsn");
            zipOutputStream.putNextEntry(lastLsnEntry);

            final byte[] binaryLsn = new byte[2 * OLongSerializer.LONG_SIZE];
            OLongSerializer.INSTANCE.serializeNative(lastLsn.getSegment(), binaryLsn, 0);
            OLongSerializer.INSTANCE.serializeNative(lastLsn.getPosition(), binaryLsn, OLongSerializer.LONG_SIZE);

            zipOutputStream.write(binaryLsn);
            zipOutputStream.closeEntry();
          } finally {
            zipOutputStream.close();
          }

        } finally {
          bufferedOutputStream.close();
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } finally {
      release();
    }
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    if (!isClosed())
      close(true, false);

    OZIPCompressionUtil.uncompressDirectory(in, getStoragePath(), iListener);
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
      ((OWOWCache) writeCache).unregisterMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for write cache cannot be unregistered", e);
    }

    try {
      if (writeAheadLog != null) {
        checkpointExecutor.shutdown();
        if (!checkpointExecutor.awaitTermination(OGlobalConfiguration.WAL_FULL_CHECKPOINT_SHUTDOWN_TIMEOUT.getValueAsInteger(),
            TimeUnit.SECONDS))
          throw new OStorageException("Cannot terminate full checkpoint task");
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OStorageException("Error on closing of storage '" + name, e);
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
            OLogManager.instance().error(this, "Cannot delete storage directory with path " + dbDir.getAbsolutePath());
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

  protected void initWalAndDiskCache() throws IOException {
    if (configuration.getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.USE_WAL)) {
      checkpointExecutor = Executors.newSingleThreadExecutor(new FullCheckpointThreadFactory());

      writeAheadLog = new ODiskWriteAheadLog(this);
      writeAheadLog.addFullCheckpointListener(this);
    } else
      writeAheadLog = null;

    long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
    long writeCacheSize = (long) Math.floor((((double) OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0)
        * diskCacheSize);

    final OWOWCache wowCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_TTL.getValueAsLong() * 1000, writeAheadLog,
        OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL.getValueAsInteger(), writeCacheSize, diskCacheSize, this, true,
        getId());
    wowCache.addLowDiskSpaceListener(this);
    try {
      wowCache.registerMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for write cache cannot be registered.");
    }

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
