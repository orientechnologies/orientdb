/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.agent.Utils;
import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OQuarto;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OInvalidInstanceIdException;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OEnterpriseLocalPaginatedStorage extends OLocalPaginatedStorage {
  private static final OLogger logger =
      OLogManager.instance().logger(OEnterpriseLocalPaginatedStorage.class);

  private static final String INCREMENTAL_BACKUP_LOCK = "backup.ibl";

  private static final String IBU_EXTENSION_V3 = ".ibu3";
  private static final int INCREMENTAL_BACKUP_VERSION = 423;

  private static final String INCREMENTAL_BACKUP_DATEFORMAT = "yyyy-MM-dd-HH-mm-ss";

  private final List<OEnterpriseStorageOperationListener> listeners = new CopyOnWriteArrayList<>();

  public OEnterpriseLocalPaginatedStorage(
      String name,
      String filePath,
      int id,
      OReadCache readCache,
      OClosableLinkedContainer<Long, OFile> files,
      long walMaxSize,
      long doubleWriteLogMaxSize,
      OrientDBInternal context) {
    super(name, filePath, id, readCache, files, walMaxSize, doubleWriteLogMaxSize, context);
    logger.info("Enterprise storage installed correctly.");
  }

  @Override
  public String incrementalBackup(final String backupDirectory, OCallable<Void, Void> started) {
    return incrementalBackup(new File(backupDirectory), started);
  }

  public boolean isLastBackupCompatibleWithUUID(final File backupDirectory) throws IOException {
    if (!backupDirectory.exists()) {
      return true;
    }

    final Path fileLockPath = backupDirectory.toPath().resolve(INCREMENTAL_BACKUP_LOCK);
    try (FileChannel lockChannel =
        FileChannel.open(fileLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      try (final FileLock fileLock = lockChannel.lock()) {
        final String[] files = fetchIBUFiles(backupDirectory);
        if (files.length > 0) {
          UUID backupUUID =
              extractDbInstanceUUID(backupDirectory, files[0], configuration.getCharset());
          try {
            checkDatabaseInstanceId(backupUUID);
          } catch (OInvalidInstanceIdException ex) {
            return false;
          }
        }
      } catch (final OverlappingFileLockException e) {
        logger.error(
            "Another incremental backup process is in progress, please wait till it will be"
                + " finished",
            null);
      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
      }

      try {
        Files.deleteIfExists(fileLockPath);
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
      }
    }
    return true;
  }

  private String incrementalBackup(
      final File backupDirectory, final OCallable<Void, Void> started) {
    String fileName = "";

    if (!backupDirectory.exists()) {
      if (!backupDirectory.mkdirs()) {
        throw new OStorageException(
            "Backup directory "
                + backupDirectory.getAbsolutePath()
                + " does not exist and can not be created");
      }
    }
    checkNoBackupInStorageDir(backupDirectory);

    final Path fileLockPath = backupDirectory.toPath().resolve(INCREMENTAL_BACKUP_LOCK);
    try (final FileChannel lockChannel =
        FileChannel.open(fileLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      try (@SuppressWarnings("unused")
          final FileLock fileLock = lockChannel.lock()) {
        RandomAccessFile rndIBUFile = null;
        try {
          final String[] files = fetchIBUFiles(backupDirectory);

          final OLogSequenceNumber lastLsn;
          long nextIndex;
          final UUID backupUUID;

          if (files.length == 0) {
            lastLsn = null;
            nextIndex = 0;
            backupUUID = null;
          } else {
            lastLsn = extractIBULsn(backupDirectory, files[files.length - 1]);
            nextIndex = extractIndexFromIBUFile(backupDirectory, files[files.length - 1]) + 1;
            backupUUID =
                extractDbInstanceUUID(backupDirectory, files[0], configuration.getCharset());
            checkDatabaseInstanceId(backupUUID);
          }

          final SimpleDateFormat dateFormat = new SimpleDateFormat(INCREMENTAL_BACKUP_DATEFORMAT);
          if (lastLsn != null) {
            fileName =
                getName()
                    + "_"
                    + dateFormat.format(new Date())
                    + "_"
                    + nextIndex
                    + IBU_EXTENSION_V3;
          } else {
            fileName =
                getName()
                    + "_"
                    + dateFormat.format(new Date())
                    + "_"
                    + nextIndex
                    + "_full"
                    + IBU_EXTENSION_V3;
          }

          final File ibuFile = new File(backupDirectory, fileName);

          if (started != null) started.call(null);
          rndIBUFile = new RandomAccessFile(ibuFile, "rw");
          try {
            final FileChannel ibuChannel = rndIBUFile.getChannel();

            final ByteBuffer versionBuffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
            versionBuffer.putInt(INCREMENTAL_BACKUP_VERSION);
            versionBuffer.rewind();

            OIOUtils.writeByteBuffer(versionBuffer, ibuChannel, 0);

            ibuChannel.position(
                2 * OIntegerSerializer.INT_SIZE
                    + 2 * OLongSerializer.LONG_SIZE
                    + OByteSerializer.BYTE_SIZE);

            OutputStream stream = Channels.newOutputStream(ibuChannel);
            OLogSequenceNumber maxLsn;
            try {
              maxLsn = incrementalBackup(stream, lastLsn, true);
              final ByteBuffer dataBuffer =
                  ByteBuffer.allocate(
                      OIntegerSerializer.INT_SIZE
                          + 2 * OLongSerializer.LONG_SIZE
                          + OByteSerializer.BYTE_SIZE);

              dataBuffer.putLong(nextIndex);
              dataBuffer.putLong(maxLsn.getSegment());
              dataBuffer.putInt(maxLsn.getPosition());

              if (lastLsn == null) dataBuffer.put((byte) 1);
              else dataBuffer.put((byte) 0);

              dataBuffer.rewind();

              ibuChannel.position(OIntegerSerializer.INT_SIZE);
              ibuChannel.write(dataBuffer);

            } finally {
              Utils.safeClose(this, stream);
            }
          } catch (RuntimeException e) {
            rndIBUFile.close();

            if (!ibuFile.delete()) {
              logger.error("%s is closed but can not be deleted", null, ibuFile.getAbsolutePath());
            }

            throw e;
          }
        } catch (IOException e) {
          throw OException.wrapException(
              new OStorageException("Error during incremental backup"), e);
        } finally {
          try {
            if (rndIBUFile != null) rndIBUFile.close();
          } catch (IOException e) {
            logger.error("Can not close %s file", e, fileName);
          }
        }
      }
    } catch (final OverlappingFileLockException e) {
      logger.error(
          "Another incremental backup process is in progress, please wait till it will be"
              + " finished",
          null);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    }

    try {
      Files.deleteIfExists(fileLockPath);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    }

    return fileName;
  }

  private UUID extractDbInstanceUUID(File backupDirectory, String file, String charset)
      throws IOException {
    final File ibuFile = new File(backupDirectory, file);
    final RandomAccessFile rndIBUFile;
    try {
      rndIBUFile = new RandomAccessFile(ibuFile, "r");
    } catch (FileNotFoundException e) {
      throw OException.wrapException(new OStorageException("Backup file was not found"), e);
    }

    try {
      final FileChannel ibuChannel = rndIBUFile.getChannel();
      ibuChannel.position(3 * OLongSerializer.LONG_SIZE + 1);

      final InputStream inputStream = Channels.newInputStream(ibuChannel);
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      final ZipInputStream zipInputStream =
          new ZipInputStream(bufferedInputStream, Charset.forName(charset));

      ZipEntry zipEntry;
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        if (zipEntry.getName().equals("database_instance.uuid")) {
          DataInputStream dis = new DataInputStream(zipInputStream);
          UUID uuid = UUID.fromString(dis.readUTF());
          return uuid;
        }
      }
    } finally {
      rndIBUFile.close();
    }
    return null;
  }

  private void checkNoBackupInStorageDir(final File backupDirectory) {
    if (getStoragePath() == null || backupDirectory == null) {
      return;
    }

    boolean invalid = false;
    try {
      final File storageDir = getStoragePath().toFile();
      if (backupDirectory.equals(storageDir)) {
        invalid = true;
      }
    } catch (final Exception e) {
    }
    if (invalid) {
      throw new OStorageException("Backup cannot be performed in the storage path");
    }
  }

  public void registerStorageListener(OEnterpriseStorageOperationListener listener) {
    this.listeners.add(listener);
  }

  public void unRegisterStorageListener(OEnterpriseStorageOperationListener listener) {
    this.listeners.remove(listener);
  }

  private String[] fetchIBUFiles(final File backupDirectory) throws IOException {
    final String[] files =
        backupDirectory.list(
            (dir, name) ->
                new File(dir, name).length() > 0 && name.toLowerCase().endsWith(IBU_EXTENSION_V3));

    if (files == null)
      throw new OStorageException(
          "Can not read list of backup files from directory " + backupDirectory.getAbsolutePath());

    final List<OPair<Long, String>> indexedFiles = new ArrayList<>(files.length);

    for (String file : files) {
      final long fileIndex = extractIndexFromIBUFile(backupDirectory, file);
      indexedFiles.add(new OPair<>(fileIndex, file));
    }

    Collections.sort(indexedFiles);

    final String[] sortedFiles = new String[files.length];

    int index = 0;
    for (OPair<Long, String> indexedFile : indexedFiles) {
      sortedFiles[index] = indexedFile.getValue();
      index++;
    }

    return sortedFiles;
  }

  private OLogSequenceNumber extractIBULsn(File backupDirectory, String file) {
    final File ibuFile = new File(backupDirectory, file);
    final RandomAccessFile rndIBUFile;
    try {
      rndIBUFile = new RandomAccessFile(ibuFile, "r");
    } catch (FileNotFoundException e) {
      throw OException.wrapException(new OStorageException("Backup file was not found"), e);
    }

    try {
      try {
        final FileChannel ibuChannel = rndIBUFile.getChannel();
        ibuChannel.position(OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

        ByteBuffer lsnData =
            ByteBuffer.allocate(OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
        ibuChannel.read(lsnData);
        lsnData.rewind();

        final long segment = lsnData.getLong();
        final int position = lsnData.getInt();

        return new OLogSequenceNumber(segment, position);
      } finally {
        rndIBUFile.close();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of backup file"), e);
    } finally {
      try {
        rndIBUFile.close();
      } catch (IOException e) {
        logger.error("Error during read of backup file", e);
      }
    }
  }

  private long extractIndexFromIBUFile(final File backupDirectory, final String fileName)
      throws IOException {
    final File file = new File(backupDirectory, fileName);

    try (final RandomAccessFile rndFile = new RandomAccessFile(file, "r")) {
      rndFile.seek(OIntegerSerializer.INT_SIZE);
      return validateLongIndex(rndFile.readLong());
    }
  }

  private long validateLongIndex(final long index) {
    return index < 0 ? 0 : Math.abs(index);
  }

  public void restoreFromIncrementalBackup(final String filePath) {
    restoreFromIncrementalBackup(new File(filePath));
  }

  private void restoreFromIncrementalBackup(final File backupDirectory) {
    if (!backupDirectory.exists()) {
      throw new OStorageException(
          "Directory which should contain incremental backup files (files with extension '"
              + IBU_EXTENSION_V3
              + "') is absent. It should be located at '"
              + backupDirectory.getAbsolutePath()
              + "'");
    }

    try {
      final String[] files = fetchIBUFiles(backupDirectory);
      if (files.length == 0) {
        throw new OStorageException(
            "Cannot find incremental backup files (files with extension '"
                + IBU_EXTENSION_V3
                + "') in directory '"
                + backupDirectory.getAbsolutePath()
                + "'");
      }

      stateLock.writeLock().lock();
      try {

        final String aesKeyEncoded =
            getConfiguration()
                .getContextConfiguration()
                .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
        final byte[] aesKey =
            aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

        if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
          throw new OInvalidStorageEncryptionKeyException(
              "Invalid length of the encryption key, provided size is " + aesKey.length);
        }

        final OQuarto<Locale, OContextConfiguration, String, Locale> quarto =
            preprocessingIncrementalRestore();
        final Locale serverLocale = quarto.one;
        final OContextConfiguration contextConfiguration = quarto.two;
        final String charset = quarto.three;
        final Locale locale = quarto.four;

        UUID restoreUUID = extractDbInstanceUUID(backupDirectory, files[0], charset);

        for (String file : files) {
          UUID fileUUID = extractDbInstanceUUID(backupDirectory, files[0], charset);
          if ((restoreUUID == null && fileUUID == null)
              || (restoreUUID != null && restoreUUID.equals(fileUUID))) {
            final File ibuFile = new File(backupDirectory, file);

            RandomAccessFile rndIBUFile = new RandomAccessFile(ibuFile, "rw");
            try {
              final FileChannel ibuChannel = rndIBUFile.getChannel();
              final ByteBuffer versionBuffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
              OIOUtils.readByteBuffer(versionBuffer, ibuChannel);
              versionBuffer.rewind();

              final int backupVersion = versionBuffer.getInt();
              if (backupVersion != INCREMENTAL_BACKUP_VERSION) {
                throw new OStorageException(
                    "Invalid version of incremental backup version was provided. Expected "
                        + INCREMENTAL_BACKUP_VERSION
                        + " , provided "
                        + backupVersion);
              }

              ibuChannel.position(2 * OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE);
              final ByteBuffer buffer = ByteBuffer.allocate(1);
              ibuChannel.read(buffer);
              buffer.rewind();

              final boolean fullBackup = buffer.get() == 1;

              final InputStream inputStream = Channels.newInputStream(ibuChannel);
              try {
                restoreFromIncrementalBackup(
                    charset,
                    serverLocale,
                    locale,
                    contextConfiguration,
                    aesKey,
                    inputStream,
                    fullBackup);
              } finally {
                Utils.safeClose(this, inputStream);
              }
            } finally {
              try {
                rndIBUFile.close();
              } catch (IOException e) {
                logger.warn("Failed to close resource %s", e, rndIBUFile);
              }
            }
          } else {
            logger.warn(
                "Skipped file '%s' is not a backup of the same database of previous backups", file);
          }

          postProcessIncrementalRestore(contextConfiguration);
        }
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during restore from incremental backup"), e);
    }
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(
      ORecordId iRid,
      String iFetchPlan,
      boolean iIgnoreCache,
      boolean prefetchRecords,
      ORecordCallback<ORawBuffer> iCallback) {

    try {
      return super.readRecord(iRid, iFetchPlan, iIgnoreCache, prefetchRecords, iCallback);
    } finally {
      listeners.forEach(OEnterpriseStorageOperationListener::onRead);
    }
  }

  @Override
  public List<ORecordOperation> commit(OTransactionInternal clientTx, boolean allocated) {
    List<ORecordOperation> operations = super.commit(clientTx, allocated);
    listeners.forEach((l) -> l.onCommit(operations));
    return operations;
  }
}
