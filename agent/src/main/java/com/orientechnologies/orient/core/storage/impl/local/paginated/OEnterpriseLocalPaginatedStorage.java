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

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.exception.OBackupInProgressException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class OEnterpriseLocalPaginatedStorage extends OLocalPaginatedStorage {
  private final AtomicBoolean backupInProgress = new AtomicBoolean(false);

  public static final String                        IBU_EXTENSION                              = ".ibu";
  public static final String                        CONF_ENTRY_NAME                            = "database.ocf";
  public static final String                        INCREMENTAL_BACKUP_DATEFORMAT              = "yyyy-MM-dd-HH-mm-ss";

  public OEnterpriseLocalPaginatedStorage(String name, String filePath, String mode, int id, OReadCache readCache)
      throws IOException {
    super(name, filePath, mode, id, readCache);
    OLogManager.instance().info(this, "Enterprise storage installed correctly.");
  }

  public void incrementalBackup(final String backupDirectory) {
    incrementalBackup(new File(backupDirectory));
  }

  private void incrementalBackup(final File backupDirectory) {
    if (!backupDirectory.exists()) {
      if (!backupDirectory.mkdirs()) {
        throw new OStorageException(
            "Backup directory " + backupDirectory.getAbsolutePath() + " does not exist and can not be created");
      }
    }

    RandomAccessFile rndIBUFile = null;
    try {
      final String[] files = fetchIBUFiles(backupDirectory);

      final OLogSequenceNumber lastLsn;
      final long nextIndex;

      if (files.length == 0) {
        lastLsn = null;
        nextIndex = 0;
      } else {
        lastLsn = extractIBULsn(backupDirectory, files[files.length - 1]);
        nextIndex = extractIndexFromIBUFile(backupDirectory, files[files.length - 1]) + 1;
      }

      final SimpleDateFormat dateFormat = new SimpleDateFormat(INCREMENTAL_BACKUP_DATEFORMAT);
      final String fileName;

      if (lastLsn != null)
        fileName = getName() + "_" + dateFormat.format(new Date()) + "_" + nextIndex + IBU_EXTENSION;
      else
        fileName = getName() + "_" + dateFormat.format(new Date()) + "_" + nextIndex + "_full" + IBU_EXTENSION;

      final File ibuFile = new File(backupDirectory, fileName);

      rndIBUFile = new RandomAccessFile(ibuFile, "rw");
      try {
        final FileChannel ibuChannel = rndIBUFile.getChannel();

        ibuChannel.position(3 * OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE);

        final OLogSequenceNumber maxLsn = incrementalBackup(Channels.newOutputStream(ibuChannel), lastLsn);
        final ByteBuffer dataBuffer = ByteBuffer.allocate(3 * OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE);

        dataBuffer.putLong(nextIndex);
        dataBuffer.putLong(maxLsn.getSegment());
        dataBuffer.putLong(maxLsn.getPosition());

        if (lastLsn == null)
          dataBuffer.put((byte) 1);
        else
          dataBuffer.put((byte) 0);

        dataBuffer.rewind();

        ibuChannel.position(0);
        ibuChannel.write(dataBuffer);
      } catch (RuntimeException e) {
        rndIBUFile.close();

        if (!ibuFile.delete()) {
          OLogManager.instance().error(this, ibuFile.getAbsolutePath() + " is closed but can not be deleted");
        }

        throw e;
      }
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    } finally {
      try {
        if (rndIBUFile != null)
          rndIBUFile.close();
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
      }
    }
  }

  private String[] fetchIBUFiles(final File backupDirectory) throws IOException {
    final String[] files = backupDirectory.list(new FilenameFilter() {
      @Override
      public boolean accept(final File dir, final String name) {
        return new File(dir, name).length() > 0 && name.toLowerCase(configuration.getLocaleInstance()).endsWith(IBU_EXTENSION);
      }
    });

    if (files == null)
      throw new OStorageException("Can not read list of backup files from directory " + backupDirectory.getAbsolutePath());

    final List<OPair<Long, String>> indexedFiles = new ArrayList<OPair<Long, String>>(files.length);

    for (String file : files) {
      final long fileIndex = extractIndexFromIBUFile(backupDirectory, file);
      indexedFiles.add(new OPair<Long, String>(fileIndex, file));
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
        ibuChannel.position(OLongSerializer.LONG_SIZE);

        ByteBuffer lsnData = ByteBuffer.allocate(2 * OLongSerializer.LONG_SIZE);
        ibuChannel.read(lsnData);
        lsnData.rewind();

        final long segment = lsnData.getLong();
        final long position = lsnData.getLong();

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
        OLogManager.instance().error(this, "Error during read of backup file", e);
      }
    }
  }

  private long extractIndexFromIBUFile(final File backupDirectory, final String fileName) throws IOException {
    final File file = new File(backupDirectory, fileName);
    final RandomAccessFile rndFile = new RandomAccessFile(file, "r");
    final long index;

    try {
      index = rndFile.readLong();
    } finally {
      rndFile.close();
    }

    return index;
  }

  private OLogSequenceNumber incrementalBackup(final OutputStream stream, final OLogSequenceNumber fromLsn) throws IOException {
    OLogSequenceNumber lastLsn;

    checkOpeness();

    if (!backupInProgress.compareAndSet(false, true)) {
      throw new OBackupInProgressException(
          "You are trying to start incremental backup but it is in progress now, please wait till it will be finished", getName(),
          OErrorCode.BACKUP_IN_PROGRESS);
    }

    stateLock.acquireReadLock();
    try {
      checkOpeness();

      final long freezeId;

      if (!isWriteAllowedDuringIncrementalBackup())
        freezeId = atomicOperationsManager
            .freezeAtomicOperations(OModificationOperationProhibitedException.class, "Incremental backup in progress");
      else
        freezeId = -1;

      try {
        final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(stream);
        try {
          final ZipOutputStream zipOutputStream = new ZipOutputStream(bufferedOutputStream,
              Charset.forName(configuration.getCharset()));
          try {
            final long startSegment;

            final long newSegmentFreezeId = atomicOperationsManager.freezeAtomicOperations(null, null);
            try {
              final OLogSequenceNumber startLsn = writeAheadLog.end();
              writeAheadLog.preventCutTill(startLsn);

              writeAheadLog.newSegment();
              startSegment = writeAheadLog.activeSegment();
            } finally {
              atomicOperationsManager.releaseAtomicOperations(newSegmentFreezeId);
            }

            try {
              lastLsn = backupPagesWithChanges(fromLsn, zipOutputStream);
              final ZipEntry configurationEntry = new ZipEntry(CONF_ENTRY_NAME);

              zipOutputStream.putNextEntry(configurationEntry);
              final byte[] btConf = configuration.toStream();

              zipOutputStream.write(btConf);
              zipOutputStream.closeEntry();

              final OLogSequenceNumber lastWALLsn = copyWALToIncrementalBackup(zipOutputStream, startSegment);

              if (lastWALLsn != null && (lastLsn == null || lastWALLsn.compareTo(lastLsn) > 0)) {
                lastLsn = lastWALLsn;
              }
            } finally {
              writeAheadLog.preventCutTill(null);
            }
          } finally {
            zipOutputStream.flush();
          }
        } finally {
          bufferedOutputStream.flush();
        }
      } finally {
        if (!isWriteAllowedDuringIncrementalBackup())
          atomicOperationsManager.releaseAtomicOperations(freezeId);
      }
    } finally {
      stateLock.releaseReadLock();

      backupInProgress.set(false);
    }

    return lastLsn;
  }

  private OLogSequenceNumber backupPagesWithChanges(final OLogSequenceNumber changeLsn, ZipOutputStream stream) throws IOException {
    OLogSequenceNumber lastLsn = changeLsn;

    final Map<String, Long> files = writeCache.files();
    final int pageSize = writeCache.pageSize();

    for (Map.Entry<String, Long> entry : files.entrySet()) {
      final String fileName = entry.getKey();

      long fileId = entry.getValue();
      final boolean closeFile = !writeCache.isOpen(fileId);

      fileId = readCache.openFile(fileId, writeCache);

      final long filledUpTo = writeCache.getFilledUpTo(fileId);
      final ZipEntry zipEntry = new ZipEntry(fileName);

      stream.putNextEntry(zipEntry);

      final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serialize(fileId, binaryFileId, 0);
      stream.write(binaryFileId, 0, binaryFileId.length);

      for (long pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry = readCache.load(fileId, pageIndex, true, writeCache, 1);
        cacheEntry.acquireSharedLock();
        try {
          final OLogSequenceNumber pageLsn = ODurablePage
              .getLogSequenceNumberFromPage(cacheEntry.getCachePointer().getSharedBuffer());

          if (changeLsn == null || pageLsn.compareTo(changeLsn) > 0) {

            final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];
            OLongSerializer.INSTANCE.serializeNative(pageIndex, data, 0);
            ODurablePage.getPageData(cacheEntry.getCachePointer().getSharedBuffer(), data, OLongSerializer.LONG_SIZE, pageSize);

            stream.write(data);

            if (lastLsn == null || pageLsn.compareTo(lastLsn) > 0) {
              lastLsn = pageLsn;
            }
          }
        } finally {
          cacheEntry.releaseSharedLock();
          readCache.release(cacheEntry, writeCache);
        }
      }

      if (closeFile)
        readCache.closeFile(fileId, true, writeCache);

      stream.closeEntry();
    }

    return lastLsn;
  }

  public void restoreFromIncrementalBackup(final String filePath) {
    restoreFromIncrementalBackup(new File(filePath));
  }

  private void restoreFromIncrementalBackup(final File backupDirectory) {
    if (!backupDirectory.exists()) {
      throw new OStorageException("Directory which should contain incremental backup files (files with extension '" + IBU_EXTENSION
          + "') is absent. It should be located at '" + backupDirectory.getAbsolutePath() + "'");
    }

    try {
      final String[] files = fetchIBUFiles(backupDirectory);
      if (files.length == 0) {
        throw new OStorageException(
            "Cannot find incremental backup files (files with extension '" + IBU_EXTENSION + "') in directory '" + backupDirectory
                .getAbsolutePath() + "'");
      }

      stateLock.acquireWriteLock();
      try {
        for (String file : files) {
          final File ibuFile = new File(backupDirectory, file);

          final RandomAccessFile rndIBUFile = new RandomAccessFile(ibuFile, "rw");
          try {
            final FileChannel ibuChannel = rndIBUFile.getChannel();
            ibuChannel.position(3 * OLongSerializer.LONG_SIZE);

            final ByteBuffer buffer = ByteBuffer.allocate(1);
            ibuChannel.read(buffer);
            buffer.rewind();

            final boolean fullBackup = buffer.get() == 1;

            final InputStream inputStream = Channels.newInputStream(ibuChannel);
            restoreFromIncrementalBackup(inputStream, fullBackup);
          } finally {
            rndIBUFile.close();
          }

        }
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    }
  }

  private void restoreFromIncrementalBackup(final InputStream inputStream, final boolean isFull) throws IOException {
    stateLock.acquireWriteLock();
    try {
      final List<String> currentFiles = new ArrayList<String>(writeCache.files().keySet());
      final Locale serverLocale = configuration.getLocaleInstance();

      closeClusters(false);
      closeIndexes(false);

      sbTreeCollectionManager.clear();

      final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      final ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream, Charset.forName(configuration.getCharset()));
      final int pageSize = writeCache.pageSize();

      ZipEntry zipEntry;
      OLogSequenceNumber maxLsn = null;

      List<String> processedFiles = new ArrayList<String>();

      if (isFull) {
        final Map<String, Long> files = writeCache.files();
        for (Map.Entry<String, Long> entry : files.entrySet()) {
          final long fileId = readCache.openFile(entry.getKey(), writeCache);

          assert entry.getValue().equals(fileId);
          readCache.deleteFile(fileId, writeCache);
        }
      }

      final File walTempDir = createWalTempDirectory();

      entryLoop:
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        if (zipEntry.getName().equals(CONF_ENTRY_NAME)) {
          replaceConfiguration(zipInputStream);

          continue;
        }

        if (zipEntry.getName().toLowerCase(serverLocale).endsWith(ODiskWriteAheadLog.WAL_SEGMENT_EXTENSION)) {
          addFileToDirectory(zipEntry.getName(), zipInputStream, walTempDir);

          continue;
        }

        final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
        OIOUtils.readFully(zipInputStream, binaryFileId, 0, binaryFileId.length);

        final long expectedFileId = OLongSerializer.INSTANCE.deserialize(binaryFileId, 0);
        Long fileId;

        final boolean isClosed;

        if (!writeCache.exists(zipEntry.getName())) {
          fileId = readCache.addFile(zipEntry.getName(), expectedFileId, writeCache);
          isClosed = true;
        } else {
          fileId = writeCache.isOpen(zipEntry.getName());

          if (fileId == null) {
            isClosed = true;
            fileId = readCache.openFile(zipEntry.getName(), writeCache);
          } else
            isClosed = false;
        }

        if (!writeCache.fileIdsAreEqual(expectedFileId, fileId))
          throw new OStorageException("Can not restore database from backup because expected and actual file ids are not the same");

        while (true) {
          final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];

          int rb = 0;

          while (rb < data.length) {
            final int b = zipInputStream.read(data, rb, data.length - rb);

            if (b == -1) {
              if (rb > 0)
                throw new OStorageException("Can not read data from file " + zipEntry.getName());
              else {
                if (isClosed)
                  readCache.closeFile(fileId, true, writeCache);

                processedFiles.add(zipEntry.getName());
                continue entryLoop;
              }
            }

            rb += b;
          }

          final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(data, 0);

          OCacheEntry cacheEntry = readCache.load(fileId, pageIndex, true, writeCache, 1);

          if (cacheEntry == null) {
            do {
              if (cacheEntry != null)
                readCache.release(cacheEntry, writeCache);

              cacheEntry = readCache.allocateNewPage(fileId, writeCache);
            } while (cacheEntry.getPageIndex() != pageIndex);
          }

          cacheEntry.acquireExclusiveLock();
          try {
            final ByteBuffer buffer = cacheEntry.getCachePointer().getSharedBuffer();
            final OLogSequenceNumber backedUpPageLsn = ODurablePage.getLogSequenceNumber(OLongSerializer.LONG_SIZE, data);
            if (isFull) {
              buffer.position(0);
              buffer.put(data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);
              cacheEntry.markDirty();

              if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                maxLsn = backedUpPageLsn;
              }
            } else {
              final OLogSequenceNumber currentPageLsn = ODurablePage.getLogSequenceNumberFromPage(buffer);
              if (backedUpPageLsn.compareTo(currentPageLsn) > 0) {
                buffer.position(0);
                buffer.put(data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

                cacheEntry.markDirty();

                if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                  maxLsn = backedUpPageLsn;
                }
              }
            }
          } finally {
            cacheEntry.releaseExclusiveLock();
            readCache.release(cacheEntry, writeCache);
          }
        }
      }

      currentFiles.removeAll(processedFiles);

      for (String file : currentFiles) {
        final long fileId = readCache.openFile(file, writeCache);
        readCache.deleteFile(fileId, writeCache);
      }

      final OWriteAheadLog restoreLog = createWalFromIBUFiles(walTempDir);
      OLogSequenceNumber restoreLsn = null;

      if (restoreLog != null) {
        final OLogSequenceNumber beginLsn = restoreLog.begin();
        restoreLsn = restoreFrom(beginLsn, restoreLog);

        restoreLog.delete();
      }

      if (maxLsn != null && writeAheadLog != null) {
        if (restoreLsn != null && restoreLsn.compareTo(maxLsn) > 0) {
          maxLsn = restoreLsn;
        }

        writeAheadLog.moveLsnAfter(maxLsn);
      }

      if (walTempDir != null) {
        if (!walTempDir.delete()) {
          OLogManager.instance().error(this, "Can not remove temporary backup directory " + walTempDir.getAbsolutePath());
        }
      }

      openClusters();
      openIndexes();

      makeFullCheckpoint();
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  private void replaceConfiguration(ZipInputStream zipInputStream) throws IOException {
    final Map<String, Object> loadProperties = configuration.getLoadProperties();

    byte[] buffer = new byte[1024];

    int rb = 0;
    while (true) {
      final int b = zipInputStream.read(buffer, rb, buffer.length - rb);

      if (b == -1)
        break;

      rb += b;

      if (rb == buffer.length) {
        byte[] oldBuffer = buffer;

        buffer = new byte[buffer.length << 1];
        System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length);
      }
    }

    configuration.fromStream(buffer, 0, rb);
    configuration.update();

    configuration.close();

    configuration.load(loadProperties);
  }

}
