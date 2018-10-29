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

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OBackupInProgressException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OCASDiskWriteAheadLog;
import com.orientechnologies.orient.core.tx.OTransactionInternal;

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
  public static final  String        IBU_EXTENSION                 = ".ibu";
  public static final  String        CONF_ENTRY_NAME               = "database.ocf";
  public static final  String        INCREMENTAL_BACKUP_DATEFORMAT = "yyyy-MM-dd-HH-mm-ss";
  private static final String        CONF_UTF_8_ENTRY_NAME         = "database_utf8.ocf";
  private final        AtomicBoolean backupInProgress              = new AtomicBoolean(false);

  private List<OEnterpriseStorageOperationListener> listeners = Collections.synchronizedList(new ArrayList<>());

  public OEnterpriseLocalPaginatedStorage(String name, String filePath, String mode, int id, OReadCache readCache,
      OClosableLinkedContainer<Long, OFileClassic> files, long walMaxSize) throws IOException {
    super(name, filePath, mode, id, readCache, files, walMaxSize);
    OLogManager.instance().info(this, "Enterprise storage installed correctly.");
  }

  @Override
  public String incrementalBackup(final String backupDirectory) {
    return incrementalBackup(new File(backupDirectory));
  }

  private String incrementalBackup(final File backupDirectory) {
    String fileName = "";

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
          OLogManager.instance().error(this, ibuFile.getAbsolutePath() + " is closed but can not be deleted", null);
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
        OLogManager.instance().error(this, "Can not close %s file", e, fileName);
      }
    }

    return fileName;
  }

  public void registerStorageListener(OEnterpriseStorageOperationListener listener) {
    this.listeners.add(listener);
  }

  public void unRegisterStorageListener(OEnterpriseStorageOperationListener listener) {
    this.listeners.remove(listener);
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

    checkOpenness();
    if (!backupInProgress.compareAndSet(false, true)) {
      throw new OBackupInProgressException(
          "You are trying to start incremental backup but it is in progress now, please wait till it will be finished", getName(),
          OErrorCode.BACKUP_IN_PROGRESS);
    }

    stateLock.acquireReadLock();
    try {

      checkOpenness();
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
            final OLogSequenceNumber freezeLsn;

            final long newSegmentFreezeId = atomicOperationsManager.freezeAtomicOperations(null, null);
            try {
              final OLogSequenceNumber startLsn = writeAheadLog.end();

              if (startLsn != null)
                freezeLsn = startLsn;
              else
                freezeLsn = new OLogSequenceNumber(0, 0);

              writeAheadLog.addCutTillLimit(freezeLsn);

              writeAheadLog.appendNewSegment();
              startSegment = writeAheadLog.activeSegment();
            } finally {
              atomicOperationsManager.releaseAtomicOperations(newSegmentFreezeId);
            }

            try {
              lastLsn = backupPagesWithChanges(fromLsn, zipOutputStream);
              final ZipEntry configurationEntry = new ZipEntry(CONF_UTF_8_ENTRY_NAME);

              zipOutputStream.putNextEntry(configurationEntry);
              final byte[] btConf = ((OStorageConfigurationImpl) getConfiguration()).toStream(Charset.forName("UTF-8"));

              zipOutputStream.write(btConf);
              zipOutputStream.closeEntry();

              final OLogSequenceNumber lastWALLsn = copyWALToIncrementalBackup(zipOutputStream, startSegment);

              if (lastWALLsn != null && (lastLsn == null || lastWALLsn.compareTo(lastLsn) > 0)) {
                lastLsn = lastWALLsn;
              }
            } finally {
              writeAheadLog.removeCutTillLimit(freezeLsn);
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
      fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

      final long filledUpTo = writeCache.getFilledUpTo(fileId);
      final ZipEntry zipEntry = new ZipEntry(fileName);

      stream.putNextEntry(zipEntry);

      final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serialize(fileId, binaryFileId, 0);
      stream.write(binaryFileId, 0, binaryFileId.length);

      for (long pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry = readCache.loadForRead(fileId, pageIndex, true, writeCache, 1, true);
        cacheEntry.acquireSharedLock();
        try {
          final OLogSequenceNumber pageLsn = ODurablePage
              .getLogSequenceNumberFromPage(cacheEntry.getCachePointer().getBufferDuplicate());

          if (changeLsn == null || pageLsn.compareTo(changeLsn) > 0) {

            final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];
            OLongSerializer.INSTANCE.serializeNative(pageIndex, data, 0);
            ODurablePage.getPageData(cacheEntry.getCachePointer().getBufferDuplicate(), data, OLongSerializer.LONG_SIZE, pageSize);

            stream.write(data);

            if (lastLsn == null || pageLsn.compareTo(lastLsn) > 0) {
              lastLsn = pageLsn;
            }
          }
        } finally {
          cacheEntry.releaseSharedLock();
          readCache.releaseFromRead(cacheEntry, writeCache);
        }
      }

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
      sharedContainer.clearResources();

      final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      final ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream, Charset.forName(configuration.getCharset()));
      final int pageSize = writeCache.pageSize();

      ZipEntry zipEntry;
      OLogSequenceNumber maxLsn = null;

      List<String> processedFiles = new ArrayList<String>();

      if (isFull) {
        final Map<String, Long> files = writeCache.files();
        for (Map.Entry<String, Long> entry : files.entrySet()) {
          final long fileId = writeCache.fileIdByName(entry.getKey());

          assert entry.getValue().equals(fileId);
          readCache.deleteFile(fileId, writeCache);
        }
      }

      final File walTempDir = createWalTempDirectory();

      entryLoop:
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        if (zipEntry.getName().equals(CONF_ENTRY_NAME)) {
          replaceConfiguration(zipInputStream, Charset.defaultCharset());

          continue;
        }

        if (zipEntry.getName().equals(CONF_UTF_8_ENTRY_NAME)) {
          replaceConfiguration(zipInputStream, Charset.forName("UTF-8"));

          continue;
        }

        if (zipEntry.getName().toLowerCase(serverLocale).endsWith(OCASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION)) {
          addFileToDirectory(zipEntry.getName(), zipInputStream, walTempDir);

          continue;
        }

        final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
        OIOUtils.readFully(zipInputStream, binaryFileId, 0, binaryFileId.length);

        final long expectedFileId = OLongSerializer.INSTANCE.deserialize(binaryFileId, 0);
        Long fileId;

        if (!writeCache.exists(zipEntry.getName())) {
          fileId = readCache.addFile(zipEntry.getName(), expectedFileId, writeCache);
        } else {
          fileId = writeCache.fileIdByName(zipEntry.getName());
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
                processedFiles.add(zipEntry.getName());
                continue entryLoop;
              }
            }

            rb += b;
          }

          final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(data, 0);

          OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, true, writeCache, 1, true, null);

          if (cacheEntry == null) {
            do {
              if (cacheEntry != null)
                readCache.releaseFromWrite(cacheEntry, writeCache);

              cacheEntry = readCache.allocateNewPage(fileId, writeCache, true, null);
            } while (cacheEntry.getPageIndex() != pageIndex);
          }

          try {
            final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
            final OLogSequenceNumber backedUpPageLsn = ODurablePage.getLogSequenceNumber(OLongSerializer.LONG_SIZE, data);
            if (isFull) {
              buffer.position(0);
              buffer.put(data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

              if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                maxLsn = backedUpPageLsn;
              }
            } else {
              final OLogSequenceNumber currentPageLsn = ODurablePage.getLogSequenceNumberFromPage(buffer);
              if (backedUpPageLsn.compareTo(currentPageLsn) > 0) {
                buffer.position(0);
                buffer.put(data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

                if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                  maxLsn = backedUpPageLsn;
                }
              }
            }
          } finally {
            readCache.releaseFromWrite(cacheEntry, writeCache);
          }
        }
      }

      currentFiles.removeAll(processedFiles);

      for (String file : currentFiles) {
        if (writeCache.exists(file)) {
          final long fileId = writeCache.fileIdByName(file);
          readCache.deleteFile(fileId, writeCache);
        }
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
          OLogManager.instance().error(this, "Can not remove temporary backup directory " + walTempDir.getAbsolutePath(), null);
        }
      }

      openClusters();
      openIndexes();

      makeFullCheckpoint();
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(ORecordId iRid, String iFetchPlan, boolean iIgnoreCache,
      boolean prefetchRecords, ORecordCallback<ORawBuffer> iCallback) {

    try {
      return super.readRecord(iRid, iFetchPlan, iIgnoreCache, prefetchRecords, iCallback);
    } finally {
      listeners.forEach((l) -> {
        l.onRead();
      });
    }
  }

  @Override
  public List<ORecordOperation> commit(OTransactionInternal clientTx) {
    List<ORecordOperation> operations = super.commit(clientTx);
    listeners.forEach((l) -> l.onCommit(operations));
    return operations;
  }

  @Override
  public void rollback(OTransactionInternal clientTx) {
    super.rollback(clientTx);
    listeners.forEach((l) -> l.onRollback());
  }

  @Override
  public void rollback(OMicroTransaction microTransaction) {
    super.rollback(microTransaction);
    listeners.forEach((l) -> l.onRollback());
  }

  private void replaceConfiguration(ZipInputStream zipInputStream, Charset charset) throws IOException {
    OContextConfiguration config = getConfiguration().getContextConfiguration();

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

    ((OStorageConfigurationImpl) getConfiguration()).fromStream(buffer, 0, rb, charset);
    ((OStorageConfigurationImpl) getConfiguration()).update();

    ((OStorageConfigurationImpl) getConfiguration()).close();

    ((OStorageConfigurationImpl) getConfiguration()).load(config);
  }

}
