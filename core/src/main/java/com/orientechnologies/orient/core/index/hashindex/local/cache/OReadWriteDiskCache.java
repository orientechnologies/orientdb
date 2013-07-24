package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.CRC32;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class OReadWriteDiskCache implements ODiskCache {
  public static final long                        MAGIC_NUMBER = 0xFACB03FEL;

  private long                                    fileCounter  = 1;

  private int                                     maxSize;
  private final int                               pageSize;

  private final ODirectMemory                     directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
  private final ConcurrentMap<Long, OFileClassic> files        = new ConcurrentHashMap<Long, OFileClassic>();

  private final boolean                           syncOnPageFlush;

  private final O2QReadCache                      readCache;
  private final OWOWCache                         writeCache;

  private final Object                            syncObject   = new Object();
  private final OStorageLocalAbstract             storageLocal;
  private final OWriteAheadLog                    writeAheadLog;

  public OReadWriteDiskCache(long maxMemory, OWriteAheadLog writeAheadLog, int pageSize, OStorageLocalAbstract storageLocal,
      int writeGroupTTL, long pageFlushInterval, boolean syncOnPageFlush) {
    this.syncOnPageFlush = syncOnPageFlush;
    this.writeAheadLog = writeAheadLog;
    this.pageSize = pageSize;
    this.storageLocal = storageLocal;

    long tmpMaxSize = maxMemory / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      maxSize = Integer.MAX_VALUE;
    } else {
      maxSize = (int) tmpMaxSize;
    }

    int writeCacheSize = maxSize / 16;
    int readCacheSize = maxSize - writeCacheSize;

    writeCache = new OWOWCache(files, syncOnPageFlush, pageSize, writeGroupTTL, writeAheadLog, pageFlushInterval, writeCacheSize);
    readCache = new O2QReadCache(files, pageSize);
  }

  @Override
  public long openFile(String fileName) throws IOException {
    synchronized (syncObject) {
      long fileId = fileCounter++;

      OFileClassic fileClassic = new OFileClassic();
      String path = storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath() + File.separator + fileName);
      fileClassic.init(path, storageLocal.getMode());

      if (fileClassic.exists())
        fileClassic.open();
      else
        fileClassic.create(-1);

      files.put(fileId, fileClassic);

      return fileId;
    }
  }

  @Override
  public void markDirty(long fileId, long pageIndex) {
    synchronized (syncObject) {
      OCacheEntry cacheEntry = writeCache.get(fileId, pageIndex);
      if (cacheEntry != null)
        return;

      cacheEntry = readCache.load(fileId, pageIndex);
      writeCache.add(cacheEntry);
    }
  }

  @Override
  public long load(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      OCacheEntry cacheEntry = writeCache.load(fileId, pageIndex);
      if (cacheEntry != null)
        return cacheEntry.dataPointer;

      cacheEntry = readCache.load(fileId, pageIndex);
      return cacheEntry.dataPointer;
    }
  }

  @Override
  public void release(long fileId, long pageIndex) {
    synchronized (syncObject) {
      OCacheEntry cacheEntry = writeCache.get(fileId, pageIndex);
      if (cacheEntry != null) {
        cacheEntry.acquireExclusiveLock();
        try {
          cacheEntry.usageCounter--;
        } finally {
          cacheEntry.releaseExclusiveLock();
        }
        return;
      }

      cacheEntry = readCache.get(fileId, pageIndex);

      cacheEntry.acquireExclusiveLock();
      try {
        cacheEntry.usageCounter--;
      } finally {
        cacheEntry.releaseExclusiveLock();
      }
    }
  }

  @Override
  public long getFilledUpTo(long fileId) throws IOException {
    synchronized (syncObject) {
      return files.get(fileId).getFilledUpTo() / pageSize;
    }
  }

  @Override
  public void flushFile(long fileId) throws IOException {
    synchronized (syncObject) {
      writeCache.flush(fileId);
    }
  }

  @Override
  public void closeFile(long fileId) throws IOException {
    closeFile(fileId, true);
  }

  @Override
  public void closeFile(long fileId, boolean flush) throws IOException {
    synchronized (syncObject) {
      if (flush)
        writeCache.flush(fileId);

      writeCache.clear(fileId);
      readCache.clear(fileId);

      files.get(fileId).close();
    }
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      writeCache.clear(fileId);
      readCache.clear(fileId);

      files.get(fileId).delete();

      files.remove(fileId);
    }
  }

  @Override
  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      final OFileClassic file = files.get(fileId);
      final String osFileName = file.getName();
      if (osFileName.startsWith(oldFileName)) {
        final File newFile = new File(storageLocal.getStoragePath() + File.separator + newFileName
            + osFileName.substring(osFileName.lastIndexOf(oldFileName) + oldFileName.length()));
        boolean renamed = file.renameTo(newFile);
        while (!renamed) {
          OMemoryWatchDog.freeMemoryForResourceCleanup(100);
          renamed = file.renameTo(newFile);
        }
      }
    }
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    synchronized (syncObject) {
      readCache.clear(fileId);
      writeCache.clear(fileId);

      files.get(fileId).shrink(0);
    }
  }

  @Override
  public boolean wasSoftlyClosed(long fileId) throws IOException {
    synchronized (syncObject) {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null)
        return false;

      return fileClassic.wasSoftlyClosed();
    }
  }

  @Override
  public void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException {
    synchronized (syncObject) {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic != null)
        fileClassic.setSoftlyClosed(softlyClosed);
    }
  }

  @Override
  public void flushBuffer() throws IOException {
    synchronized (syncObject) {
      writeCache.flush();
    }
  }

  @Override
  public void clear() throws IOException {
    synchronized (syncObject) {
      writeCache.clear();
      readCache.clear();
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (syncObject) {
      clear();
      for (OFileClassic fileClassic : files.values()) {
        if (fileClassic.isOpen()) {
          fileClassic.synch();
          fileClassic.close();
        }
      }
    }
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;
    final List<OPageDataVerificationError> errors = new ArrayList<OPageDataVerificationError>();

    synchronized (syncObject) {
      for (long fileId : files.keySet()) {

        OFileClassic fileClassic = files.get(fileId);

        boolean fileIsCorrect;
        try {

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Flashing file " + fileClassic.getName() + "... ");

          flushFile(fileId);

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Start verification of content of " + fileClassic.getName() + "file ...");

          long time = System.currentTimeMillis();

          long filledUpTo = fileClassic.getFilledUpTo();
          fileIsCorrect = true;

          for (long pos = 0; pos < filledUpTo; pos += pageSize) {
            boolean checkSumIncorrect = false;
            boolean magicNumberIncorrect = false;

            byte[] data = new byte[pageSize];

            fileClassic.read(pos, data, data.length);

            long magicNumber = OLongSerializer.INSTANCE.deserializeNative(data, 0);

            if (magicNumber != MAGIC_NUMBER) {
              magicNumberIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage("Error: Magic number for page " + (pos / pageSize) + " in file "
                    + fileClassic.getName() + " does not much !!!");
              fileIsCorrect = false;
            }

            final int storedCRC32 = OIntegerSerializer.INSTANCE.deserializeNative(data, OLongSerializer.LONG_SIZE);

            final int calculatedCRC32 = calculatePageCrc(data);
            if (storedCRC32 != calculatedCRC32) {
              checkSumIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage("Error: Checksum for page " + (pos / pageSize) + " in file "
                    + fileClassic.getName() + " is incorrect !!!");
              fileIsCorrect = false;
            }

            if (magicNumberIncorrect || checkSumIncorrect)
              errors.add(new OPageDataVerificationError(magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileClassic
                  .getName()));

            if (commandOutputListener != null && System.currentTimeMillis() - time > notificationTimeOut) {
              time = notificationTimeOut;
              commandOutputListener.onMessage((pos / pageSize) + " pages were processed ...");
            }
          }
        } catch (IOException ioe) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Error: Error during processing of file " + fileClassic.getName() + ". "
                + ioe.getMessage());

          fileIsCorrect = false;
        }

        if (!fileIsCorrect) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file " + fileClassic.getName() + " is finished with errors.");
        } else {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file " + fileClassic.getName() + " is successfully finished.");
        }
      }

      return errors.toArray(new OPageDataVerificationError[errors.size()]);
    }
  }

  private int calculatePageCrc(byte[] pageData) {
    int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  @Override
  public Set<ODirtyPage> logDirtyPagesTable() throws IOException {
    return null;
  }

  @Override
  public void forceSyncStoredChanges() throws IOException {
    synchronized (syncObject) {
      for (OFileClassic fileClassic : files.values())
        fileClassic.synch();
    }
  }

  @Override
  public boolean isOpen(long fileId) {
    synchronized (syncObject) {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic != null)
        return fileClassic.isOpen();
      return false;
    }
  }
}
