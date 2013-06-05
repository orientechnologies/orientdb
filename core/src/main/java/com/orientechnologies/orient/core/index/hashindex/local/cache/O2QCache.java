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
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.CRC32;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllLRUListEntriesAreUsed;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Artem Loginov
 * @since 14.03.13
 */
public class O2QCache implements ODiskCache {
  public static final long                                     MAGIC_NUMBER = 0xFACB03FEL;

  public final int                                             writeQueueLength;

  private int                                                  maxSize;
  private int                                                  K_IN;
  private int                                                  K_OUT;

  private final int                                            pageSize;

  private LRUList                                              am;
  private LRUList                                              a1out;
  private LRUList                                              a1in;

  private final ODirectMemory                                  directMemory;

  private final Map<Long, OFileClassic>                        files;

  /**
   * List of pages which were flushed out of the buffer but were not written to the disk.
   */
  private final Map<FileLockKey, Long>                         evictedPages;

  /**
   * Contains all pages in cache for given file, not only dirty onces.
   */
  private final Map<Long, Set<Long>>                           filePages;

  /**
   * Keys is a file id. Values is a sorted set of dirty pages.
   */
  private final Map<Long, SortedMap<Long, OLogSequenceNumber>> dirtyPages;

  private final Object                                         syncObject;
  private final OStorageLocalAbstract                          storageLocal;

  private final OWriteAheadLog                                 writeAheadLog;

  private final boolean                                        syncOnPageFlush;
  private long                                                 fileCounter  = 1;
  private int                                                  crcOffset;

  public O2QCache(long maxMemory, int writeQueueLength, ODirectMemory directMemory, OWriteAheadLog writeAheadLog, int pageSize,
      OStorageLocalAbstract storageLocal, boolean syncOnPageFlush) {

    this.writeQueueLength = writeQueueLength;
    this.writeAheadLog = writeAheadLog;
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.storageLocal = storageLocal;
    this.syncOnPageFlush = syncOnPageFlush;
    this.files = new HashMap<Long, OFileClassic>();
    this.filePages = new HashMap<Long, Set<Long>>();
    this.dirtyPages = new HashMap<Long, SortedMap<Long, OLogSequenceNumber>>();

    this.evictedPages = new HashMap<FileLockKey, Long>();

    long tmpMaxSize = maxMemory / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      maxSize = Integer.MAX_VALUE;
    } else {
      maxSize = (int) tmpMaxSize;
    }

    K_IN = maxSize >> 2;
    K_OUT = maxSize >> 1;

    am = new LRUList();
    a1out = new LRUList();
    a1in = new LRUList();

    syncObject = new Object();
  }

  LRUList getAm() {
    return am;
  }

  LRUList getA1out() {
    return a1out;
  }

  LRUList getA1in() {
    return a1in;
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

      filePages.put(fileId, new HashSet<Long>());
      dirtyPages.put(fileId, new TreeMap<Long, OLogSequenceNumber>());

      return fileId;
    }
  }

  @Override
  public void markDirty(long fileId, long pageIndex) {
    synchronized (syncObject) {
      LRUEntry lruEntry = a1in.get(fileId, pageIndex);

      if (lruEntry != null) {
        doMarkDirty(fileId, pageIndex, lruEntry);
        return;
      }

      lruEntry = am.get(fileId, pageIndex);
      if (lruEntry != null) {
        doMarkDirty(fileId, pageIndex, lruEntry);
      } else
        throw new IllegalStateException("Requested page number " + pageIndex + " for file " + files.get(fileId).getName()
            + " is not in cache");
    }
  }

  private void doMarkDirty(long fileId, long pageIndex, LRUEntry lruEntry) {
    if (lruEntry.isDirty)
      return;

    dirtyPages.get(fileId).put(pageIndex, lruEntry.loadedLSN);
    lruEntry.isDirty = true;
  }

  private OLogSequenceNumber getLogSequenceNumberFromPage(long dataPointer) {
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + (2 * OIntegerSerializer.INT_SIZE));
    final int segment = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    return new OLogSequenceNumber(segment, position);
  }

  @Override
  public long load(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final LRUEntry lruEntry = updateCache(fileId, pageIndex);
      lruEntry.usageCounter++;
      return lruEntry.dataPointer;
    }
  }

  @Override
  public void release(long fileId, long pageIndex) {
    synchronized (syncObject) {
      LRUEntry lruEntry = get(fileId, pageIndex);
      if (lruEntry != null)
        lruEntry.usageCounter--;
      else
        throw new IllegalStateException("record should be released is already free!");
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
      final OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null || !fileClassic.isOpen())
        return;

      final SortedMap<Long, OLogSequenceNumber> dirtyPages = this.dirtyPages.get(fileId);

      for (Iterator<Long> iterator = dirtyPages.keySet().iterator(); iterator.hasNext();) {
        Long pageIndex = iterator.next();
        LRUEntry lruEntry = get(fileId, pageIndex);

        if (lruEntry == null) {
          final Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
          if (dataPointer != null) {
            flushData(fileId, pageIndex, dataPointer);
            iterator.remove();
          }
        } else {
          if (lruEntry.usageCounter == 0) {
            flushData(fileId, lruEntry.pageIndex, lruEntry.dataPointer);
            iterator.remove();
            lruEntry.isDirty = false;
          } else {
            throw new OBlockedPageException("Unable to perform flush file because some pages is in use.");
          }
        }
      }

      fileClassic.synch();
    }
  }

  @Override
  public void closeFile(final long fileId) throws IOException {
    synchronized (syncObject) {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null || !fileClassic.isOpen())
        return;

      final Set<Long> pageIndexes = filePages.get(fileId);
      Long[] sortedPageIndexes = new Long[pageIndexes.size()];
      sortedPageIndexes = pageIndexes.toArray(sortedPageIndexes);
      Arrays.sort(sortedPageIndexes);

      final SortedMap<Long, OLogSequenceNumber> fileDirtyPages = dirtyPages.get(fileId);

      for (Long pageIndex : sortedPageIndexes) {
        LRUEntry lruEntry = get(fileId, pageIndex);
        if (lruEntry != null) {
          if (lruEntry.usageCounter == 0) {
            lruEntry = remove(fileId, pageIndex);

            flushData(fileId, pageIndex, lruEntry.dataPointer);
            fileDirtyPages.remove(pageIndex);

            directMemory.free(lruEntry.dataPointer);
          }
        } else {
          Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
          if (dataPointer != null) {
            flushData(fileId, pageIndex, dataPointer);
            fileDirtyPages.remove(pageIndex);
          }
        }
      }

      pageIndexes.clear();
      fileClassic.close();
    }
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      if (isOpen(fileId))
        truncateFile(fileId);

      files.get(fileId).delete();

      files.remove(fileId);
      filePages.remove(fileId);
      dirtyPages.remove(fileId);
    }
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    synchronized (syncObject) {
      final Set<Long> pageEntries = filePages.get(fileId);
      for (Long pageIndex : pageEntries) {
        LRUEntry lruEntry = get(fileId, pageIndex);
        if (lruEntry != null) {
          if (lruEntry.usageCounter == 0) {
            lruEntry = remove(fileId, pageIndex);
            if (lruEntry.dataPointer != ODirectMemory.NULL_POINTER)
              directMemory.free(lruEntry.dataPointer);
          }
        } else {
          Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
          if (dataPointer != null)
            directMemory.free(dataPointer);
        }
      }

      dirtyPages.get(fileId).clear();

      pageEntries.clear();
      files.get(fileId).shrink(0);
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
  public void flushBuffer() throws IOException {
    synchronized (syncObject) {
      for (long fileId : files.keySet())
        flushFile(fileId);
    }
  }

  @Override
  public void clear() throws IOException {
    synchronized (syncObject) {
      flushBuffer();

      am.clear();
      a1in.clear();
      a1out.clear();
      for (Set<Long> fileEntries : filePages.values())
        fileEntries.clear();
      for (SortedMap<Long, OLogSequenceNumber> fileDirtyPages : dirtyPages.values())
        fileDirtyPages.clear();
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
  public boolean isOpen(long fileId) {
    synchronized (syncObject) {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic != null)
        return fileClassic.isOpen();
    }

    return false;
  }

  private LRUEntry updateCache(long fileId, long pageIndex) throws IOException {
    LRUEntry lruEntry = am.get(fileId, pageIndex);
    if (lruEntry != null) {
      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lruEntry.loadedLSN);

      return lruEntry;
    }

    lruEntry = a1out.remove(fileId, pageIndex);
    if (lruEntry != null) {
      removeColdestPageIfNeeded();

      CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
      lruEntry.dataPointer = cacheResult.dataPointer;
      lruEntry.isDirty = cacheResult.isDirty;

      OLogSequenceNumber lsn;
      if (cacheResult.isDirty)
        lsn = dirtyPages.get(fileId).get(pageIndex);
      else
        lsn = getLogSequenceNumberFromPage(cacheResult.dataPointer);

      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lsn);
      return lruEntry;
    }

    lruEntry = a1in.get(fileId, pageIndex);
    if (lruEntry != null)
      return lruEntry;

    try {
      removeColdestPageIfNeeded();
    } catch (OAllLRUListEntriesAreUsed e) {
      if (OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.getValueAsBoolean()) {
        maxSize = (int) Math.ceil(maxSize * (1 + OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_STEP.getValueAsFloat()));
        K_IN = maxSize >> 2;
        K_OUT = maxSize >> 1;
      } else {
        throw e;
      }
    }

    CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
    OLogSequenceNumber lsn;
    if (cacheResult.isDirty)
      lsn = dirtyPages.get(fileId).get(pageIndex);
    else
      lsn = getLogSequenceNumberFromPage(cacheResult.dataPointer);

    lruEntry = a1in.putToMRU(fileId, pageIndex, cacheResult.dataPointer, cacheResult.isDirty, lsn);

    filePages.get(fileId).add(pageIndex);

    return lruEntry;
  }

  private void removeColdestPageIfNeeded() throws IOException {
    if (am.size() + a1in.size() >= maxSize) {
      if (a1in.size() > K_IN) {
        LRUEntry removedFromAInEntry = a1in.removeLRU();
        checkSizeIncreaseNeccerity(removedFromAInEntry);
        assert removedFromAInEntry.usageCounter == 0;
        evictFileContent(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex, removedFromAInEntry.dataPointer,
            removedFromAInEntry.isDirty);

        a1out.putToMRU(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex, ODirectMemory.NULL_POINTER, false, null);
        if (a1out.size() > K_OUT) {
          LRUEntry removedEntry = a1out.removeLRU();
          checkSizeIncreaseNeccerity(removedEntry);
          assert removedEntry.usageCounter == 0;
          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        LRUEntry removedEntry = am.removeLRU();
        checkSizeIncreaseNeccerity(removedEntry);
        assert removedEntry.usageCounter == 0;
        evictFileContent(removedEntry.fileId, removedEntry.pageIndex, removedEntry.dataPointer, removedEntry.isDirty);
        Set<Long> pageEntries = filePages.get(removedEntry.fileId);
        pageEntries.remove(removedEntry.pageIndex);
      }
    }
  }

  private void checkSizeIncreaseNeccerity(LRUEntry removedFromAInEntry) {
    if (removedFromAInEntry == null) {
      String message = "All records in aIn queue in 2q cache are used!";
      OLogManager.instance().warn(this, message);
      if (OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.getValueAsBoolean()) {
        OLogManager.instance().warn(this, "Cache size will be increased.");
      }
      throw new OAllLRUListEntriesAreUsed(message);
    }
  }

  private CacheResult cacheFileContent(long fileId, long pageIndex) throws IOException {
    FileLockKey key = new FileLockKey(fileId, pageIndex);
    if (evictedPages.containsKey(key))
      return new CacheResult(true, evictedPages.remove(key));

    final OFileClassic fileClassic = files.get(fileId);
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    byte[] content = new byte[pageSize];
    long dataPointer;
    if (fileClassic.getFilledUpTo() >= endPosition) {
      fileClassic.read(startPosition, content, content.length);
      dataPointer = directMemory.allocate(content);
    } else {
      fileClassic.allocateSpace((int) (endPosition - fileClassic.getFilledUpTo()));
      dataPointer = directMemory.allocate(content);
    }

    return new CacheResult(false, dataPointer);
  }

  private void evictFileContent(long fileId, long pageIndex, long dataPointer, boolean isDirty) throws IOException {
    if (isDirty) {
      if (evictedPages.size() >= writeQueueLength)
        flushEvictedPages();

      evictedPages.put(new FileLockKey(fileId, pageIndex), dataPointer);
    } else {
      directMemory.free(dataPointer);
    }
  }

  private void flushData(final long fileId, final long pageIndex, final long dataPointer) throws IOException {
    if (writeAheadLog != null) {
      OLogSequenceNumber lsn = getLogSequenceNumberFromPage(dataPointer);
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLSN();
      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = directMemory.get(dataPointer, pageSize);
    OLongSerializer.INSTANCE.serializeNative(MAGIC_NUMBER, content, 0);

    final int crc32 = calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final OFileClassic fileClassic = files.get(fileId);

    fileClassic.write(pageIndex * pageSize, content);

    if (syncOnPageFlush)
      fileClassic.synch();
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

  @Override
  public Set<ODirtyPage> logDirtyPagesTable() throws IOException {
    synchronized (syncObject) {
      if (writeAheadLog == null)
        return Collections.emptySet();

      Set<ODirtyPage> logDirtyPages = new HashSet<ODirtyPage>(dirtyPages.size());
      for (long fileId : dirtyPages.keySet()) {
        SortedMap<Long, OLogSequenceNumber> pages = dirtyPages.get(fileId);
        for (Map.Entry<Long, OLogSequenceNumber> pageEntry : pages.entrySet()) {
          final ODirtyPage logDirtyPage = new ODirtyPage(files.get(fileId).getName(), pageEntry.getKey(), pageEntry.getValue());
          logDirtyPages.add(logDirtyPage);
        }
      }

      writeAheadLog.logDirtyPages(logDirtyPages);
      return logDirtyPages;
    }
  }

  @Override
  public void forceSyncStoredChanges() throws IOException {
    synchronized (syncObject) {
      for (OFileClassic fileClassic : files.values())
        fileClassic.synch();
    }
  }

  private void flushEvictedPages() throws IOException {
    @SuppressWarnings("unchecked")
    Map.Entry<FileLockKey, Long>[] sortedPages = evictedPages.entrySet().toArray(new Map.Entry[evictedPages.size()]);
    Arrays.sort(sortedPages, new Comparator<Map.Entry>() {
      @Override
      public int compare(Map.Entry entryOne, Map.Entry entryTwo) {
        FileLockKey fileLockKeyOne = (FileLockKey) entryOne.getKey();
        FileLockKey fileLockKeyTwo = (FileLockKey) entryTwo.getKey();
        return fileLockKeyOne.compareTo(fileLockKeyTwo);
      }
    });

    for (Map.Entry<FileLockKey, Long> entry : sortedPages) {
      long evictedDataPointer = entry.getValue();
      FileLockKey fileLockKey = entry.getKey();

      flushData(fileLockKey.fileId, fileLockKey.pageIndex, evictedDataPointer);
      dirtyPages.get(fileLockKey.fileId).remove(fileLockKey.pageIndex);

      directMemory.free(evictedDataPointer);
    }

    evictedPages.clear();
  }

  int getMaxSize() {
    return maxSize;
  }

  private static class CacheResult {
    private final boolean isDirty;
    private final long    dataPointer;

    private CacheResult(boolean dirty, long dataPointer) {
      isDirty = dirty;
      this.dataPointer = dataPointer;
    }
  }

  private LRUEntry get(long fileId, long pageIndex) {
    LRUEntry lruEntry = am.get(fileId, pageIndex);

    if (lruEntry != null) {
      return lruEntry;
    }

    lruEntry = a1in.get(fileId, pageIndex);
    return lruEntry;
  }

  private LRUEntry remove(long fileId, long pageIndex) {
    LRUEntry lruEntry = am.remove(fileId, pageIndex);
    if (lruEntry != null) {
      if (lruEntry.usageCounter > 1)
        throw new IllegalStateException("Record cannot be removed because it is used!");
      return lruEntry;
    }
    lruEntry = a1out.remove(fileId, pageIndex);
    if (lruEntry != null) {
      return lruEntry;
    }
    lruEntry = a1in.remove(fileId, pageIndex);
    if (lruEntry != null && lruEntry.usageCounter > 1)
      throw new IllegalStateException("Record cannot be removed because it is used!");
    return lruEntry;
  }

  private int calculatePageCrc(byte[] pageData) {
    int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  private static final class FileLockKey implements Comparable<FileLockKey> {
    private final long fileId;
    private final long pageIndex;

    private FileLockKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      FileLockKey that = (FileLockKey) o;

      if (fileId != that.fileId)
        return false;
      if (pageIndex != that.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public int compareTo(FileLockKey otherKey) {
      if (fileId > otherKey.fileId)
        return 1;
      if (fileId < otherKey.fileId)
        return -1;

      if (pageIndex > otherKey.pageIndex)
        return 1;
      if (pageIndex < otherKey.pageIndex)
        return -1;

      return 0;
    }
  }
}
