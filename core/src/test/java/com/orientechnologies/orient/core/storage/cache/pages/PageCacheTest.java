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

package com.orientechnologies.orient.core.storage.cache.pages;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.storage.cache.*;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.impl.local.OFullCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Sitnikov
 */
@RunWith(Parameterized.class)
public class PageCacheTest {

  @Parameterized.Parameters
  public static Object[] cases() {
    return new Object[] { new Case() {
      @Override
      public OPageCache createPageCache(OReadCache readCache) {
        return new OPassthroughPageCache(readCache);
      }

      @Override
      public int[][] vectors() {
        return new int[][] { { 6, 0, 0, 0 }, { 6, 1, 0, 0 }, { 6, 2, 0, 0 }, { 6, 3, 0, 0 }, { 6, 4, 0, 0 }, { 6, 5, 0, 0 },
            { 6, 6, 0, 0 }, { 6, 6, 1, 0 }, { 6, 7, 1, 0 }, { 6, 7, 2, 0 }, { 6, 8, 2, 0 }, { 6, 8, 3, 0 }, { 6, 8, 4, 0 },
            { 6, 9, 4, 0 }, { 6, 11, 4, 0 }, { 6, 12, 4, 0 }, { 6, 12, 5, 0 }, { 6, 13, 5, 0 }, { 6, 13, 6, 0 }, { 6, 14, 6, 0 },
            { 7, 16, 17, 3 } };
      }
    }, new Case() {
      @Override
      public OPageCache createPageCache(OReadCache readCache) {
        return new OTinyPageCache(readCache, 2);
      }

      @Override
      public int[][] vectors() {
        return new int[][] { { 6, 0, 0, 0 }, { 6, 1, 0, 0 }, { 6, 1, 0, 0 }, { 6, 2, 0, 0 }, { 6, 2, 0, 0 }, { 6, 2, 0, 0 },
            { 6, 3, 0, 0 }, { 6, 3, 0, 0 }, { 6, 4, 0, 0 }, { 6, 4, 0, 0 }, { 6, 5, 1, 0 }, { 6, 5, 1, 0 }, { 6, 5, 1, 0 },
            { 6, 6, 3, 0 }, { 6, 8, 3, 0 }, { 6, 9, 3, 0 }, { 6, 9, 4, 0 }, { 6, 10, 4, 0 }, { 6, 10, 5, 0 }, { 6, 11, 5, 0 },
            { 7, 12, 13, 3 } };
      }

    }, new Case() {
      @Override
      public OPageCache createPageCache(OReadCache readCache) {
        return new OLruPageCache(readCache, 2, 2);
      }

      @Override
      public int[][] vectors() {
        return new int[][] { { 6, 0, 0, 0 }, { 6, 1, 0, 0 }, { 6, 1, 0, 0 }, { 6, 2, 0, 0 }, { 6, 2, 0, 0 }, { 6, 2, 0, 0 },
            { 6, 3, 0, 0 }, { 6, 3, 0, 0 }, { 6, 3, 0, 0 }, { 6, 3, 0, 0 }, { 6, 3, 0, 0 }, { 6, 3, 0, 0 }, { 6, 3, 0, 0 },
            { 6, 4, 0, 0 }, { 6, 4, 0, 0 }, { 6, 5, 0, 0 }, { 6, 5, 1, 0 }, { 6, 6, 1, 0 }, { 6, 6, 1, 0 }, { 6, 7, 2, 0 },
            { 7, 8, 9, 3 } };
      }
    } };
  }

  private final Case case_;

  private WriteCache      writeCache;
  private ReadCache       readCache;
  private OPageCache      pageCache;
  private AtomicOperation atomicOperation;
  private WriteAheadLog   writeAheadLog;

  public PageCacheTest(Case case_) {
    this.case_ = case_;
  }

  @Before
  public void before() throws IOException {
    this.writeCache = new WriteCache();
    this.readCache = new ReadCache();
    this.pageCache = case_.createPageCache(readCache);
    this.atomicOperation = new AtomicOperation(new OLogSequenceNumber(0, 0), null, readCache, writeCache, -1,
        new OPerformanceStatisticManager(null, 10000, 10000));
    this.writeAheadLog = new WriteAheadLog();

    writeCache.addFile("file");
    readCache.allocateNewPage(0, writeCache);
    readCache.allocateNewPage(0, writeCache);
    readCache.allocateNewPage(0, writeCache);
    readCache.allocateNewPage(0, writeCache);
    readCache.allocateNewPage(0, writeCache);
    readCache.allocateNewPage(0, writeCache);
  }

  @After
  public void after() throws IOException {
    writeCache.close();
  }

  @Test
  public void testCommit() throws IOException {
    final int[][] vectors = case_.vectors();

    OCacheEntry page0;
    OCacheEntry page1;
    OCacheEntry page2;
    OCacheEntry page3;
    OCacheEntry page4;
    OCacheEntry page5;
    OCacheEntry page6;

    verifyStep(vectors, 0);

    page0 = atomicOperation.loadPage(0, 0, false, 1);
    verifyStep(vectors, 1);

    page0 = atomicOperation.loadPage(0, 0, false, 1);
    verifyStep(vectors, 2);

    page1 = atomicOperation.loadPage(0, 1, false, 1);
    verifyStep(vectors, 3);

    page1 = atomicOperation.loadPage(0, 1, false, 1);
    verifyStep(vectors, 4);

    page0 = atomicOperation.loadPage(0, 0, false, 1);
    verifyStep(vectors, 5);

    page2 = atomicOperation.loadPage(0, 2, false, 1);
    verifyStep(vectors, 6);

    atomicOperation.releasePage(page1);
    verifyStep(vectors, 7);

    page2 = atomicOperation.loadPage(0, 2, false, 1);
    verifyStep(vectors, 8);

    atomicOperation.releasePage(page1);
    verifyStep(vectors, 9);

    page2 = atomicOperation.loadPage(0, 2, false, 1);
    verifyStep(vectors, 10);

    atomicOperation.releasePage(page2);
    verifyStep(vectors, 11);

    atomicOperation.releasePage(page2);
    verifyStep(vectors, 12);

    page3 = atomicOperation.loadPage(0, 3, false, 1);
    verifyStep(vectors, 13);

    page1 = atomicOperation.loadPage(0, 1, false, 1);
    page2 = atomicOperation.loadPage(0, 2, false, 1);
    verifyStep(vectors, 14);

    page4 = atomicOperation.loadPage(0, 4, false, 1);
    verifyStep(vectors, 15);

    atomicOperation.releasePage(page4);
    verifyStep(vectors, 16);

    page5 = atomicOperation.loadPage(0, 5, false, 1);
    verifyStep(vectors, 17);

    atomicOperation.releasePage(page1);
    verifyStep(vectors, 18);

    page6 = atomicOperation.addPage(0);
    page5 = atomicOperation.loadPage(0, 5, false, 1);
    verifyStep(vectors, 19);

    // cached at this point for tiny and lru
    atomicOperation.getChanges(0, 0).setByteValue(page0.getCachePointer().getSharedBuffer(), (byte) 0, 0);
    // not cached at this point for tiny and lru
    atomicOperation.getChanges(0, 1).setByteValue(page0.getCachePointer().getSharedBuffer(), (byte) 0, 0);
    // completely new page, not cached
    atomicOperation.getChanges(0, 6).setByteValue(page0.getCachePointer().getSharedBuffer(), (byte) 0, 0);

    atomicOperation.releasePage(page0);
    atomicOperation.releasePage(page0);
    atomicOperation.releasePage(page0);
    atomicOperation.releasePage(page2);
    atomicOperation.releasePage(page2);
    atomicOperation.releasePage(page3);
    atomicOperation.releasePage(page5);
    atomicOperation.releasePage(page5);
    atomicOperation.releasePage(page6);

    atomicOperation.commitChanges(writeAheadLog);
    verifyStep(vectors, 20);
  }

  private void verifyStep(int[][] vectors, int step) {
    final int[] stepVector = vectors[step];

    assertEquals("step #" + step, stepVector[0], writeCache.pagesCreated);
    assertEquals("step #" + step, stepVector[1], writeCache.pagesLoaded);
    assertEquals("step #" + step, stepVector[2], readCache.pagesReleased);
    assertEquals("step #" + step, stepVector[3], writeCache.pagesStored);
  }

  private interface Case {
    OPageCache createPageCache(OReadCache readCache);

    int[][] vectors();
  }

  private static class ReadCache implements OReadCache {

    public int pagesReleased = 0;

    @Override
    public long addFile(String fileName, OWriteCache writeCache) throws IOException {
      return 0;
    }

    @Override
    public long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
      return 0;
    }

    @Override
    public OCacheEntry load(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
        throws IOException {
      final OCachePointer[] pointers = writeCache.load(fileId, pageIndex, pageCount, false, null);
      return pointers == null || pointers.length == 0 ? null : new OCacheEntry(fileId, pageIndex, pointers[0], false);
    }

    @Override
    public void pinPage(OCacheEntry cacheEntry) throws IOException {

    }

    @Override
    public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache) throws IOException {
      final OCachePointer[] pointers = writeCache.load(fileId, writeCache.getFilledUpTo(fileId), 1, true, null);
      return new OCacheEntry(fileId, pointers[0].getPageIndex(), pointers[0], false);
    }

    @Override
    public void release(OCacheEntry cacheEntry, OWriteCache writeCache) {
      ++pagesReleased;
      if (cacheEntry.isDirty())
        writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());
    }

    @Override
    public long getUsedMemory() {
      return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public void truncateFile(long fileId, OWriteCache writeCache) throws IOException {

    }

    @Override
    public void closeFile(long fileId, boolean flush, OWriteCache writeCache) throws IOException {

    }

    @Override
    public void deleteFile(long fileId, OWriteCache writeCache) throws IOException {

    }

    @Override
    public void deleteStorage(OWriteCache writeCache) throws IOException {

    }

    @Override
    public void closeStorage(OWriteCache writeCache) throws IOException {

    }

    @Override
    public void loadCacheState(OWriteCache writeCache) {

    }

    @Override
    public void storeCacheState(OWriteCache writeCache) {

    }
  }

  private static class WriteCache implements OWriteCache {

    public int pagesCreated = 0;
    public int pagesLoaded  = 0;
    public int pagesStored  = 0;

    private int              fileCounter = 0;
    private long[]           fileSizes   = new long[10];
    private List<ByteBuffer> buffers     = new ArrayList<ByteBuffer>();

    @Override
    public String restoreFileById(long fileId) throws IOException {
      return null;
    }

    @Override
    public void startFuzzyCheckpoints() {

    }

    @Override
    public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {

    }

    @Override
    public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {

    }

    @Override
    public long bookFileId(String fileName) throws IOException {
      return 0;
    }

    @Override
    public long loadFile(String fileName) throws IOException {
      return 0;
    }

    @Override
    public long addFile(String fileName) throws IOException {
      return fileCounter++;
    }

    @Override
    public long addFile(String fileName, long fileId) throws IOException {
      return 0;
    }

    @Override
    public long fileIdByName(String fileName) {
      return 0;
    }

    @Override
    public boolean checkLowDiskSpace() {
      return false;
    }

    @Override
    public void makeFuzzyCheckpoint() {

    }

    @Override
    public boolean exists(String fileName) {
      return false;
    }

    @Override
    public boolean exists(long fileId) {
      return false;
    }

    @Override
    public Future store(long fileId, long pageIndex, OCachePointer dataPointer) {
      ++pagesStored;
      return null;
    }

    @Override
    public OCachePointer[] load(long fileId, long startPageIndex, int pageCount, boolean addNewPages, OModifiableBoolean cacheHit)
        throws IOException {

      final long size = getFilledUpTo(fileId);
      final boolean exists = startPageIndex < size;

      if (!exists && !addNewPages)
        return null;

      if (exists)
        ++pagesLoaded;
      else {
        final long created = startPageIndex - size + 1;
        pagesCreated += created;
        fileSizes[(int) fileId] += created;
      }

      return new OCachePointer[] { new OCachePointer(acquireBuffer(), OByteBufferPool.instance(), null, fileId, startPageIndex) };
    }

    private ByteBuffer acquireBuffer() {
      final ByteBuffer byteBuffer = OByteBufferPool.instance().acquireDirect(false);
      buffers.add(byteBuffer);
      return byteBuffer;
    }

    @Override
    public void flush(long fileId) {

    }

    @Override
    public void flush() {

    }

    @Override
    public long getFilledUpTo(long fileId) throws IOException {
      return fileSizes[(int) fileId];
    }

    @Override
    public long getExclusiveWriteCachePagesSize() {
      return 0;
    }

    @Override
    public void deleteFile(long fileId) throws IOException {

    }

    @Override
    public void truncateFile(long fileId) throws IOException {

    }

    @Override
    public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {

    }

    @Override
    public long[] close() throws IOException {
      for (ByteBuffer b : buffers)
        OByteBufferPool.instance().release(b);
      buffers.clear();
      return new long[0];
    }

    @Override
    public void close(long fileId, boolean flush) throws IOException {

    }

    @Override
    public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
      return new OPageDataVerificationError[0];
    }

    @Override
    public long[] delete() throws IOException {
      return new long[0];
    }

    @Override
    public String fileNameById(long fileId) {
      return null;
    }

    @Override
    public int getId() {
      return 0;
    }

    @Override
    public Map<String, Long> files() {
      return null;
    }

    @Override
    public int pageSize() {
      return 0;
    }

    @Override
    public boolean fileIdsAreEqual(long firsId, long secondId) {
      return false;
    }

    @Override
    public void addBackgroundExceptionListener(OBackgroundExceptionListener listener) {

    }

    @Override
    public void removeBackgroundExceptionListener(OBackgroundExceptionListener listener) {

    }

    @Override
    public File getRootDirectory() {
      return null;
    }

    @Override
    public int internalFileId(long fileId) {
      return 0;
    }

    @Override
    public long externalFileId(int fileId) {
      return 0;
    }

    @Override
    public OPerformanceStatisticManager getPerformanceStatisticManager() {
      return null;
    }
  }

  private class AtomicOperation extends OAtomicOperation {

    public AtomicOperation(OLogSequenceNumber startLSN, OOperationUnitId operationUnitId, OReadCache readCache,
        OWriteCache writeCache, int storageId, OPerformanceStatisticManager performanceStatisticManager) {
      super(startLSN, operationUnitId, readCache, writeCache, storageId, performanceStatisticManager);
    }

    @Override
    protected OPageCache createPageCache(OReadCache readCache) {
      return pageCache;
    }
  }

  private static class WriteAheadLog implements OWriteAheadLog {

    @Override
    public OLogSequenceNumber logFuzzyCheckPointStart(OLogSequenceNumber flushedLsn) throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber logFuzzyCheckPointEnd() throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber logFullCheckpointStart() throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber logFullCheckpointEnd() throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber getLastCheckpoint() {
      return null;
    }

    @Override
    public OLogSequenceNumber begin() throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber end() {
      return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, OOperationUnitId unitId)
        throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback,
        OLogSequenceNumber startLsn, Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber log(OWALRecord record) throws IOException {
      return new OLogSequenceNumber(0, 0);
    }

    @Override
    public void truncate() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void close(boolean flush) throws IOException {

    }

    @Override
    public void delete() throws IOException {

    }

    @Override
    public void delete(boolean flush) throws IOException {

    }

    @Override
    public OWALRecord read(OLogSequenceNumber lsn) throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber next(OLogSequenceNumber lsn) throws IOException {
      return null;
    }

    @Override
    public OLogSequenceNumber getFlushedLsn() {
      return null;
    }

    @Override
    public void cutTill(OLogSequenceNumber lsn) throws IOException {

    }

    @Override
    public void addFullCheckpointListener(OFullCheckpointRequestListener listener) {

    }

    @Override
    public void removeFullCheckpointListener(OFullCheckpointRequestListener listener) {

    }

    @Override
    public void moveLsnAfter(OLogSequenceNumber lsn) throws IOException {

    }

    @Override
    public void preventCutTill(OLogSequenceNumber lsn) throws IOException {

    }

    @Override
    public File[] nonActiveSegments(long fromSegment) {
      return new File[0];
    }

    @Override
    public long activeSegment() {
      return 0;
    }

    @Override
    public void newSegment() throws IOException {

    }

    @Override
    public long getPreferredSegmentCount() {
      return 0;
    }

    @Override
    public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {

    }

    @Override
    public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {

    }
  }

}
