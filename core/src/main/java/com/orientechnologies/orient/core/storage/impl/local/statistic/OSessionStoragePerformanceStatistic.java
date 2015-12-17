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
package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Container for performance statistic gathered after
 * call of {@link OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()}.
 *
 * @author Andrey Lomakin
 */
public class OSessionStoragePerformanceStatistic {
  /**
   * Amount of bytes in megabyte
   */
  private static final int MEGABYTE = 1024 * 1024;

  /**
   * Amount of nanoseconds in second
   */
  private static final int NANOS_IN_SECOND = 1000000000;

  /**
   * Thread local which contains performance statistic for current session.
   *
   * @see OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()
   * @see OAbstractPaginatedStorage#completeGatheringPerformanceStatisticForCurrentThread()
   */
  private static final ThreadLocal<OSessionStoragePerformanceStatistic> THREAD_LOCAL = new ThreadLocal<OSessionStoragePerformanceStatistic>();

  /**
   * Initiates new session for gathering performance statistic for storage.
   * Typically is used in {@link OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()}
   *
   * @param pageSize Size of page for storage is used for calculation of metrics which are measured in megabytes.
   */
  public static void initThreadLocalInstance(final int pageSize) {
    THREAD_LOCAL.set(new OSessionStoragePerformanceStatistic(pageSize));
  }

  /**
   * Stops current session of gathering of storage performance statistic.
   *
   * @return Performance statistic for current session.
   */
  public static OSessionStoragePerformanceStatistic clearThreadLocalInstance() {
    final OSessionStoragePerformanceStatistic result = THREAD_LOCAL.get();
    THREAD_LOCAL.remove();
    return result;
  }

  /**
   * @return instance of container for gathering of storage performance statistic if session of gathering performance is initiated by
   * call if {@link #initThreadLocalInstance(int)} or <code>null</code> otherwise.
   */
  public static OSessionStoragePerformanceStatistic getStatisticInstance() {
    return THREAD_LOCAL.get();
  }

  /**
   * Stack of time stamps which is used to init clock in startTimerXXX methods.
   */
  private Deque<Long> timeStamps = new ArrayDeque<Long>();

  /**
   * Amount of times when cache was accessed during the session.
   */
  private long cacheAccessCount = 0;

  /**
   * Amount of "cache hit" times during the session.
   */
  private long cacheHit = 0;

  /**
   * Summary time which was spent on access of pages from file system.
   */
  private long pageReadFromFileTime = 0;

  /**
   * Amount of pages in total which were accessed from file system.
   */
  private long pageReadFromFileCount = 0;

  /**
   * Summary time which was spent on access of pages from disk cache.
   */
  private long pageReadFromCacheTime = 0;

  /**
   * Amount of pages in total which were accessed from disk cache.
   */
  private long pageReadFromCacheCount = 0;

  /**
   * Summary time which was spent to write pages to disk cache.
   */
  private long pageWriteToCacheTime = 0;

  /**
   * Amount of pages in total which were written to disk cache.
   */
  private long pageWriteToCacheCount = 0;

  /**
   * Amount of times when atomic operation commit was performed.
   */
  private long commitCount = 0;

  /**
   * Summary time which was spent on atomic operation commits.
   */
  private long commitTime = 0;

  /**
   * Page size in cache.
   */
  private final int pageSize;

  /**
   * Object which is used to get current PC nano time.
   */
  private final NanoTimer nanoTimer;

  /**
   * Creates object and initiates it with value of size of page in cache.
   *
   * @param pageSize Page size in cache.
   */
  public OSessionStoragePerformanceStatistic(int pageSize) {
    this(pageSize, new NanoTimer() {
      @Override
      public long getNano() {
        return System.nanoTime();
      }
    });
  }

  /**
   * Creates object and initiates it with page size for storage and time service is needed
   * to get current value of PC nano time.
   *
   * @param pageSize  Size of page in storage
   * @param nanoTimer Service to get current value of PC nano time.
   */
  public OSessionStoragePerformanceStatistic(int pageSize, NanoTimer nanoTimer) {
    this.pageSize = pageSize;
    this.nanoTimer = nanoTimer;
  }

  /**
   * @return Read speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getReadSpeedFromCacheInMB() {
    final long pageSpeed = getReadSpeedFromCacheInPages();
    if (pageSpeed < 0)
      return -1;

    return (pageSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Read speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getReadSpeedFromCacheInPages() {
    if (pageReadFromCacheTime == 0)
      return -1;

    return (pageReadFromCacheCount * NANOS_IN_SECOND) / pageReadFromCacheTime;
  }

  /**
   * @return Read speed of data on file system level in pages per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getReadSpeedFromFileInPages() {
    if (pageReadFromFileTime == 0)
      return -1;

    return (pageReadFromFileCount * NANOS_IN_SECOND) / pageReadFromFileTime;
  }

  /**
   * @return Read speed of data on file system level in megabytes per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getReadSpeedFromFileInMB() {
    final long pageSpeed = getReadSpeedFromFileInPages();
    if (pageSpeed < 0)
      return -1;

    return (pageSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Amount of pages read from cache in total.
   */
  public long getAmountOfPagesReadFromCache() {
    return pageReadFromCacheCount;
  }

  /**
   * @return Amount of pages are read from file.
   */
  public long getAmountOfPagesReadFromFile() {
    return pageReadFromFileCount;
  }

  /**
   * @return Write speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getWriteSpeedInCacheInPages() {
    if (pageWriteToCacheTime == 0)
      return -1;

    return (pageWriteToCacheCount * NANOS_IN_SECOND) / pageWriteToCacheTime;
  }

  /**
   * @return Write speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getWriteSpeedInCacheInMB() {
    final long pageSpeed = getWriteSpeedInCacheInPages();
    if (pageSpeed < 0)
      return -1;

    return (pageSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Amount of pages written to cache.
   */
  public long getAmountOfPagesWrittenInCache() {
    return pageWriteToCacheCount;
  }

  /**
   * @return Average time of commit of atomic operation in nanoseconds
   * or value which is less than 0, which means that value can not be calculated.
   */
  public long getCommitTimeAvg() {
    if (commitCount == 0)
      return -1;

    return commitTime / commitCount;
  }

  /**
   * @return Percent of cache hits
   * or value which is less than 0, which means that value can not be calculated.
   */
  public int getCacheHits() {
    if (cacheAccessCount == 0)
      return -1;

    return (int) ((cacheHit * 100) / cacheAccessCount);
  }

  /**
   * Converts properties of given class into values of fields of returned document.
   * Names of fields equal to names of properties.
   *
   * @return Performance characteristics of storage gathered after call of
   * {@link OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()}
   */
  public ODocument toDocument() {
    final ODocument document = new ODocument();

    document.field("readSpeedFromCacheInMB", getReadSpeedFromCacheInMB(), OType.LONG);
    document.field("readSpeedFromCacheInPages", getReadSpeedFromCacheInPages(), OType.LONG);
    document.field("readSpeedFromFileInPages", getReadSpeedFromFileInPages(), OType.LONG);
    document.field("readSpeedFromFileInMB", getReadSpeedFromFileInMB(), OType.LONG);
    document.field("amountOfPagesReadFromCache", getAmountOfPagesReadFromCache(), OType.LONG);
    document.field("writeSpeedInCacheInPages", getWriteSpeedInCacheInPages(), OType.LONG);
    document.field("writeSpeedInCacheInMB", getWriteSpeedInCacheInMB(), OType.LONG);
    document.field("amountOfPagesWrittenInCache", getAmountOfPagesWrittenInCache(), OType.LONG);
    document.field("commitTimeAvg", getCommitTimeAvg(), OType.LONG);
    document.field("amountOfPagesReadFromFile", getAmountOfPagesReadFromFile(), OType.LONG);
    document.field("cacheHits", getCacheHits(), OType.INTEGER);

    return document;
  }

  /**
   * Increments counter of page accesses from cache.
   */
  public void incrementPageAccessOnCacheLevel() {
    cacheAccessCount++;
  }

  /**
   * Increments counter of cache hits
   */
  public void incrementCacheHit() {
    cacheHit++;
  }

  /**
   * Starts timer which counts how much time was spent on read of page from file system.
   */
  public void startPageReadFromFileTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from file system.
   *
   * @param readPages Amount of pages which were read by single call to file system.
   */
  public void stopPageReadFromFileTimer(int readPages) {
    final long entTs = nanoTimer.getNano();

    pageReadFromFileTime += (entTs - timeStamps.pop());
    pageReadFromFileCount += readPages;
  }

  /**
   * Starts timer which counts how much time was spent on read of page from disk cache.
   */
  public void startPageReadFromCacheTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from disk cache.
   */
  public void stopPageReadFromCacheTimer() {
    final long entTs = nanoTimer.getNano();
    pageReadFromCacheTime += (entTs - timeStamps.pop());
    pageReadFromCacheCount++;
  }

  /**
   * Starts timer which counts how much time was spent on write of page to disk cache.
   */
  public void startPageWriteInCacheTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent to write page to disk cache.
   */
  public void stopPageWriteInCacheTimer() {
    final long entTs = nanoTimer.getNano();

    pageWriteToCacheTime += (entTs - timeStamps.pop());
    pageWriteToCacheCount++;
  }

  /**
   * Starts timer which counts how much time was spent on atomic operation commit.
   */
  public void startCommitTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent on atomic operation commit.
   */
  public void stopCommitTimer() {
    final long entTs = nanoTimer.getNano();

    commitTime += (entTs - timeStamps.pop());
    commitCount++;
  }

  /**
   * Interface which is used by this tool to get current PC nano time.
   * Implementation which calls <code>System.nanoTime()</code> is used by default.
   */
  public interface NanoTimer {
    /**
     * @return Current PC nano time.
     */
    long getNano();
  }

}
