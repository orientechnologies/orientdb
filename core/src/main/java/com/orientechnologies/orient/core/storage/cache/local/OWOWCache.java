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
package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.common.collection.closabledictionary.OClosableEntry;
import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.thread.OThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OQuarto;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OWriteCacheException;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * Write part of disk cache which is used to collect pages which were changed on read cache and store them to the disk in background
 * thread.
 * In current implementation only single background thread is used to store all changed data, despite of SSD parallelization
 * capabilities we suppose that better to write data in single big chunk by one thread than by many small chunks from many threads
 * introducing contention and multi threading overhead. Another reasons for usage of only one thread are
 * <ol> <li>That we should give room for readers to read data during data write phase</li> <li>It provides much less synchronization
 * overhead</li> </ol>
 * Background thread is running by with predefined intervals. Such approach allows SSD GC to use pauses to make some clean up of
 * half empty erase blocks.
 * Also write cache is used for checking of free space left on disk and putting of database in "read mode" if space limit is reached
 * and to perform fuzzy checkpoints.
 * Write cache holds two different type of pages, pages which are shared with read cache and pages which belong only to write cache
 * (so called exclusive pages).
 * Files in write cache are accessed by id , there are two types of ids, internal used inside of write cache and external used
 * outside of write cache. Presence of two types of ids is caused by the fact that read cache is global across all storages but each
 * storage has its own write cache. So all ids of files should be global across whole read cache. External id is created from
 * internal id by prefixing of internal id (in byte presentation) with bytes of write cache id which is unique across all storages
 * opened inside of single JVM. Write cache accepts external ids as file ids and converts them to internal ids inside of its
 * methods.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/23/13
 */
public final class OWOWCache extends OAbstractWriteCache implements OWriteCache, OCachePointer.WritersListener {
  /**
   * Extension for the file which contains mapping between file name and file id
   */
  private static final String NAME_ID_MAP_EXTENSION = ".cm";

  /**
   * Name for file which contains first version of binary format
   */
  private static final String NAME_ID_MAP_V1 = "name_id_map" + NAME_ID_MAP_EXTENSION;

  /**
   * Name for file which contains second version of binary format. Second version of format contains not only file name which is
   * used in write cache but also file name which is used in file system so those two names may be different which allows usage of
   * case sensitive file names.
   */
  private static final String NAME_ID_MAP_V2 = "name_id_map_v2" + NAME_ID_MAP_EXTENSION;

  /**
   * Name of file temporary which contains second version of binary format. Temporary name is used to prevent situation when DB is
   * crashed because of migration from first to second version of binary format and data are lost.
   *
   * @see #NAME_ID_MAP_V2
   * @see #convertNameIdMapFromV1ToV2()
   */
  private static final String NAME_ID_MAP_V2_T = "name_id_map_v2_t" + NAME_ID_MAP_EXTENSION;

  /**
   * Marks pages which have a checksum stored.
   */
  public static final long MAGIC_NUMBER_WITH_CHECKSUM = 0xFACB03FEL;

  /**
   * Marks pages which have no checksum stored.
   */
  private static final long MAGIC_NUMBER_WITHOUT_CHECKSUM = 0xEF30BCAFL;

  private static final int MAGIC_NUMBER_OFFSET = 0;

  private static final int CHECKSUM_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int PAGE_OFFSET_TO_CHECKSUM_FROM = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

  private static final int CHUNK_SIZE = 32 * 1024 * 1024;

  /**
   * Executor which runs in single thread all tasks are related to flush of write cache data.
   */
  private static final OScheduledThreadPoolExecutorWithLogging commitExecutor;

  /**
   * Executor which is used to call event listeners in  background thread
   */
  private static final ExecutorService cacheEventsPublisher;

  static {
    cacheEventsPublisher = new OThreadPoolExecutorWithLogging(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
        new CacheEventsPublisherFactory());

    commitExecutor = new OScheduledThreadPoolExecutorWithLogging(1, new FlushThreadFactory());
    commitExecutor.setMaximumPoolSize(1);
  }

  /**
   * Limit of free space on disk after which database will be switched to "read only" mode
   */
  private final long freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024L * 1024L;

  /**
   * Interval between values of {@link #amountOfNewPagesAdded} field, after which we will check amount of free space on disk
   */
  private final int diskSizeCheckInterval = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL_IN_PAGES.getValueAsInteger();

  /**
   * Listeners which are called once we detect that there is not enough space left on disk to work. Mostly used to put database in
   * "read only" mode
   */
  private final List<WeakReference<OLowDiskSpaceListener>> lowDiskSpaceListeners = new CopyOnWriteArrayList<>();

  /**
   * Listeners which are called once we detect that some of the pages of files are broken.
   */
  private final List<WeakReference<OPageIsBrokenListener>> pageIsBrokenListeners = new CopyOnWriteArrayList<>();

  /**
   * The last amount of pages which were added to the file system by database when check of free space was performed. It is used
   * together with {@link #amountOfNewPagesAdded} to detect when new disk space check should be performed.
   */
  private final AtomicLong lastDiskSpaceCheck = new AtomicLong(0);

  /**
   * Path to the storage root directory where all files served by write cache will be stored
   */
  private final Path storagePath;

  private final FileStore fileStore;

  /**
   * Container of all files are managed by write cache. That is special type of container which ensures that only limited amount of
   * files is open at the same time and opens closed files upon request
   */
  private final OClosableLinkedContainer<Long, OFileClassic> files;

  /**
   * The main storage of pages for write cache. If pages is hold by write cache it should be present in this map. Map is ordered by
   * position to speed up flush of pages to the disk
   */
  private final ConcurrentHashMap<PageKey, OCachePointer> writeCachePages = new ConcurrentHashMap<>();

  /**
   * Storage for the pages which are hold only by write cache and are not shared with read cache.
   */
  private final ConcurrentSkipListSet<PageKey> exclusiveWritePages = new ConcurrentSkipListSet<>();

  private final OReadersWriterSpinLock dirtyPagesLock = new OReadersWriterSpinLock();

  /**
   * Container for dirty pages. Dirty pages table is concept taken from ARIES protocol. It contains earliest LSNs of operations on
   * each page which is potentially changed but not flushed to the disk.
   * It allows us by calculation of minimal LSN contained by this container calculate which part of write ahead log may be already
   * truncated.
   * "dirty pages" itself is managed using following algorithm.
   * <ol> <li>Just after acquiring the exclusive lock on page we fetch LSN of latest record logged into WAL</li> <li>If page with
   * given index is absent into table we add it to this container</li> </ol>
   * Because we add last WAL LSN if we are going to modify page, it means that we can calculate smallest LSN of operation which is
   * not flushed to the log yet without locking of all operations on database.
   * There is may be situation when thread locks the page but did not add LSN to the dirty pages table yet. If at the moment of
   * start of iteration over the dirty pages table we have a non empty dirty pages table it means that new operation on page will
   * have LSN bigger than any LSN already stored in table.
   * If dirty pages table is empty at the moment of iteration it means at the moment of start of iteration all page changes were
   * flushed to the disk.
   */
  private final ConcurrentHashMap<PageKey, OLogSequenceNumber> dirtyPages = new ConcurrentHashMap<>();

  /**
   * Copy of content of {@link #dirtyPages} table at the moment when {@link #convertSharedDirtyPagesToLocal()} was called. This
   * field is not thread safe because it is used inside of tasks which are running inside of {@link #commitExecutor} thread. It is
   * used to keep results of postprocessing of {@link #dirtyPages} table.
   * Every time we invoke {@link #convertSharedDirtyPagesToLocal()} all content of dirty pages is removed and copied to current
   * field and {@link #localDirtyPagesBySegment} filed.
   * Such approach is possible because {@link #dirtyPages} table is filled by many threads but is read only from inside of {@link
   * #commitExecutor} thread.
   */
  private final HashMap<PageKey, OLogSequenceNumber> localDirtyPages = new HashMap<>();

  /**
   * Copy of content of {@link #dirtyPages} table sorted by log segment and pages sorted by page index.
   *
   * @see #localDirtyPages for details
   */
  private final TreeMap<Long, TreeSet<PageKey>> localDirtyPagesBySegment = new TreeMap<>();

  /**
   * This counter is need for "free space" check implementation. Once amount of added pages is reached some threshold, amount of
   * free space available on disk will be checked.
   */
  private final AtomicLong amountOfNewPagesAdded = new AtomicLong();

  /**
   * Approximate amount of all pages contained by write cache at the moment
   */
  private final AtomicLong writeCacheSize = new AtomicLong();

  /**
   * Amount of exclusive pages are hold by write cache.
   */
  private final AtomicLong exclusiveWriteCacheSize = new AtomicLong();

  /**
   * Amount of times when maximum limit of exclusive write pages allowed to be stored by write cache is reached
   */
  private final LongAdder cacheOverflowCountSum = new LongAdder();

  /**
   * Serialized is used to encode/decode names of files are managed by write cache.
   */
  private final OBinarySerializer<String> stringSerializer;

  /**
   * Size of single page in cache in bytes.
   */
  private final int pageSize;

  /**
   * WAL instance
   */
  private final OWriteAheadLog writeAheadLog;

  /**
   * Lock manager is used to acquire locks in RW mode for cases when we are going to read or write page from write cache.
   */
  private final OLockManager<PageKey> lockManager = new OPartitionedLockManager<>();

  /**
   * We acquire lock managed by this manager in read mode if we need to read data from files, and in write mode if we
   * add/remove/truncate file.
   */
  private final OReadersWriterSpinLock filesLock = new OReadersWriterSpinLock();

  /**
   * Mapping between case sensitive file names are used in write cache and file's internal id. Name of file in write cache is case
   * sensitive and can be different from file name which is used to store file in file system.
   */
  private final ConcurrentMap<String, Integer> nameIdMap = new ConcurrentHashMap<>();

  /**
   * Mapping between file's internal ids and case sensitive file names are used in write cache. Name of file in write cache is case
   * sensitive and can be different from file name which is used to store file in file system.
   */
  private final ConcurrentMap<Integer, String> idNameMap = new ConcurrentHashMap<>();

  /**
   * File which contains map between names and ids of files used in write cache. Each record in file has following format (size of
   * file name in bytes (int), file name (string), internal file id)
   * Internal file id may have negative value, it means that this file existed in storage but was deleted. We still keep mapping
   * between files so if file with given name will be created again it will get the same file id, which can be handy during process
   * of restore of storage data after crash.
   */
  private FileChannel nameIdMapHolder;

  private final Random fileIdGen = new Random();

  /**
   * Path to the {@link #nameIdMapHolder} file.
   */
  private Path nameIdMapHolderPath;

  /**
   * Write cache id , which should be unique across all storages.
   */
  private final int id;

  /**
   * Pool of direct memory <code>ByteBuffer</code>s. We can not use them directly because they do not have deallocator.
   */
  private final OByteBufferPool bufferPool;

  private final String storageName;

  private volatile OChecksumMode checksumMode;

  /**
   * Error thrown during data flush.
   * Once error registered no more write operations are allowed.
   */
  private Throwable flushError;

  private long flushedPagesSum;
  private long flushedPagesTime;

  private long walFlushTime;
  private long walFlushCount;

  private long chunkSizeSum;
  private long chunkSizeTimeSum;
  private long chunkSizeCountSum;

  private final LongAdder loadedPagesSum     = new LongAdder();
  private final LongAdder loadedPagesTimeSum = new LongAdder();

  private final LongAdder cacheOverflowTimeSum = new LongAdder();

  private long exclusivePagesSum;
  private long lsnPagesSum;

  private long lsnFlushIntervalSum;
  private long lsnFlushIntervalCount;

  private long statisticTs = -1;

  private long lastTsLSNFlush           = -1;
  private long lsnFlushIntervalBoundary = -1;
  private int  lastSegmentCount         = 1;

  private final int exclusiveWriteCacheMaxSize;

  private final boolean callFsync;

  private final boolean printCacheStatistics;
  private final int     statisticsPrintInterval;

  private long lastFlushTs                      = -1;
  private long backgroundExclusiveFlushBoundary = -1;

  private long lsnPagesFlushIntervalSum;
  private int  lsnPagesFlushIntervalCount;

  private final int chunkSize;

  private final    long      pagesFlushInterval;
  private volatile boolean   stopFlush;
  private volatile Future<?> flushFuture;

  private final ConcurrentHashMap<ExclusiveFlushTask, CountDownLatch> triggeredTasks = new ConcurrentHashMap<>();

  private final int shutdownTimeout;

  /**
   * Listeners which are called when exception in background data flush thread is happened.
   */
  private final List<WeakReference<OBackgroundExceptionListener>> backgroundExceptionListeners = new CopyOnWriteArrayList<>();

  public OWOWCache(final int pageSize, final OByteBufferPool bufferPool, final OWriteAheadLog writeAheadLog,
      final long pagesFlushInterval, final int shutdownTimeout, final long exclusiveWriteCacheMaxSize, final Path storagePath,
      final String storageName, final OBinarySerializer<String> stringSerializer,
      final OClosableLinkedContainer<Long, OFileClassic> files, final int id, final OChecksumMode checksumMode,
      final boolean callFsync, final boolean printCacheStatistics, final int statisticsPrintInterval) {

    this.shutdownTimeout = shutdownTimeout;
    this.pagesFlushInterval = pagesFlushInterval;
    this.callFsync = callFsync;

    this.printCacheStatistics = printCacheStatistics;
    this.statisticsPrintInterval = statisticsPrintInterval;

    filesLock.acquireWriteLock();
    try {
      this.id = id;
      this.files = files;
      this.chunkSize = CHUNK_SIZE / pageSize;

      this.pageSize = pageSize;
      this.writeAheadLog = writeAheadLog;
      this.bufferPool = bufferPool;

      this.checksumMode = checksumMode;
      this.exclusiveWriteCacheMaxSize = normalizeMemory(exclusiveWriteCacheMaxSize, pageSize);

      this.storagePath = storagePath;
      try {
        this.fileStore = Files.getFileStore(this.storagePath);
      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error during retrieving of file store"), e);
      }

      this.stringSerializer = stringSerializer;
      this.storageName = storageName;

      if (pagesFlushInterval > 0) {
        flushFuture = commitExecutor.schedule(new PeriodicFlushTask(), pagesFlushInterval, TimeUnit.MILLISECONDS);
      }

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Loads files already registered in storage. Has to be called before usage of this cache
   */
  public void loadRegisteredFiles() throws IOException, InterruptedException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Adds listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to trigger
   */
  @Override
  public void addBackgroundExceptionListener(final OBackgroundExceptionListener listener) {
    backgroundExceptionListeners.add(new WeakReference<>(listener));
  }

  /**
   * Removes listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to remove
   */
  @Override
  public void removeBackgroundExceptionListener(final OBackgroundExceptionListener listener) {
    final List<WeakReference<OBackgroundExceptionListener>> itemsToRemove = new ArrayList<>(1);

    for (final WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener l = ref.get();
      if (l != null && l.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    backgroundExceptionListeners.removeAll(itemsToRemove);
  }

  /**
   * Fires event about exception is thrown in data flush thread
   */
  private void fireBackgroundDataFlushExceptionEvent(final Throwable e) {
    for (final WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener listener = ref.get();
      if (listener != null) {
        listener.onException(e);
      }
    }
  }

  private static int normalizeMemory(final long maxSize, final int pageSize) {
    final long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }

  /**
   * Directory which contains all files managed by write cache.
   *
   * @return Directory which contains all files managed by write cache or <code>null</code> in case of in memory database.
   */
  @Override
  public Path getRootDirectory() {
    return storagePath;
  }

  @Override
  public void addLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    lowDiskSpaceListeners.add(new WeakReference<>(listener));
  }

  /**
   * @inheritDoc
   */
  @Override
  public void addPageIsBrokenListener(final OPageIsBrokenListener listener) {
    pageIsBrokenListeners.add(new WeakReference<>(listener));
  }

  /**
   * @inheritDoc
   */
  @Override
  public void removePageIsBrokenListener(final OPageIsBrokenListener listener) {
    final List<WeakReference<OPageIsBrokenListener>> itemsToRemove = new ArrayList<>(1);

    for (final WeakReference<OPageIsBrokenListener> ref : pageIsBrokenListeners) {
      final OPageIsBrokenListener pageIsBrokenListener = ref.get();

      if (pageIsBrokenListener == null || pageIsBrokenListener.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    pageIsBrokenListeners.removeAll(itemsToRemove);
  }

  @Override
  public void removeLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    final List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<>(1);

    for (final WeakReference<OLowDiskSpaceListener> ref : lowDiskSpaceListeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    lowDiskSpaceListeners.removeAll(itemsToRemove);
  }

  /**
   * This method is called once new pages are added to the disk inside of
   * {@link OWriteCache#load(long, long, int, OModifiableBoolean, boolean)}  method.
   * If total amount of added pages minus amount of added pages at the time of last disk space check bigger than threshold value
   * {@link #diskSizeCheckInterval} new disk space check is performed and if amount of space left on disk less than threshold
   * {@link
   * #freeSpaceLimit} then database is switched in "read only" mode
   */
  private void freeSpaceCheckAfterNewPageAdd() throws IOException {
    final long newPagesAdded = amountOfNewPagesAdded.addAndGet(1);
    final long lastSpaceCheck = lastDiskSpaceCheck.get();

    if (newPagesAdded - lastSpaceCheck > diskSizeCheckInterval || lastSpaceCheck == 0) {
      //usable space may be less than free space
      final long freeSpace = Files.getFileStore(storagePath).getUsableSpace();

      if (freeSpace < freeSpaceLimit) {
        callLowSpaceListeners(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));
      }

      lastDiskSpaceCheck.lazySet(newPagesAdded);
    }
  }

  private void callLowSpaceListeners(final OLowDiskSpaceInformation information) {
    cacheEventsPublisher.execute(new Runnable() {
      @Override
      public void run() {
        for (final WeakReference<OLowDiskSpaceListener> lowDiskSpaceListenerWeakReference : lowDiskSpaceListeners) {
          final OLowDiskSpaceListener listener = lowDiskSpaceListenerWeakReference.get();
          if (listener != null) {
            try {
              listener.lowDiskSpace(information);
            } catch (final Exception e) {
              OLogManager.instance().error(this, "Error during notification of low disk space for storage " + storageName, e);
            }
          }
        }
      }
    });
  }

  private void callPageIsBrokenListeners(final String fileName, final long pageIndex) {
    cacheEventsPublisher.execute(new Runnable() {
      @Override
      public void run() {
        for (final WeakReference<OPageIsBrokenListener> pageIsBrokenListenerWeakReference : pageIsBrokenListeners) {
          final OPageIsBrokenListener listener = pageIsBrokenListenerWeakReference.get();
          if (listener != null) {
            try {
              listener.pageIsBroken(fileName, pageIndex);
            } catch (final Exception e) {
              OLogManager.instance().error(this, "Error during notification of page is broken for storage " + storageName, e);
            }
          }
        }
      }
    });
  }

  @Override
  public long bookFileId(final String fileName) {
    filesLock.acquireWriteLock();
    try {
      final Integer fileId = nameIdMap.get(fileName);

      if (fileId != null) {
        if (fileId < 0) {
          return composeFileId(id, -fileId);
        } else {
          throw new OStorageException("File " + fileName + " has already been added to the storage");
        }
      }

      while (true) {
        final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
        if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
          nameIdMap.put(fileName, -nextId);
          idNameMap.put(-nextId, fileName);
          return composeFileId(id, nextId);
        }
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public int pageSize() {
    return pageSize;
  }

  /**
   * @inheritDoc
   */
  @Override
  public boolean fileIdsAreEqual(final long firsId, final long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }

  @Override
  public long loadFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      Integer fileId = nameIdMap.get(fileName);
      final OFileClassic fileClassic;

      //check that file is already registered
      if (!(fileId == null || fileId < 0)) {
        final long externalId = composeFileId(id, fileId);
        fileClassic = files.get(externalId);

        if (fileClassic != null) {
          return externalId;
        } else {
          throw new OStorageException("File with given name " + fileName + " only partially registered in storage");
        }
      }

      if (fileId == null) {
        while (true) {
          final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
          if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
            fileId = nextId;
            break;
          }
        }
      } else {
        idNameMap.remove(fileId);
        fileId = -fileId;
      }

      fileClassic = createFileInstance(fileName, fileId);

      if (!fileClassic.exists()) {
        throw new OStorageException("File with name " + fileName + " does not exist in storage " + storageName);
      } else {
        // REGISTER THE FILE
        OLogManager.instance().debug(this,
            "File '" + fileName + "' is not registered in 'file name - id' map, but exists in file system. Registering it");

        openFile(fileClassic);

        final long externalId = composeFileId(id, fileId);
        files.add(externalId, fileClassic);

        nameIdMap.put(fileName, fileId);
        idNameMap.put(fileId, fileName);

        writeNameIdEntry(new NameFileIdEntry(fileName, fileId, fileClassic.getName()), true);

        return externalId;
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Load file was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public long addFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      Integer fileId = nameIdMap.get(fileName);
      final OFileClassic fileClassic;

      if (fileId != null && fileId >= 0) {
        throw new OStorageException("File with name " + fileName + " already exists in storage " + storageName);
      }

      if (fileId == null) {
        while (true) {
          final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
          if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
            fileId = nextId;
            break;
          }
        }
      } else {
        idNameMap.remove(fileId);
        fileId = -fileId;
      }

      fileClassic = createFileInstance(fileName, fileId);
      createFile(fileClassic, callFsync);

      final long externalId = composeFileId(id, fileId);
      files.add(externalId, fileClassic);

      nameIdMap.put(fileName, fileId);
      idNameMap.put(fileId, fileName);

      writeNameIdEntry(new NameFileIdEntry(fileName, fileId, fileClassic.getName()), true);

      return externalId;
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File add was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public long fileIdByName(final String fileName) {
    final Integer intId = nameIdMap.get(fileName);

    if (intId == null || intId < 0) {
      return -1;
    }

    return composeFileId(id, intId);
  }

  @Override
  public int internalFileId(final long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public long externalFileId(final int fileId) {
    return composeFileId(id, fileId);
  }

  @Override
  public Long getMinimalNotFlushedSegment() {
    final Future<Long> future = commitExecutor.submit(new FindMinDirtySegment());
    try {
      return future.get();
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void updateDirtyPagesTable(final OCachePointer pointer, final OLogSequenceNumber startLSN) {
    if (writeAheadLog == null) {
      return;
    }

    final long fileId = pointer.getFileId();
    final long pageIndex = pointer.getPageIndex();

    final PageKey pageKey = new PageKey(internalFileId(fileId), pageIndex);

    OLogSequenceNumber dirtyLSN;
    if (startLSN != null) {
      dirtyLSN = startLSN;
    } else {
      dirtyLSN = writeAheadLog.end();
    }

    if (dirtyLSN == null) {
      dirtyLSN = new OLogSequenceNumber(0, 0);
    }

    dirtyPagesLock.acquireReadLock();
    try {
      dirtyPages.putIfAbsent(pageKey, dirtyLSN);
    } finally {
      dirtyPagesLock.releaseReadLock();
    }
  }

  @Override
  public long addFile(final String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      OFileClassic fileClassic;

      final Integer existingFileId = nameIdMap.get(fileName);

      final int intId = extractFileId(fileId);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == intId) {
          throw new OStorageException("File with name '" + fileName + "'' already exists in storage '" + storageName + "'");
        } else {
          throw new OStorageException(
              "File with given name '" + fileName + "' already exists but has different id " + existingFileId + " vs. proposed "
                  + fileId);
        }
      }

      fileId = composeFileId(id, intId);
      fileClassic = files.get(fileId);

      if (fileClassic != null) {
        if (!fileClassic.getName().equals(createInternalFileName(fileName, intId))) {
          throw new OStorageException(
              "File with given id exists but has different name " + fileClassic.getName() + " vs. proposed " + fileName);
        }

        fileClassic.shrink(0);

        if (callFsync) {
          fileClassic.synch();
        }
      } else {
        fileClassic = createFileInstance(fileName, intId);
        createFile(fileClassic, callFsync);

        files.add(fileId, fileClassic);
      }

      idNameMap.remove(-intId);

      nameIdMap.put(fileName, intId);
      idNameMap.put(intId, fileName);

      writeNameIdEntry(new NameFileIdEntry(fileName, intId, fileClassic.getName()), true);

      return fileId;
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File add was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public boolean checkLowDiskSpace() throws IOException {
    final long freeSpace = fileStore.getUsableSpace();
    return freeSpace < freeSpaceLimit;
  }

  @Override
  public void makeFuzzyCheckpoint(final long segmentId, Optional<byte[]> lastMetadata) throws IOException {
    if (writeAheadLog != null) {
      filesLock.acquireReadLock();
      try {
        final OLogSequenceNumber startLSN = writeAheadLog.begin(segmentId);
        if (startLSN == null) {
          return;
        }

        writeAheadLog.logFuzzyCheckPointStart(startLSN, lastMetadata);

        for (final Integer intId : nameIdMap.values()) {
          if (intId < 0) {
            continue;
          }

          if (callFsync) {
            final long fileId = composeFileId(id, intId);
            final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
            try {
              final OFileClassic fileClassic = entry.get();
              fileClassic.synch();
            } finally {
              files.release(entry);
            }
          }

        }

        writeAheadLog.logFuzzyCheckPointEnd();
        writeAheadLog.flush();

        writeAheadLog.cutAllSegmentsSmallerThan(segmentId);
      } catch (final InterruptedException e) {
        throw OException.wrapException(new OStorageException("Fuzzy checkpoint was interrupted"), e);
      } finally {
        filesLock.releaseReadLock();
      }
    }
  }

  @Override
  public void flushTillSegment(final long segmentId) {
    final Future<Void> future = commitExecutor.submit(new FlushTillSegmentTask(segmentId));
    try {
      future.get();
    } catch (final Exception e) {
      throw ODatabaseException.wrapException(new OStorageException("Error during data flush"), e);
    }
  }

  @Override
  public boolean exists(final String fileName) {
    filesLock.acquireReadLock();
    try {
      final Integer intId = nameIdMap.get(fileName);

      if (intId != null && intId >= 0) {
        final OFileClassic fileClassic = files.get(externalFileId(intId));

        if (fileClassic == null) {
          return false;
        }

        return fileClassic.exists();
      }

      return false;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public boolean exists(long fileId) {
    filesLock.acquireReadLock();
    try {
      final int intId = extractFileId(fileId);
      fileId = composeFileId(id, intId);

      final OFileClassic file = files.get(fileId);

      if (file == null) {
        return false;
      }

      return file.exists();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void checkCacheOverflow() throws InterruptedException {
    while (exclusiveWriteCacheSize.get() > exclusiveWriteCacheMaxSize) {
      final CountDownLatch cacheBoundaryLatch = new CountDownLatch(1);
      final CountDownLatch completionLatch = new CountDownLatch(1);
      final ExclusiveFlushTask exclusiveFlushTask = new ExclusiveFlushTask(cacheBoundaryLatch, completionLatch);

      triggeredTasks.put(exclusiveFlushTask, completionLatch);
      commitExecutor.submit(exclusiveFlushTask);

      long startTs = 0;
      if (printCacheStatistics) {
        startTs = System.nanoTime();
      }

      cacheBoundaryLatch.await();
      if (printCacheStatistics) {
        final long endTs = System.nanoTime();

        cacheOverflowCountSum.increment();
        cacheOverflowTimeSum.add(endTs - startTs);
      }
    }
  }

  @Override
  public void store(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      final PageKey pageKey = new PageKey(intId, pageIndex);

      final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
      try {
        final OCachePointer pagePointer = writeCachePages.get(pageKey);

        if (pagePointer == null) {
          doPutInCache(dataPointer, pageKey);
        } else {
          assert pagePointer.equals(dataPointer);
        }

      } finally {
        groupLock.unlock();
      }

    } finally {
      filesLock.releaseReadLock();
    }
  }

  private void doPutInCache(final OCachePointer dataPointer, final PageKey pageKey) {
    writeCachePages.put(pageKey, dataPointer);

    writeCacheSize.incrementAndGet();

    dataPointer.setWritersListener(this);
    dataPointer.incrementWritersReferrer();
  }

  @Override
  public Map<String, Long> files() {
    filesLock.acquireReadLock();
    try {
      final Map<String, Long> result = new HashMap<>(1_000);

      for (final Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() > 0) {
          result.put(entry.getKey(), composeFileId(id, entry.getValue()));
        }
      }

      return result;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public OCachePointer[] load(final long fileId, final long startPageIndex, final int pageCount, final OModifiableBoolean cacheHit,
      final boolean verifyChecksums) throws IOException {
    final int intId = extractFileId(fileId);
    if (pageCount < 1) {
      throw new IllegalArgumentException("Amount of pages to load should be not less than 1 but provided value is " + pageCount);
    }

    filesLock.acquireReadLock();
    try {
      //first check that requested page is already cached so we do not need to load it from file
      final PageKey startPageKey = new PageKey(intId, startPageIndex);
      final Lock startPageLock = lockManager.acquireSharedLock(startPageKey);

      //check if page already presented in write cache
      final OCachePointer startPagePointer = writeCachePages.get(startPageKey);

      //page is not cached load it from file
      if (startPagePointer == null) {
        //load it from file and preload requested pages
        //there is small optimization
        //if we need single page no need to release already locked page
        final Lock[] pageLocks;
        final PageKey[] pageKeys;

        if (pageCount > 1) {
          startPageLock.unlock();

          pageKeys = new PageKey[pageCount];
          for (int i = 0; i < pageCount; i++) {
            pageKeys[i] = new PageKey(intId, startPageIndex + i);
          }

          pageLocks = lockManager.acquireSharedLocksInBatch(pageKeys);
        } else {
          pageLocks = new Lock[] { startPageLock };
          pageKeys = new PageKey[] { startPageKey };
        }

        final OCachePointer[] pagePointers;
        try {
          //load requested page and preload requested amount of pages
          pagePointers = loadFileContent(intId, startPageIndex, pageCount, verifyChecksums);

          if (pagePointers != null) {
            if (pagePointers.length == 0) {
              return pagePointers;
            }

            for (int n = 0; n < pagePointers.length; n++) {
              pagePointers[n].incrementReadersReferrer();

              if (n > 0) {
                final OCachePointer pagePointer = writeCachePages.get(pageKeys[n]);

                assert pageKeys[n].pageIndex == pagePointers[n].getPageIndex();

                //if page already exists in cache we should drop already loaded page and load cache page instead
                if (pagePointer != null) {
                  pagePointers[n].decrementReadersReferrer();
                  pagePointers[n] = pagePointer;
                  pagePointers[n].incrementReadersReferrer();
                }
              }
            }

            return pagePointers;
          }

        } finally {
          for (final Lock pageLock : pageLocks) {
            pageLock.unlock();
          }
        }

        return new OCachePointer[0];
      } else {
        startPagePointer.incrementReadersReferrer();
        startPageLock.unlock();

        cacheHit.setValue(true);

        return new OCachePointer[] { startPagePointer };
      }
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public int allocateNewPage(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    filesLock.acquireReadLock();
    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        final OFileClassic fileClassic = entry.get();
        long allocationIndex = fileClassic.getFileSize() / pageSize;

        while (true) {
          final Lock lock = lockManager.acquireExclusiveLock(new PageKey(intId, allocationIndex));
          try {
            if (fileClassic.getFileSize() == allocationIndex * pageSize) {
              final long allocatedPosition = fileClassic.allocateSpace(pageSize);
              assert allocationIndex * pageSize == allocatedPosition;

              //we check is it enough space on disk to continue to write data on it
              //otherwise we switch storage in read-only mode
              freeSpaceCheckAfterNewPageAdd();

              return (int) allocationIndex;
            } else {
              allocationIndex = fileClassic.getFileSize() / pageSize;
            }
          } finally {
            lock.unlock();
          }
        }
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Allocation of page was interrupted"), e);
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void addOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.incrementAndGet();
    exclusiveWritePages.add(new PageKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void removeOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.decrementAndGet();
    exclusiveWritePages.remove(new PageKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void flush(final long fileId) {
    final Future<Void> future = commitExecutor.submit(new FileFlushTask(Collections.singleton(extractFileId(fileId))));
    try {
      future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(new OInterruptedException("File flush was interrupted"), e);
    } catch (final Exception e) {
      throw OException.wrapException(new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  @Override
  public void flush() {

    final Future<Void> future = commitExecutor.submit(new FileFlushTask(nameIdMap.values()));
    try {
      future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(new OInterruptedException("File flush was interrupted"), e);
    } catch (final Exception e) {
      throw OException.wrapException(new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  @Override
  public long getFilledUpTo(long fileId) {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireReadLock();
    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        return entry.get().getFileSize() / pageSize;
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Calculation of file size was interrupted"), e);
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public long getExclusiveWriteCachePagesSize() {
    return exclusiveWriteCacheSize.get();
  }

  @Override
  public void deleteFile(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      final String fileName = doDeleteFile(intId);

      if (fileName != null) {
        final String name = idNameMap.get(intId);

        idNameMap.remove(intId);

        nameIdMap.put(name, -intId);
        idNameMap.put(-intId, name);

        writeNameIdEntry(new NameFileIdEntry(name, -intId, fileName), true);
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      removeCachedPages(intId);
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        entry.get().shrink(0);
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File truncation was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void replaceFileContentWith(long fileId, final Path newContentFile) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      removeCachedPages(intId);

      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        entry.get().replaceContentWith(newContentFile);
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File content replacement was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void renameFile(long fileId, final String newFileName) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);

      if (entry == null) {
        return;
      }

      final String oldOsFileName;
      final String newOsFileName = createInternalFileName(newFileName, intId);

      try {
        final OFileClassic file = entry.get();
        oldOsFileName = file.getName();

        final Path newFile = storagePath.resolve(newOsFileName);
        file.renameTo(newFile);
      } finally {
        files.release(entry);
      }

      final String oldFileName = idNameMap.get(intId);

      nameIdMap.remove(oldFileName);
      nameIdMap.put(newFileName, intId);

      idNameMap.put(intId, newFileName);

      writeNameIdEntry(new NameFileIdEntry(oldFileName, -1, oldOsFileName), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, intId, newOsFileName), true);
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Rename of file was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private void stopFlush() {
    stopFlush = true;

    for (final CountDownLatch completionLatch : triggeredTasks.values()) {
      try {
        if (!completionLatch.await(shutdownTimeout, TimeUnit.MINUTES)) {
          throw new OWriteCacheException("Can not shutdown data flush for storage " + storageName);
        }
      } catch (final InterruptedException e) {
        throw OException
            .wrapException(new OWriteCacheException("Flush of the data for storage " + storageName + " has been interrupted"), e);
      }
    }

    if (flushFuture != null) {
      try {
        flushFuture.get(shutdownTimeout, TimeUnit.MINUTES);
      } catch (final InterruptedException | CancellationException e) {
        //ignore
      } catch (final ExecutionException e) {
        throw OException.wrapException(new OWriteCacheException("Error in execution of data flush for storage " + storageName), e);
      } catch (final TimeoutException e) {
        throw OException.wrapException(new OWriteCacheException("Can not shutdown data flush for storage " + storageName), e);
      }
    }

  }

  @Override
  public long[] close() throws IOException {
    flush();

    stopFlush();

    filesLock.acquireWriteLock();
    try {
      final Collection<Integer> fileIds = nameIdMap.values();

      final List<Long> closedIds = new ArrayList<>(1_000);
      final Map<Integer, String> idFileNameMap = new HashMap<>(1_000);

      for (final Integer intId : fileIds) {
        if (intId >= 0) {
          final long extId = composeFileId(id, intId);
          final OFileClassic fileClassic = files.remove(extId);

          idFileNameMap.put(intId, fileClassic.getName());
          fileClassic.close();
          closedIds.add(extId);
        }
      }

      if (nameIdMapHolder != null) {
        nameIdMapHolder.truncate(0);

        for (final Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
          final String fileName;

          if (entry.getValue() >= 0) {
            fileName = idFileNameMap.get(entry.getValue());
          } else {
            fileName = entry.getKey();
          }

          writeNameIdEntry(new NameFileIdEntry(entry.getKey(), entry.getValue(), fileName), false);
        }

        nameIdMapHolder.force(true);
        nameIdMapHolder.close();
      }

      nameIdMap.clear();
      idNameMap.clear();

      final long[] ids = new long[closedIds.size()];
      int n = 0;

      for (final long id : closedIds) {
        ids[n] = id;
        n++;
      }

      return ids;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void close(long fileId, final boolean flush) {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      if (flush) {
        flush(intId);
      } else {
        removeCachedPages(intId);
      }

      if (!files.close(fileId)) {
        throw new OStorageException("Can not close file with id " + internalFileId(fileId) + " because it is still in use");
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public String restoreFileById(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    filesLock.acquireWriteLock();
    try {
      for (final Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() == -intId) {
          addFile(entry.getKey(), fileId);
          return entry.getKey();
        }
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    return null;
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(final OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;

    final List<OPageDataVerificationError> errors = new ArrayList<>(0);

    filesLock.acquireWriteLock();
    try {
      for (final Integer intId : nameIdMap.values()) {
        if (intId < 0) {
          continue;
        }

        checkFileStoredPages(commandOutputListener, notificationTimeOut, errors, intId);
      }

      return errors.toArray(new OPageDataVerificationError[0]);
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private void checkFileStoredPages(final OCommandOutputListener commandOutputListener,
      @SuppressWarnings("SameParameterValue") final int notificationTimeOut, final List<OPageDataVerificationError> errors,
      final Integer intId) throws InterruptedException {
    boolean fileIsCorrect;
    final long externalId = composeFileId(id, intId);
    final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalId);
    final OFileClassic fileClassic = entry.get();
    final String fileName = idNameMap.get(intId);

    try {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage("Flashing file " + fileName + "... ");
      }

      flush(intId);

      if (commandOutputListener != null) {
        commandOutputListener.onMessage("Start verification of content of " + fileName + "file ...\n");
      }

      long time = System.currentTimeMillis();

      final long filledUpTo = fileClassic.getFileSize();
      fileIsCorrect = true;

      for (long pos = 0; pos < filledUpTo; pos += pageSize) {
        boolean checkSumIncorrect = false;
        boolean magicNumberIncorrect = false;

        final byte[] data = new byte[pageSize];

        final OPointer pointer = bufferPool.acquireDirect(true);
        try {
          final ByteBuffer byteBuffer = pointer.getNativeByteBuffer();
          fileClassic.read(pos, byteBuffer, true);
          byteBuffer.rewind();
          byteBuffer.get(data);
        } finally {
          bufferPool.release(pointer);
        }

        final long magicNumber = OLongSerializer.INSTANCE.deserializeNative(data, MAGIC_NUMBER_OFFSET);

        if (magicNumber != MAGIC_NUMBER_WITH_CHECKSUM && magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
          magicNumberIncorrect = true;
          if (commandOutputListener != null) {
            commandOutputListener
                .onMessage("Error: Magic number for page " + (pos / pageSize) + " in file '" + fileName + "' does not match!\n");
          }
          fileIsCorrect = false;
        }

        if (magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
          final int storedCRC32 = OIntegerSerializer.INSTANCE.deserializeNative(data, CHECKSUM_OFFSET);

          final CRC32 crc32 = new CRC32();
          crc32.update(data, PAGE_OFFSET_TO_CHECKSUM_FROM, data.length - PAGE_OFFSET_TO_CHECKSUM_FROM);
          final int calculatedCRC32 = (int) crc32.getValue();

          if (storedCRC32 != calculatedCRC32) {
            checkSumIncorrect = true;
            if (commandOutputListener != null) {
              commandOutputListener
                  .onMessage("Error: Checksum for page " + (pos / pageSize) + " in file '" + fileName + "' is incorrect!\n");
            }
            fileIsCorrect = false;
          }
        }

        if (magicNumberIncorrect || checkSumIncorrect) {
          errors.add(new OPageDataVerificationError(magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileName));
        }

        if (commandOutputListener != null && System.currentTimeMillis() - time > notificationTimeOut) {
          time = notificationTimeOut;
          commandOutputListener.onMessage((pos / pageSize) + " pages were processed...\n");
        }
      }
    } catch (final IOException ioe) {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage("Error: Error during processing of file '" + fileName + "'. " + ioe.getMessage() + "\n");
      }

      fileIsCorrect = false;
    } finally {
      files.release(entry);
    }

    if (!fileIsCorrect) {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage("Verification of file '" + fileName + "' is finished with errors.\n");
      }
    } else {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage("Verification of file '" + fileName + "' is successfully finished.\n");
      }
    }
  }

  @Override
  public long[] delete() throws IOException {
    final List<Long> result = new ArrayList<>(1_000);
    filesLock.acquireWriteLock();
    try {
      for (final int intId : nameIdMap.values()) {
        if (intId < 0) {
          continue;
        }

        final long externalId = composeFileId(id, intId);
        doDeleteFile(externalId);
        result.add(externalId);
      }

      if (nameIdMapHolderPath != null) {
        if (Files.exists(nameIdMapHolderPath)) {
          nameIdMapHolder.close();

          Files.delete(nameIdMapHolderPath);
        }

        nameIdMapHolder = null;
        nameIdMapHolderPath = null;
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    stopFlush();

    final long[] fIds = new long[result.size()];
    int n = 0;
    for (final Long fid : result) {
      fIds[n] = fid;
      n++;
    }

    return fIds;
  }

  @Override
  public String fileNameById(final long fileId) {
    final int intId = extractFileId(fileId);

    return idNameMap.get(intId);
  }

  @Override
  public String nativeFileNameById(final long fileId) {
    final OFileClassic fileClassic = files.get(fileId);
    if (fileClassic != null) {
      return fileClassic.getName();
    }

    return null;
  }

  @Override
  public int getId() {
    return id;
  }

  public long getCacheOverflowCount() {
    return cacheOverflowCountSum.sum();
  }

  public long getWriteCacheSize() {
    return writeCacheSize.get();
  }

  public long getExclusiveWriteCacheSize() {
    return exclusiveWriteCacheSize.get();
  }

  private static void openFile(final OFileClassic fileClassic) {
    if (fileClassic.exists()) {
      if (!fileClassic.isOpen()) {
        fileClassic.open();
      }
    } else {
      throw new OStorageException("File " + fileClassic + " does not exist.");
    }

  }

  private static void createFile(final OFileClassic fileClassic, final boolean callFsync) throws IOException {
    if (!fileClassic.exists()) {
      fileClassic.create();

      if (callFsync) {
        fileClassic.synch();
      }
    } else {
      if (!fileClassic.isOpen()) {
        fileClassic.open();
      }
      fileClassic.shrink(0);
      if (callFsync) {
        fileClassic.synch();
      }
    }
  }

  private void initNameIdMapping() throws IOException, InterruptedException {
    if (nameIdMapHolder == null) {
      if (!Files.exists(storagePath)) {
        Files.createDirectories(storagePath);
      }

      final Path nameIdMapHolderV1 = storagePath.resolve(NAME_ID_MAP_V1);
      final Path nameIdMapHolderV2 = storagePath.resolve(NAME_ID_MAP_V2);

      if (Files.exists(nameIdMapHolderV1)) {
        if (Files.exists(nameIdMapHolderV2)) {
          Files.delete(nameIdMapHolderV2);
        }

        nameIdMapHolderPath = nameIdMapHolderV1;
        nameIdMapHolder = FileChannel.open(nameIdMapHolderPath, StandardOpenOption.READ);

        readNameIdMapV1();
        convertNameIdMapFromV1ToV2();

        nameIdMapHolder.close();

        nameIdMapHolderPath = storagePath.resolve(NAME_ID_MAP_V2);

        Files.delete(nameIdMapHolderV1);

        nameIdMapHolder = FileChannel.open(nameIdMapHolderPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      } else {
        nameIdMapHolderPath = storagePath.resolve(NAME_ID_MAP_V2);
        nameIdMapHolder = FileChannel
            .open(nameIdMapHolderPath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);

        readNameIdMapV2();
      }
    }
  }

  private void convertNameIdMapFromV1ToV2() throws IOException {
    final Path nameIdMapHolderFileV2T = storagePath.resolve(NAME_ID_MAP_V2_T);

    if (Files.exists(nameIdMapHolderFileV2T)) {
      Files.delete(nameIdMapHolderFileV2T);
    }

    final FileChannel v2NameIdMapHolder = FileChannel
        .open(nameIdMapHolderFileV2T, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0) {
        final OFileClassic fileClassic = files.get(externalFileId(nameIdEntry.getValue()));
        final String fileSystemName = fileClassic.getName();

        final NameFileIdEntry nameFileIdEntry = new NameFileIdEntry(nameIdEntry.getKey(), nameIdEntry.getValue(), fileSystemName);
        writeNameIdEntry(v2NameIdMapHolder, nameFileIdEntry, false);
      } else {
        final NameFileIdEntry nameFileIdEntry = new NameFileIdEntry(nameIdEntry.getKey(), nameIdEntry.getValue(), "");
        writeNameIdEntry(v2NameIdMapHolder, nameFileIdEntry, false);
      }
    }

    v2NameIdMapHolder.force(true);
    v2NameIdMapHolder.close();

    Files.move(nameIdMapHolderFileV2T, storagePath.resolve(NAME_ID_MAP_V2));
  }

  private OFileClassic createFileInstance(final String fileName, final int fileId) {
    final String internalFileName = createInternalFileName(fileName, fileId);
    return new OFileClassic(storagePath.resolve(internalFileName));
  }

  private static String createInternalFileName(final String fileName, final int fileId) {
    final int extSeparator = fileName.lastIndexOf('.');

    String prefix;
    if (extSeparator < 0) {
      prefix = fileName;
    } else if (extSeparator == 0) {
      prefix = "";
    } else {
      prefix = fileName.substring(0, extSeparator);
    }

    final String suffix;
    if (extSeparator < 0 || extSeparator == fileName.length() - 1) {
      suffix = "";
    } else {
      suffix = fileName.substring(extSeparator + 1);
    }

    prefix = prefix + "_" + fileId;

    if (extSeparator >= 0) {
      return prefix + "." + suffix;
    }

    return prefix;
  }

  /**
   * Read information about files are registered inside of write cache/storage
   * File consist of rows of variable length which contains following entries: <ol> <li>Internal file id, may be positive or
   * negative depends on whether file is removed or not</li> <li>Name of file inside of write cache, this name is case
   * sensitive</li> <li>Name of file which is used inside file system it can be different from name of file used inside write
   * cache</li> </ol>
   */
  private void readNameIdMapV2() throws IOException, InterruptedException {
    nameIdMap.clear();

    long localFileCounter = -1;

    nameIdMapHolder.position(0);

    NameFileIdEntry nameFileIdEntry;

    final Map<Integer, String> idFileNameMap = new HashMap<>(1_000);

    while ((nameFileIdEntry = readNextNameIdEntryV2()) != null) {
      final long absFileId = Math.abs(nameFileIdEntry.fileId);

      if (localFileCounter < absFileId) {
        localFileCounter = absFileId;
      }

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
      idNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.name);

      idFileNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.fileSystemName);
    }

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      final int fileId = nameIdEntry.getValue();

      if (fileId >= 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          final Path path = storagePath.resolve(idFileNameMap.get((nameIdEntry.getValue())));
          final OFileClassic fileClassic = new OFileClassic(path);

          if (fileClassic.exists()) {
            fileClassic.open();
            files.add(externalId, fileClassic);
          } else {
            idNameMap.remove(fileId);

            nameIdMap.put(nameIdEntry.getKey(), -fileId);
            idNameMap.put(-fileId, nameIdEntry.getKey());
          }
        }
      }
    }
  }

  private void readNameIdMapV1() throws IOException, InterruptedException {
    //older versions of ODB incorrectly logged file deletions
    //some deleted files have the same id
    //because we reuse ids of removed files when we re-create them
    //we need to fix this situation
    final Map<Integer, Set<String>> filesWithNegativeIds = new HashMap<>(1_000);

    nameIdMap.clear();

    long localFileCounter = -1;

    nameIdMapHolder.position(0);

    NameFileIdEntry nameFileIdEntry;
    while ((nameFileIdEntry = readNextNameIdEntryV1()) != null) {

      final long absFileId = Math.abs(nameFileIdEntry.fileId);
      if (localFileCounter < absFileId) {
        localFileCounter = absFileId;
      }

      final Integer existingId = nameIdMap.get(nameFileIdEntry.name);

      if (existingId != null && existingId < 0) {
        final Set<String> files = filesWithNegativeIds.get(existingId);

        if (files != null) {
          files.remove(nameFileIdEntry.name);

          if (files.isEmpty()) {
            filesWithNegativeIds.remove(existingId);
          }
        }
      }

      if (nameFileIdEntry.fileId < 0) {
        Set<String> files = filesWithNegativeIds.get(nameFileIdEntry.fileId);

        if (files == null) {
          files = new HashSet<>(8);
          files.add(nameFileIdEntry.name);
          filesWithNegativeIds.put(nameFileIdEntry.fileId, files);
        } else {
          files.add(nameFileIdEntry.name);
        }
      }

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
      idNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.name);
    }

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          final OFileClassic fileClassic = new OFileClassic(storagePath.resolve(nameIdEntry.getKey()));

          if (fileClassic.exists()) {
            fileClassic.open();
            files.add(externalId, fileClassic);
          } else {
            final Integer fileId = nameIdMap.get(nameIdEntry.getKey());

            if (fileId != null && fileId > 0) {
              nameIdMap.put(nameIdEntry.getKey(), -fileId);

              idNameMap.remove(fileId);
              idNameMap.put(-fileId, nameIdEntry.getKey());
            }
          }
        }
      }
    }

    final Set<String> fixedFiles = new HashSet<>(8);

    for (final Map.Entry<Integer, Set<String>> entry : filesWithNegativeIds.entrySet()) {
      final Set<String> files = entry.getValue();

      if (files.size() > 1) {
        idNameMap.remove(entry.getKey());

        for (final String fileName : files) {
          int fileId;

          while (true) {
            final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
            if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
              fileId = nextId;
              break;
            }
          }

          nameIdMap.put(fileName, -fileId);
          idNameMap.put(-fileId, fileName);

          fixedFiles.add(fileName);
        }
      }
    }

    if (!fixedFiles.isEmpty()) {
      OLogManager.instance().warn(this, "Removed files " + fixedFiles + " had duplicated ids. Problem is fixed automatically.");
    }
  }

  private NameFileIdEntry readNextNameIdEntryV1() throws IOException {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final int nameSize = buffer.getInt();
      buffer = ByteBuffer.allocate(nameSize + OLongSerializer.LONG_SIZE);

      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final String name = stringSerializer.deserializeFromByteBufferObject(buffer);
      final int fileId = (int) buffer.getLong();

      return new NameFileIdEntry(name, fileId);
    } catch (final EOFException ignore) {
      return null;
    }
  }

  private NameFileIdEntry readNextNameIdEntryV2() throws IOException {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(2 * OIntegerSerializer.INT_SIZE);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final int fileId = buffer.getInt();
      final int nameSize = buffer.getInt();

      buffer = ByteBuffer.allocate(nameSize);

      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final String name = stringSerializer.deserializeFromByteBufferObject(buffer);

      buffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final int fileNameSize = buffer.getInt();

      buffer = ByteBuffer.allocate(fileNameSize);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final String fileName = stringSerializer.deserializeFromByteBufferObject(buffer);

      return new NameFileIdEntry(name, fileId, fileName);

    } catch (final EOFException ignore) {
      return null;
    }
  }

  private void writeNameIdEntry(final NameFileIdEntry nameFileIdEntry, final boolean sync) throws IOException {
    writeNameIdEntry(nameIdMapHolder, nameFileIdEntry, sync);
  }

  private void writeNameIdEntry(final FileChannel nameIdMapHolder, final NameFileIdEntry nameFileIdEntry, final boolean sync)
      throws IOException {
    final int nameSize = stringSerializer.getObjectSize(nameFileIdEntry.name);
    final int fileNameSize = stringSerializer.getObjectSize(nameFileIdEntry.fileSystemName);

    //file id size + file name size + file name + file system name size + file system name
    final ByteBuffer serializedRecord = ByteBuffer.allocate(3 * OIntegerSerializer.INT_SIZE + nameSize + fileNameSize);

    //serialize file id
    OIntegerSerializer.INSTANCE.serializeInByteBufferObject(nameFileIdEntry.fileId, serializedRecord, 0);

    //serialize file name
    OIntegerSerializer.INSTANCE.serializeInByteBufferObject(nameSize, serializedRecord, OIntegerSerializer.INT_SIZE);
    stringSerializer.serializeInByteBufferObject(nameFileIdEntry.name, serializedRecord, 2 * OIntegerSerializer.INT_SIZE);

    //serialize file system name
    OIntegerSerializer.INSTANCE
        .serializeInByteBufferObject(fileNameSize, serializedRecord, 2 * OIntegerSerializer.INT_SIZE + nameSize);
    stringSerializer
        .serializeInByteBufferObject(nameFileIdEntry.fileSystemName, serializedRecord, 3 * OIntegerSerializer.INT_SIZE + nameSize);

    serializedRecord.position(0);

    OIOUtils.writeByteBuffer(serializedRecord, nameIdMapHolder, nameIdMapHolder.size());
    nameIdMapHolder.write(serializedRecord);

    if (sync) {
      nameIdMapHolder.force(true);
    }
  }

  private String doDeleteFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    removeCachedPages(intId);

    final OFileClassic fileClassic = files.remove(fileId);

    String name = null;
    if (fileClassic != null) {
      name = fileClassic.getName();

      if (fileClassic.exists()) {
        fileClassic.delete();
      }
    }

    return name;
  }

  private void removeCachedPages(final int fileId) {
    final Future<Void> future = commitExecutor.submit(new RemoveFilePagesTask(fileId));
    try {
      future.get();
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("File data removal was interrupted"), e);
    } catch (final Exception e) {
      throw OException.wrapException(new OWriteCacheException("File data removal was abnormally terminated"), e);
    }
  }

  private OCachePointer[] loadFileContent(final int intId, final long startPageIndex, final int pageCount,
      final boolean verifyChecksums) throws IOException {
    final long fileId = composeFileId(id, intId);

    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        final OFileClassic fileClassic = entry.get();
        if (fileClassic == null) {
          throw new IllegalArgumentException("File with id " + intId + " not found in WOW Cache");
        }

        final long firstPageStartPosition = startPageIndex * pageSize;
        final long firstPageEndPosition = firstPageStartPosition + pageSize;

        if (fileClassic.getFileSize() >= firstPageEndPosition) {
          long startTs = 0;
          int pagesRead = 0;

          if (printCacheStatistics) {
            startTs = System.nanoTime();
          }

          try {
            if (pageCount == 1) {
              final OPointer pointer = bufferPool.acquireDirect(false);
              final ByteBuffer buffer = pointer.getNativeByteBuffer();
              assert buffer.position() == 0;
              fileClassic.read(firstPageStartPosition, buffer, false);

              if (verifyChecksums && (checksumMode == OChecksumMode.StoreAndVerify || checksumMode == OChecksumMode.StoreAndThrow
                  || checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode)) {
                verifyMagicAndChecksum(buffer, pointer, fileId, startPageIndex, null);
              }

              buffer.position(0);

              final OCachePointer dataPointer = new OCachePointer(pointer, bufferPool, fileId, startPageIndex);
              pagesRead = 1;
              return new OCachePointer[] { dataPointer };
            }

            final long maxPageCount = (fileClassic.getFileSize() - firstPageStartPosition) / pageSize;
            final int realPageCount = Math.min((int) maxPageCount, pageCount);

            final OPointer[] pointers = new OPointer[realPageCount];
            final ByteBuffer[] buffers = new ByteBuffer[realPageCount];
            for (int i = 0; i < pointers.length; i++) {
              final OPointer pointer = bufferPool.acquireDirect(false);
              pointers[i] = pointer;
              buffers[i] = pointer.getNativeByteBuffer();
            }

            fileClassic.read(firstPageStartPosition, buffers, false);

            if (verifyChecksums && (checksumMode == OChecksumMode.StoreAndVerify || checksumMode == OChecksumMode.StoreAndThrow
                || checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode)) {
              for (int i = 0; i < pointers.length; ++i) {
                verifyMagicAndChecksum(buffers[i], pointers[i], fileId, startPageIndex + i, pointers);
              }
            }

            final OCachePointer[] dataPointers = new OCachePointer[pointers.length];
            for (int n = 0; n < pointers.length; n++) {
              buffers[n].position(0);
              dataPointers[n] = new OCachePointer(pointers[n], bufferPool, fileId, startPageIndex + n);
            }
            pagesRead = dataPointers.length;

            return dataPointers;
          } finally {
            if (printCacheStatistics) {
              final long endTs = System.nanoTime();
              final long loadTime = endTs - startTs;

              loadedPagesSum.add(pagesRead);
              loadedPagesTimeSum.add(loadTime);
            }
          }

        } else {
          return null;
        }
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Data load was interrupted"), e);
    }
  }

  private void addMagicAndChecksum(final ByteBuffer buffer) {
    assert buffer.order() == ByteOrder.nativeOrder();

    buffer.position(MAGIC_NUMBER_OFFSET);
    OLongSerializer.INSTANCE
        .serializeInByteBufferObject(checksumMode == OChecksumMode.Off ? MAGIC_NUMBER_WITHOUT_CHECKSUM : MAGIC_NUMBER_WITH_CHECKSUM,
            buffer);

    if (checksumMode != OChecksumMode.Off) {
      buffer.position(PAGE_OFFSET_TO_CHECKSUM_FROM);
      final CRC32 crc32 = new CRC32();
      crc32.update(buffer);
      final int computedChecksum = (int) crc32.getValue();

      buffer.position(CHECKSUM_OFFSET);
      OIntegerSerializer.INSTANCE.serializeInByteBufferObject(computedChecksum, buffer);
    }
  }

  private void verifyMagicAndChecksum(final ByteBuffer buffer, final OPointer pointer, final long fileId, final long pageIndex,
      final OPointer[] pointersToRelease) {
    assert buffer.order() == ByteOrder.nativeOrder();

    buffer.position(MAGIC_NUMBER_OFFSET);
    final long magicNumber = OLongSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    if (magicNumber != MAGIC_NUMBER_WITH_CHECKSUM) {
      if (magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
        final String message = "Magic number verification failed for page `" + pageIndex + "` of `" + fileNameById(fileId) + "`.";
        OLogManager.instance().error(this, "%s", null, message);

        if (checksumMode == OChecksumMode.StoreAndThrow) {
          if (pointersToRelease == null) {
            bufferPool.release(pointer);
          } else {
            for (final OPointer bufferToRelease : pointersToRelease) {
              bufferPool.release(bufferToRelease);
            }
          }

          throw new OStorageException(message);
        } else if (checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode) {
          dumpStackTrace(message);
          callPageIsBrokenListeners(fileNameById(fileId), pageIndex);
        }
      }

      return;
    }

    buffer.position(CHECKSUM_OFFSET);
    final int storedChecksum = OIntegerSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    buffer.position(PAGE_OFFSET_TO_CHECKSUM_FROM);
    final CRC32 crc32 = new CRC32();
    crc32.update(buffer);
    final int computedChecksum = (int) crc32.getValue();

    if (computedChecksum != storedChecksum) {
      final String message = "Checksum verification failed for page `" + pageIndex + "` of `" + fileNameById(fileId) + "`.";
      OLogManager.instance().error(this, "%s", null, message);
      if (checksumMode == OChecksumMode.StoreAndThrow) {
        if (pointersToRelease == null) {
          bufferPool.release(pointer);
        } else {
          for (final OPointer pointerToRelease : pointersToRelease) {
            bufferPool.release(pointerToRelease);
          }
        }

        throw new OStorageException(message);
      } else if (checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode) {
        dumpStackTrace(message);

        callPageIsBrokenListeners(fileNameById(fileId), pageIndex);
      }
    }
  }

  private void dumpStackTrace(final String message) {
    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);

    printWriter.println(message);
    final Exception exception = new Exception();
    exception.printStackTrace(printWriter);
    printWriter.flush();

    OLogManager.instance().error(this, stringWriter.toString(), null);
  }

  private void flushPage(final int fileId, final long pageIndex, final ByteBuffer buffer, final OLogSequenceNumber endLSN)
      throws InterruptedException, IOException {
    long startTs = 0;
    if (printCacheStatistics) {
      startTs = System.nanoTime();
    }

    boolean waitedForWALFlush = false;

    if (endLSN != null && writeAheadLog != null) {
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

      while (flushedLSN == null || flushedLSN.compareTo(endLSN) < 0) {
        writeAheadLog.flush();
        flushedLSN = writeAheadLog.getFlushedLsn();
        waitedForWALFlush = true;
      }
    }

    long walTs = startTs;
    if (printCacheStatistics && waitedForWALFlush) {
      walTs = System.nanoTime();
    }

    long flushTs = 0;
    if (printCacheStatistics) {
      flushTs = System.nanoTime();
    }

    final long externalId = composeFileId(id, fileId);
    final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalId);
    try {
      final OFileClassic fileClassic = entry.get();

      addMagicAndChecksum(buffer);
      buffer.position(0);
      fileClassic.write(pageIndex * pageSize, buffer);
    } finally {
      files.release(entry);
    }

    if (printCacheStatistics) {
      flushedPagesSum++;

      final long endTs = System.nanoTime();
      flushedPagesTime += endTs - flushTs;

      if (waitedForWALFlush) {
        walFlushTime += walTs - startTs;
        walFlushCount++;
      }
    }
  }

  private void printReport() {
    if (statisticTs == -1) {
      statisticTs = System.nanoTime();
    } else {
      final long ts = System.nanoTime();
      if (ts - statisticTs > statisticsPrintInterval * 1_000_000_000L) {

        if (lsnFlushIntervalCount == 0) {
          lsnFlushIntervalCount = 1;
        }

        if (flushedPagesTime == 0) {
          flushedPagesTime = 1;
        }

        if (chunkSizeCountSum == 0) {
          chunkSizeCountSum = 1;
        }

        final Map.Entry<Long, TreeSet<PageKey>> entry = localDirtyPagesBySegment.firstEntry();

        final long loadedPages = this.loadedPagesSum.sum();
        long loadedPagesTime = this.loadedPagesTimeSum.sum();

        if (loadedPagesTime == 0) {
          loadedPagesTime = 1;
        }

        final long cacheOverflowTime = cacheOverflowTimeSum.sum();
        final long cacheOverflowCount = cacheOverflowCountSum.sum();

        final OLogSequenceNumber walBegin;
        final OLogSequenceNumber walEnd;

        if (writeAheadLog != null) {
          walBegin = writeAheadLog.begin();
          walEnd = writeAheadLog.end();
        } else {
          walBegin = null;
          walEnd = null;
        }

        OLogManager.instance().infoNoDb(this,
            "Write cache stat:%s:Amount of flushed lsn pages %d, amount of flushed of exclusive pages %d, avg. "
                + " first dirty pages segment index %d, first dirty pages segment size %d,"
                + " avg. LSN flush interval %d, total amount of flushed pages %d, write speed %d page/s, "
                + "write speed is %d KB/s, %d times cache was waiting for WAL flush, avg %d ms. cache was waiting for WAL flush, "
                + "%d pages were read from the disk, read speed is %d pages/s (%d KB/s), "
                + "data threads were waiting because of cache overflow %d times, avg. wait time is %d ms., "
                + "avg. chunk size %d, avg, chunk flush time %d ms., WAL begin %s, WAL end %s, %d percent of exclusive write cache is filled, "
                + "LSN flush interval boundary %d ms", storageName, lsnPagesSum, exclusivePagesSum,
            entry == null ? -1 : entry.getKey().intValue(), entry == null ? -1 : entry.getValue().size(),
            lsnFlushIntervalSum / lsnFlushIntervalCount / 1_000_000, flushedPagesSum,
            1_000_000_000L * flushedPagesSum / flushedPagesTime,
            1_000_000_000L * flushedPagesSum / flushedPagesTime * pageSize / 1024, walFlushCount,
            walFlushCount > 0 ? walFlushTime / walFlushCount / 1_000_000 : 0, loadedPages,
            1_000_000_000L * loadedPages / loadedPagesTime, 1_000_000_000L * loadedPages / loadedPagesTime * pageSize / 1024,
            cacheOverflowCount, cacheOverflowCount > 0 ? cacheOverflowTime / cacheOverflowCount / 1_000_000 : 0,
            chunkSizeSum / chunkSizeCountSum, chunkSizeTimeSum / chunkSizeCountSum / 1_000_000, walBegin, walEnd,
            100 * exclusiveWriteCacheSize.get() / exclusiveWriteCacheMaxSize, lsnFlushIntervalBoundary / 1_000_000);

        statisticTs = ts;

        lsnPagesSum = 0;
        exclusivePagesSum = 0;

        lsnFlushIntervalCount = 0;
        lsnFlushIntervalSum = 0;

        flushedPagesSum = 0;
        flushedPagesTime = 0;

        walFlushCount = 0;
        walFlushTime = 0;

        loadedPagesSum.add(-loadedPages);
        loadedPagesTimeSum.add(-loadedPagesTime);

        cacheOverflowTimeSum.add(-cacheOverflowTime);
        cacheOverflowCountSum.add(-cacheOverflowCount);

        chunkSizeSum = 0;
        chunkSizeTimeSum = 0;
        chunkSizeCountSum = 0;
      }

    }

  }

  public void setChecksumMode(final OChecksumMode checksumMode) { // for testing purposes only
    this.checksumMode = checksumMode;
  }

  private static final class NameFileIdEntry {
    private final String name;
    private final int    fileId;
    private final String fileSystemName;

    private NameFileIdEntry(final String name, final int fileId) {
      this.name = name;
      this.fileId = fileId;
      this.fileSystemName = name;
    }

    private NameFileIdEntry(final String name, final int fileId, final String fileSystemName) {
      this.name = name;
      this.fileId = fileId;
      this.fileSystemName = fileSystemName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final NameFileIdEntry that = (NameFileIdEntry) o;

      if (fileId != that.fileId) {
        return false;
      }
      if (!name.equals(that.name)) {
        return false;
      }
      return fileSystemName.equals(that.fileSystemName);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + fileId;
      result = 31 * result + fileSystemName.hashCode();
      return result;
    }
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final int  fileId;
    private final long pageIndex;

    private PageKey(final int fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int compareTo(final PageKey other) {
      if (fileId > other.fileId) {
        return 1;
      }
      if (fileId < other.fileId) {
        return -1;
      }

      return Long.compare(pageIndex, other.pageIndex);

    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId) {
        return false;
      }
      return pageIndex == pageKey.pageIndex;
    }

    @Override
    public int hashCode() {
      int result = fileId;
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "PageKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }

    public PageKey previous() {
      return pageIndex == -1 ? this : new PageKey(fileId, pageIndex - 1);
    }
  }

  private final class FlushTillSegmentTask implements Callable<Void> {
    private final long segmentId;

    private FlushTillSegmentTask(final long segmentId) {
      this.segmentId = segmentId;
    }

    @Override
    public Void call() throws Exception {
      if (flushError != null) {
        OLogManager.instance()
            .errorNoDb(this, "Can not flush data till provided segment because of issue during data write, %s", null,
                flushError.getMessage());
        return null;
      }

      if (writeAheadLog == null) {
        return null;
      }

      convertSharedDirtyPagesToLocal();
      Map.Entry<Long, TreeSet<PageKey>> firstEntry = localDirtyPagesBySegment.firstEntry();

      if (firstEntry == null) {
        return null;
      }

      long minDirtySegment = firstEntry.getKey();
      while (minDirtySegment < segmentId) {
        flushExclusivePagesIfNeeded();

        flushWriteCacheFromMinLSN(writeAheadLog.begin().getSegment(), segmentId, chunkSize);

        firstEntry = localDirtyPagesBySegment.firstEntry();

        if (firstEntry == null) {
          return null;
        }

        minDirtySegment = firstEntry.getKey();
      }

      return null;
    }

    private void flushExclusivePagesIfNeeded() throws InterruptedException, IOException {
      final long ewcSize = exclusiveWriteCacheSize.get();
      assert ewcSize >= 0;

      if (ewcSize >= 0.8 * exclusiveWriteCacheMaxSize) {
        flushExclusiveWriteCache(null, ewcSize);
      }
    }
  }

  private final class ExclusiveFlushTask implements Runnable {
    private final CountDownLatch cacheBoundaryLatch;
    private final CountDownLatch completionLatch;

    private ExclusiveFlushTask(final CountDownLatch cacheBoundaryLatch, final CountDownLatch completionLatch) {
      this.cacheBoundaryLatch = cacheBoundaryLatch;
      this.completionLatch = completionLatch;
    }

    @Override
    public void run() {
      if (stopFlush) {
        return;
      }

      try {
        if (flushError != null) {
          OLogManager.instance()
              .errorNoDb(this, "Can not flush data because of issue during data write, %s", null, flushError.getMessage());
          return;
        }

        if (writeCachePages.isEmpty()) {
          return;
        }

        final long ewcSize = exclusiveWriteCacheSize.get();

        assert ewcSize >= 0;

        if (cacheBoundaryLatch != null && ewcSize <= exclusiveWriteCacheMaxSize) {
          cacheBoundaryLatch.countDown();
        }

        if (ewcSize > exclusiveWriteCacheMaxSize) {
          flushExclusiveWriteCache(cacheBoundaryLatch, chunkSize);
        }

      } catch (final Error | Exception t) {
        OLogManager.instance().error(this, "Exception during data flush", t);
        OWOWCache.this.fireBackgroundDataFlushExceptionEvent(t);
        flushError = t;
      } finally {
        if (cacheBoundaryLatch != null) {
          cacheBoundaryLatch.countDown();
        }

        if (completionLatch != null) {
          completionLatch.countDown();
        }
      }

    }
  }

  private final class PeriodicFlushTask implements Runnable {

    @Override
    public void run() {
      if (stopFlush) {
        return;
      }

      try {
        if (printCacheStatistics) {
          printReport();
        }

        if (flushError != null) {
          OLogManager.instance()
              .errorNoDb(this, "Can not flush data because of issue during data write, %s", null, flushError.getMessage());
          return;
        }

        try {
          if (writeCachePages.isEmpty()) {
            return;
          }

          final long startTs = System.nanoTime();
          long ewcSize = exclusiveWriteCacheSize.get();

          int exclusivePages = 0;
          if (ewcSize >= 0.8 * exclusiveWriteCacheMaxSize) {
            exclusivePages = flushExclusiveWriteCache(null, ewcSize);
          }

          final long lsnFlushInterval;
          if (lastTsLSNFlush == -1) {
            lastTsLSNFlush = startTs;
            lsnFlushInterval = 0;
          } else {
            lsnFlushInterval = startTs - lastTsLSNFlush;
          }

          int lsnPages = 0;
          if (writeAheadLog != null) {
            convertSharedDirtyPagesToLocal();

            final long startSegment = writeAheadLog.begin().getSegment();
            final long endSegment = writeAheadLog.end().getSegment();
            final int segmentCount = localDirtyPagesBySegment.size();

            if (lsnFlushInterval >= lsnFlushIntervalBoundary || segmentCount > lastSegmentCount) {
              lastTsLSNFlush = startTs;

              final Map.Entry<Long, TreeSet<PageKey>> lsnEntry = localDirtyPagesBySegment.firstEntry();

              if (lsnEntry != null && segmentCount > 0 && lsnEntry.getKey() < endSegment) {
                final long lsnTs = System.nanoTime();

                lsnPages = flushWriteCacheFromMinLSN(startSegment, endSegment, 8 * chunkSize);

                lsnFlushIntervalSum += lsnFlushInterval;
                lsnFlushIntervalCount++;

                if (lsnPages > 0) {
                  final long endTs = System.nanoTime();

                  final int segments = localDirtyPagesBySegment.size();
                  final long flushInterval = endTs - lsnTs;

                  if (lsnPagesFlushIntervalCount == 10) {
                    @SuppressWarnings("UnnecessaryLocalVariable")
                    final long avgFlushInterval = lsnPagesFlushIntervalSum / lsnPagesFlushIntervalCount;

                    lsnPagesFlushIntervalSum = avgFlushInterval;
                    lsnPagesFlushIntervalCount = 1;
                  }

                  lsnPagesFlushIntervalSum += flushInterval;
                  lsnPagesFlushIntervalCount++;

                  final long avgFlushInterval = lsnPagesFlushIntervalSum / lsnPagesFlushIntervalCount;

                  if (segments <= 2) {
                    lsnFlushIntervalBoundary = 8 * avgFlushInterval;
                  } else if (segments <= 3) {
                    lsnFlushIntervalBoundary = 4 * avgFlushInterval;
                  } else if (segments <= 4) {
                    lsnFlushIntervalBoundary = 2 * avgFlushInterval;
                  } else if (segments <= 5) {
                    lsnFlushIntervalBoundary = avgFlushInterval;
                  } else if (segments <= 6) {
                    lsnFlushIntervalBoundary = avgFlushInterval / 2;
                  } else if (segments <= 7) {
                    lsnFlushIntervalBoundary = avgFlushInterval / 4;
                  } else if (segments <= 8) {
                    lsnFlushIntervalBoundary = avgFlushInterval / 8;
                  } else {
                    lsnFlushIntervalBoundary = 0;
                  }
                }
              }
            }

            lastSegmentCount = segmentCount;
          }

          if (lsnPages + exclusivePages == 0) {
            final long backgroundExclusiveInterval;

            if (lastFlushTs == -1) {
              backgroundExclusiveInterval = 0;
            } else {
              backgroundExclusiveInterval = startTs - lastFlushTs;
            }

            ewcSize = exclusiveWriteCacheSize.get();

            if (ewcSize >= chunkSize && backgroundExclusiveInterval >= backgroundExclusiveFlushBoundary) {
              final long backgroundTs = System.nanoTime();

              final int backgroundPages = flushExclusiveWriteCache(null, (long) (0.1 * exclusiveWriteCacheMaxSize));

              if (backgroundPages > 0) {
                final long endTs = System.nanoTime();
                backgroundExclusiveFlushBoundary = 9 * (endTs - backgroundTs);
              }

              lastFlushTs = startTs;
            }
          } else {
            lastFlushTs = startTs;
          }
        } catch (final Error | Exception t) {
          OLogManager.instance().error(this, "Exception during data flush", t);
          OWOWCache.this.fireBackgroundDataFlushExceptionEvent(t);
          flushError = t;
        }
      } finally {
        if (pagesFlushInterval > 0 && !stopFlush) {
          flushFuture = commitExecutor.schedule(this, pagesFlushInterval, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  final class FindMinDirtySegment implements Callable<Long> {
    @Override
    public Long call() {
      if (flushError != null) {
        OLogManager.instance()
            .errorNoDb(this, "Can not calculate minimum LSN because of issue during data write, %s", null, flushError.getMessage());
        return null;
      }

      convertSharedDirtyPagesToLocal();

      if (localDirtyPagesBySegment.isEmpty()) {
        return null;
      }

      return localDirtyPagesBySegment.firstKey();
    }
  }

  private void convertSharedDirtyPagesToLocal() {
    dirtyPagesLock.acquireWriteLock();
    try {
      for (final Map.Entry<PageKey, OLogSequenceNumber> entry : dirtyPages.entrySet()) {
        final OLogSequenceNumber localLSN = localDirtyPages.get(entry.getKey());

        if (localLSN == null || localLSN.compareTo(entry.getValue()) > 0) {
          localDirtyPages.put(entry.getKey(), entry.getValue());

          final long segment = entry.getValue().getSegment();
          TreeSet<PageKey> pages = localDirtyPagesBySegment.get(segment);
          if (pages == null) {
            pages = new TreeSet<>();
            pages.add(entry.getKey());

            localDirtyPagesBySegment.put(segment, pages);
          } else {
            pages.add(entry.getKey());
          }
        }
      }

      dirtyPages.clear();
    } finally {
      dirtyPagesLock.releaseWriteLock();
    }
  }

  private void removeFromDirtyPages(final PageKey pageKey) {
    dirtyPages.remove(pageKey);

    final OLogSequenceNumber lsn = localDirtyPages.remove(pageKey);
    if (lsn != null) {
      final long segment = lsn.getSegment();
      final TreeSet<PageKey> pages = localDirtyPagesBySegment.get(segment);
      assert pages != null;

      final boolean removed = pages.remove(pageKey);
      if (pages.isEmpty()) {
        localDirtyPagesBySegment.remove(segment);
      }

      assert removed;
    }
  }

  private int flushWriteCacheFromMinLSN(final long segStart, final long segEnd, final int pagesFlushLimit)
      throws InterruptedException, IOException {
    //first we try to find page which contains the oldest not flushed changes
    //that is needed to allow to compact WAL as earlier as possible
    convertSharedDirtyPagesToLocal();

    int flushedPages = 0;
    int copiedPages = 0;

    final ArrayList<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk = new ArrayList<>(pagesFlushLimit);

    long currentSegment = segStart;

    flushCycle:
    while (flushedPages < pagesFlushLimit) {
      if (!chunk.isEmpty()) {
        throw new IllegalStateException("Chunk is not empty !");
      }

      final TreeSet<PageKey> segmentPages = localDirtyPagesBySegment.get(currentSegment);

      if (segmentPages == null) {
        currentSegment++;

        if (currentSegment >= segEnd) {
          break;
        }

        continue;
      }

      final Iterator<PageKey> lsnPagesIterator = segmentPages.iterator();

      final List<PageKey> pageKeysToFlush = new ArrayList<>(pagesFlushLimit);

      while (lsnPagesIterator.hasNext() && pageKeysToFlush.size() < pagesFlushLimit - flushedPages) {
        final PageKey pageKey = lsnPagesIterator.next();
        pageKeysToFlush.add(pageKey);
      }

      long lastPageIndex = -1;
      long lastFileId = -1;
      OLogSequenceNumber maxFullLogLSN = null;

      for (final PageKey pageKey : pageKeysToFlush) {
        if (lastFileId == -1) {
          if (!chunk.isEmpty()) {
            throw new IllegalStateException("Chunk is not empty !");
          }
        } else {
          if (lastPageIndex == -1) {
            throw new IllegalStateException("Last page index is -1");
          }

          if (lastFileId != pageKey.fileId || lastPageIndex != pageKey.pageIndex - 1) {
            if (!chunk.isEmpty()) {
              flushedPages += flushPagesChunk(chunk, maxFullLogLSN);
            }

            maxFullLogLSN = null;
          }
        }

        final OCachePointer pointer = writeCachePages.get(pageKey);

        if (pointer == null) {
          //we marked page as dirty but did not put it in cache yet
          if (!chunk.isEmpty()) {
            flushedPages += flushPagesChunk(chunk, maxFullLogLSN);
          }

          break flushCycle;
        }

        if (pointer.tryAcquireSharedLock()) {
          final long version;
          final OLogSequenceNumber fullLogLSN;

          final OPointer directPointer = bufferPool.acquireDirect(false);
          final ByteBuffer copy = directPointer.getNativeByteBuffer();
          try {
            version = pointer.getVersion();
            final ByteBuffer buffer = pointer.getBufferDuplicate();

            fullLogLSN = pointer.getEndLSN();

            assert buffer != null;
            buffer.position(0);
            copy.position(0);

            copy.put(buffer);

            removeFromDirtyPages(pageKey);

            copiedPages++;
          } finally {
            pointer.releaseSharedLock();
          }

          if (fullLogLSN != null && (maxFullLogLSN == null || fullLogLSN.compareTo(maxFullLogLSN) > 0)) {
            maxFullLogLSN = fullLogLSN;
          }

          copy.position(0);

          chunk.add(new OQuarto<>(version, copy, directPointer, pointer));

          if (chunk.size() >= pagesFlushLimit || chunk.size() >= chunkSize) {
            flushedPages += flushPagesChunk(chunk, maxFullLogLSN);
            maxFullLogLSN = null;

            lastPageIndex = -1;
            lastFileId = -1;
          } else {
            lastPageIndex = pageKey.pageIndex;
            lastFileId = pageKey.fileId;
          }
        } else {
          if (!chunk.isEmpty()) {
            flushedPages += flushPagesChunk(chunk, maxFullLogLSN);
          }

          maxFullLogLSN = null;

          lastPageIndex = -1;
          lastFileId = -1;
        }
      }

      if (!chunk.isEmpty()) {
        flushedPages += flushPagesChunk(chunk, maxFullLogLSN);
      }
    }

    if (!chunk.isEmpty()) {
      throw new IllegalStateException("Chunk is not empty !");
    }

    if (copiedPages != flushedPages) {
      throw new IllegalStateException("Copied pages (" + copiedPages + " ) != flushed pages (" + flushedPages + ")");
    }

    lsnPagesSum += flushedPages;
    return flushedPages;
  }

  private int flushPagesChunk(final ArrayList<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk,
      final OLogSequenceNumber fullLogLSN) throws InterruptedException, IOException {

    if (chunk.isEmpty()) {
      return 0;
    }

    long startTs = 0;
    if (printCacheStatistics) {
      startTs = System.nanoTime();
    }

    boolean wasWaitingForWAL = false;
    if (fullLogLSN != null) {
      if (writeAheadLog != null) {
        OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

        while (flushedLSN == null || flushedLSN.compareTo(fullLogLSN) < 0) {
          writeAheadLog.flush();
          flushedLSN = writeAheadLog.getFlushedLsn();
          wasWaitingForWAL = true;
        }
      }
    }

    long walTs = startTs;
    if (printCacheStatistics && wasWaitingForWAL) {
      walTs = System.nanoTime();
    }

    long flushTs = 0;
    if (printCacheStatistics) {
      flushTs = System.nanoTime();
    }

    final ByteBuffer[] buffers = new ByteBuffer[chunk.size()];
    final OPointer[] directPointers = new OPointer[chunk.size()];

    for (int i = 0; i < buffers.length; i++) {
      final OQuarto<Long, ByteBuffer, OPointer, OCachePointer> quarto = chunk.get(i);
      final ByteBuffer buffer = quarto.two;

      addMagicAndChecksum(buffer);

      buffer.position(0);
      buffers[i] = buffer;
      directPointers[i] = quarto.three;
    }

    final OQuarto<Long, ByteBuffer, OPointer, OCachePointer> firstChunk = chunk.get(0);

    final OCachePointer firstCachePointer = firstChunk.four;
    final long firstFileId = firstCachePointer.getFileId();
    final long firstPageIndex = firstCachePointer.getPageIndex();

    final OClosableEntry<Long, OFileClassic> fileEntry = files.acquire(firstFileId);
    try {
      final OFileClassic file = fileEntry.get();
      file.write(firstPageIndex * pageSize, buffers);
    } finally {
      files.release(fileEntry);
    }

    for (final OPointer directPointer : directPointers) {
      bufferPool.release(directPointer);
    }

    for (final OQuarto<Long, ByteBuffer, OPointer, OCachePointer> triple : chunk) {
      final OCachePointer pointer = triple.four;

      final PageKey pageKey = new PageKey(internalFileId(pointer.getFileId()), pointer.getPageIndex());
      final long version = triple.one;

      final Lock lock = lockManager.acquireExclusiveLock(pageKey);
      try {
        if (!pointer.tryAcquireSharedLock()) {
          continue;
        }

        try {
          if (version == pointer.getVersion()) {
            writeCachePages.remove(pageKey);
            writeCacheSize.decrementAndGet();

            pointer.decrementWritersReferrer();
            pointer.setWritersListener(null);
          }
        } finally {
          pointer.releaseSharedLock();
        }
      } finally {
        lock.unlock();
      }
    }

    final int flushedPages = chunk.size();
    chunk.clear();

    if (printCacheStatistics) {
      final long endTs = System.nanoTime();

      flushedPagesSum += flushedPages;
      flushedPagesTime += endTs - flushTs;

      chunkSizeSum += flushedPages;
      chunkSizeCountSum++;
      chunkSizeTimeSum += endTs - flushTs;

      if (wasWaitingForWAL) {
        walFlushCount++;
        walFlushTime += walTs - startTs;
      }
    }

    return flushedPages;
  }

  private int flushExclusiveWriteCache(final CountDownLatch latch, long pagesToFlush) throws InterruptedException, IOException {
    final Iterator<PageKey> iterator = exclusiveWritePages.iterator();

    int flushedPages = 0;
    int copiedPages = 0;

    final long ewcSize = exclusiveWriteCacheSize.get();
    pagesToFlush = Math.min(Math.max(pagesToFlush, chunkSize), ewcSize);

    final ArrayList<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk = new ArrayList<>(chunkSize);

    if (latch != null && ewcSize <= exclusiveWriteCacheMaxSize) {
      latch.countDown();
    }

    flushCycle:
    while (flushedPages < pagesToFlush) {
      long lastFileId = -1;
      long lastPageIndex = -1;

      OLogSequenceNumber maxFullLogLSN = null;

      while (chunk.size() < chunkSize && flushedPages + chunk.size() < pagesToFlush) {
        if (!iterator.hasNext()) {
          if (!chunk.isEmpty()) {
            flushedPages += flushPagesChunk(chunk, maxFullLogLSN);

            if (latch != null && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
              latch.countDown();
            }
          }

          break flushCycle;
        }

        final PageKey pageKey = iterator.next();

        final OCachePointer pointer = writeCachePages.get(pageKey);
        final long version;

        if (pointer == null) {
          iterator.remove();
        } else {
          if (pointer.tryAcquireSharedLock()) {
            final OLogSequenceNumber fullLSN;

            final OPointer directPointer = bufferPool.acquireDirect(false);
            final ByteBuffer copy = directPointer.getNativeByteBuffer();
            try {
              version = pointer.getVersion();
              final ByteBuffer buffer = pointer.getBufferDuplicate();

              fullLSN = pointer.getEndLSN();

              assert buffer != null;
              buffer.position(0);
              copy.position(0);

              copy.put(buffer);

              removeFromDirtyPages(pageKey);

              copiedPages++;
            } finally {
              pointer.releaseSharedLock();
            }

            if (fullLSN != null && (maxFullLogLSN == null || maxFullLogLSN.compareTo(fullLSN) < 0)) {
              maxFullLogLSN = fullLSN;
            }

            copy.position(0);

            if (chunk.isEmpty()) {
              chunk.add(new OQuarto<>(version, copy, directPointer, pointer));
            } else {
              if (lastFileId != pointer.getFileId() || lastPageIndex != pointer.getPageIndex() - 1) {
                flushedPages += flushPagesChunk(chunk, maxFullLogLSN);

                if (latch != null && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
                  latch.countDown();
                }

                maxFullLogLSN = null;

                chunk.add(new OQuarto<>(version, copy, directPointer, pointer));
              } else {
                chunk.add(new OQuarto<>(version, copy, directPointer, pointer));
              }
            }

            lastFileId = pointer.getFileId();
            lastPageIndex = pointer.getPageIndex();
          } else {
            if (!chunk.isEmpty()) {
              flushedPages += flushPagesChunk(chunk, maxFullLogLSN);

              if (latch != null && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
                latch.countDown();
              }
            }

            maxFullLogLSN = null;

            lastFileId = -1;
            lastPageIndex = -1;
          }
        }
      }

      if (!chunk.isEmpty()) {
        flushedPages += flushPagesChunk(chunk, maxFullLogLSN);

        if (latch != null && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
          latch.countDown();
        }
      }
    }

    if (!chunk.isEmpty()) {
      throw new IllegalStateException("Chunk is not empty !");
    }

    if (copiedPages != flushedPages) {
      throw new IllegalStateException("Copied pages (" + copiedPages + " ) != flushed pages (" + flushedPages + ")");
    }

    exclusivePagesSum += flushedPages;
    return flushedPages;
  }

  private final class FileFlushTask implements Callable<Void> {
    private final Set<Integer> fileIdSet;

    private FileFlushTask(final Collection<Integer> fileIds) {
      this.fileIdSet = new HashSet<>(fileIds);
    }

    @Override
    public Void call() throws Exception {
      if (flushError != null) {
        OLogManager.instance()
            .errorNoDb(this, "Can not flush file data because of issue during data write, %s", null, flushError.getMessage());
        return null;
      }

      final Iterator<Map.Entry<PageKey, OCachePointer>> entryIterator = writeCachePages.
          entrySet().iterator();

      while (entryIterator.hasNext()) {
        final Map.Entry<PageKey, OCachePointer> entry = entryIterator.next();
        final PageKey pageKey = entry.getKey();
        if (fileIdSet.contains(pageKey.fileId)) {
          final OCachePointer pagePointer = entry.getValue();
          final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
          try {
            if (!pagePointer.tryAcquireSharedLock()) {
              continue;
            }

            try {
              final ByteBuffer buffer = pagePointer.getBufferDuplicate();

              final OPointer directPointer = bufferPool.acquireDirect(false);
              try {
                final ByteBuffer copy = directPointer.getNativeByteBuffer();

                assert buffer != null;
                buffer.position(0);
                copy.put(buffer);

                final OLogSequenceNumber endLSN = pagePointer.getEndLSN();
                flushPage(pageKey.fileId, pageKey.pageIndex, copy, endLSN);
              } finally {
                bufferPool.release(directPointer);
              }

              removeFromDirtyPages(pageKey);
            } finally {
              pagePointer.releaseSharedLock();
            }

            pagePointer.decrementWritersReferrer();
            pagePointer.setWritersListener(null);

            entryIterator.remove();
          } finally {
            groupLock.unlock();
          }

          writeCacheSize.decrementAndGet();
        }
      }

      if (callFsync) {
        for (final int iFileId : fileIdSet) {
          final long finalId = composeFileId(id, iFileId);
          final OClosableEntry<Long, OFileClassic> entry = files.acquire(finalId);
          if (entry != null) {
            try {
              entry.get().synch();
            } finally {
              files.release(entry);
            }
          }
        }
      }

      return null;
    }
  }

  private final class RemoveFilePagesTask implements Callable<Void> {
    private final int fileId;

    private RemoveFilePagesTask(final int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() {
      final Iterator<Map.Entry<PageKey, OCachePointer>> entryIterator = writeCachePages.entrySet().iterator();
      while (entryIterator.hasNext()) {
        final Map.Entry<PageKey, OCachePointer> entry = entryIterator.next();
        final PageKey pageKey = entry.getKey();

        if (pageKey.fileId == fileId) {
          final OCachePointer pagePointer = entry.getValue();
          final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
          try {
            pagePointer.acquireExclusiveLock();
            try {
              pagePointer.decrementWritersReferrer();
              pagePointer.setWritersListener(null);
              writeCacheSize.decrementAndGet();

              removeFromDirtyPages(pageKey);
            } finally {
              pagePointer.releaseExclusiveLock();
            }

            entryIterator.remove();
          } finally {
            groupLock.unlock();
          }
        }
      }

      return null;
    }
  }

  private static final class FlushThreadFactory implements ThreadFactory {

    private FlushThreadFactory() {
    }

    @Override
    public final Thread newThread(final Runnable r) {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB Write Cache Flush Task");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    }
  }

  private static final class CacheEventsPublisherFactory implements ThreadFactory {

    private CacheEventsPublisherFactory() {
    }

    @Override
    public final Thread newThread(final Runnable r) {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);

      thread.setDaemon(true);
      thread.setName("OrientDB Write Cache Event Publisher");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());

      return thread;
    }
  }
}
