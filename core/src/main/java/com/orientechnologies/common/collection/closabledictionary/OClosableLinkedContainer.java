package com.orientechnologies.common.collection.closabledictionary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Container for the elements which may be in open/closed state. But only limited amount of elements
 * are hold by given container may be in open state.
 *
 * <p>Elements may be added in this container only in open state,as result after addition of some
 * elements other rarely used elements will be closed.
 *
 * <p>When you want to use elements from container you should acquire them {@link #acquire(Object)}.
 * So element still will be inside of container but in acquired state. As result it will not be
 * automatically closed when container will try to close rarely used items.
 *
 * <p>Container uses LRU eviction policy to choose item to be closed.
 *
 * <p>When you finish to work with element from container you should release (@link {@link
 * #release(OClosableEntry)}) element back to container.
 *
 * @param <K> Key associated with entry stored inside of container.
 * @param <V> Value which may be in open/closed stated and associated with key.
 */
public class OClosableLinkedContainer<K, V extends OClosableItem> {
  /**
   * Design of container consist of several major parts.
   *
   * <p>Operation buffers.
   *
   * <p>Operation buffers are needed to log all events inside of container which may cause changes
   * in content and order of LRU items. Following operations are logged: 1. Add. 2. Remove. 3.
   * Acquire.
   *
   * <p>Instead of logging all of these operations using single buffer (logger) we split logging by
   * several buffers. So only few threads are logging at any buffer at the same moment reducing
   * chance of contention.
   *
   * <p>There are two types of buffers : state buffer and read buffers. State buffer is used to log
   * operations which can not be lost such as add and remove. Read buffers are used to log
   * operations small part of which may be lost.
   *
   * <p>As result write buffer is implemented as concurrent linked queue and read buffers are
   * implemented as arrays with two types of counters. So in nutshell read buffer is array based
   * implementation of ring buffer. Counters have following meaning : write counter - next position
   * inside of array which is used to write in buffer entry which was accessed as result of acquire
   * operation, read counter - next position inside of array which is used to read data from buffer
   * during flushing of data. So this buffer is the implementation of Lamport queue algorithm not
   * taking into account that we may work with several producers. To decrease contention between
   * threads we do not perform CAS operations on write counter during operation logging. As result
   * threads may overwrite logs of each other which is acceptable because part of statistic may be
   * lost.
   *
   * <p>Content of all buffers is processed (flushed) when one of the read buffers is reached
   * threshold between position of write counter during last buffer flush (this position is stored
   * at "drain at write count" field associated with each buffer) and current position of write
   * counter. Buffers also flushed when we log any operation in write buffer.
   *
   * <p>There is no common lock between of operations with data and logging of those operations to
   * buffers so related records can be reordered during processing of buffers. For example we may
   * add item in t1 thread and remove item in t2 thread. But operations logged in buffers may be
   * processed in different order and record removed from cache may stay in LRU list forever. To
   * avoid given situation state machine was introduced.
   *
   * <p>Each entry has following states: open, closed, retired (removed from map but not from LRU
   * list), dead(completely removed), acquired.
   *
   * <p>Following state transitions are allowed:
   *
   * <p>open->(evict() method is called during buffer flush)->close closed->(acquire() method is
   * called)->acquired acquired->(release())->open
   *
   * <p>open->(remove())->retired closed->(remove())->retired acquired->(remove())->retired
   *
   * <p>It is seen from state flow that it is impossible to close item in "acquired" state. Also
   * during of processing of "add" operation, item will be added into LRU list only if it is in
   *
   * <p>LRU list is modified during flush of the buffers , to make those modifications safe flush of
   * the buffer is performed under exclusive lock. To avoid contention between threads any thread
   * which has been noticed that read buffer should be drained calls tryLock but not lock operation.
   */

  /** The number of CPUs */
  private static final int NCPU = Runtime.getRuntime().availableProcessors();

  /** The number of read buffers to use. */
  private static final int NUMBER_OF_READ_BUFFERS = closestPowerOfTwo(NCPU);

  /** Mask value for indexing into the read buffers. */
  private static final int READ_BUFFERS_MASK = NUMBER_OF_READ_BUFFERS - 1;

  /** The number of pending read operations before attempting to drain. */
  private static final int READ_BUFFER_THRESHOLD = 32;

  /** The maximum number of read operations to perform per amortized drain. */
  private static final int READ_BUFFER_DRAIN_THRESHOLD = 2 * READ_BUFFER_THRESHOLD;

  /** The maximum number of write operations to perform per amortized drain. */
  private static final int WRITE_BUFFER_DRAIN_THRESHOLD = 32;

  /** The maximum number of pending reads per buffer. */
  private static final int READ_BUFFER_SIZE = 2 * READ_BUFFER_DRAIN_THRESHOLD;

  /** Mask value for indexing into the read buffer. */
  private static final int READ_BUFFER_INDEX_MASK = READ_BUFFER_SIZE - 1;

  /** Last indexes of buffers on which buffer flush procedure was stopped */
  private final long[] readBufferReadCount = new long[NUMBER_OF_READ_BUFFERS];

  /**
   * Next indexes of buffers on which threads will write new entries during logging of acquire
   * operations
   */
  private final AtomicLong[] readBufferWriteCount;

  /** Values of {@link #readBufferWriteCount} on which buffers were flushed. */
  private final AtomicLong[] readBufferDrainAtWriteCount;

  /**
   * Read buffers are used to lot {@link #acquire(Object)} operations when item is not switched from
   * closed to open states. So in cases when amount of items inside of {@link #lruList} is not going
   * to be changed, but only information about recency of items should be modified.
   */
  private final AtomicReference<OClosableEntry<K, V>>[][] readBuffers;

  /**
   * Lock which wraps all buffer flush operations and as result protects changes of {@link
   * #lruList}.
   */
  private final Lock lruLock = new ReentrantLock();

  /** LRU list to updated statistic of recency of contained items. */
  private final OClosableLRUList<K, V> lruList = new OClosableLRUList<K, V>();

  /**
   * Main source of truth of container if value is absent in this field it is absent in container.
   */
  private final ConcurrentHashMap<K, OClosableEntry<K, V>> data =
      new ConcurrentHashMap<K, OClosableEntry<K, V>>();

  /**
   * Buffer which contains operation which includes changes of states from closed to open, and from
   * any state to retired. In other words this buffer contains information about operations which
   * affect amount of items inside of {@link #lruList} and those operations can not be lost.
   */
  private final ConcurrentLinkedQueue<Runnable> stateBuffer = new ConcurrentLinkedQueue<Runnable>();

  /** Maximum amount of open items inside of container. */
  private final int openLimit;

  /** Status which indicates whether flush of buffers should be performed or may be delayed. */
  private final AtomicReference<DrainStatus> drainStatus =
      new AtomicReference<DrainStatus>(DrainStatus.IDLE);

  /** Latch which prevents addition or open of new files if limit of open files is reached */
  private final AtomicReference<CountDownLatch> openLatch = new AtomicReference<CountDownLatch>();

  /** Amount of simultaneously open files in container */
  private final AtomicInteger openFiles = new AtomicInteger();

  /**
   * Creates new instance of container and set limit of open files which may be hold by container.
   *
   * @param openLimit Limit of open files hold by container.
   */
  public OClosableLinkedContainer(final int openLimit) {
    this.openLimit = openLimit;

    AtomicLong[] rbwc = new AtomicLong[NUMBER_OF_READ_BUFFERS];
    AtomicLong[] rbdawc = new AtomicLong[NUMBER_OF_READ_BUFFERS];
    AtomicReference<OClosableEntry<K, V>>[][] rbs = new AtomicReference[NUMBER_OF_READ_BUFFERS][];

    for (int i = 0; i < NUMBER_OF_READ_BUFFERS; i++) {
      rbwc[i] = new AtomicLong();
      rbdawc[i] = new AtomicLong();

      rbs[i] = new AtomicReference[READ_BUFFER_SIZE];
      for (int n = 0; n < READ_BUFFER_SIZE; n++) {
        rbs[i][n] = new AtomicReference<OClosableEntry<K, V>>();
      }
    }

    readBufferWriteCount = rbwc;
    readBufferDrainAtWriteCount = rbdawc;
    readBuffers = rbs;
  }

  /**
   * Adds item to the container. Item should be in open state.
   *
   * @param key Key associated with given item.
   * @param item Item associated with passed in key.
   */
  public void add(K key, V item) throws InterruptedException {
    if (!item.isOpen())
      throw new IllegalArgumentException("All passed in items should be in open state");

    checkOpenFilesLimit();

    final OClosableEntry<K, V> closableEntry = new OClosableEntry<K, V>(item);
    final OClosableEntry<K, V> oldEntry = data.putIfAbsent(key, closableEntry);

    if (oldEntry != null) {
      throw new IllegalStateException("Item with key " + key + " already exists");
    }

    logAdd(closableEntry);
  }

  /**
   * Removes item associated with passed in key.
   *
   * @param key Key associated with item to remove.
   * @return Removed item.
   */
  public V remove(K key) {
    final OClosableEntry<K, V> removed = data.remove(key);

    if (removed != null) {
      long preStatus = removed.makeRetired();

      if (OClosableEntry.isOpen(preStatus)) {
        countClosedFiles();
      }

      logRemoved(removed);
      return removed.get();
    }

    return null;
  }

  /**
   * Acquires item associated with passed in key in container. It is guarantied that item will not
   * be closed if limit of open items will be exceeded and container will close rarely used items.
   *
   * @param key Key associated with item
   * @return Acquired item if key exists into container or <code>null</code> if there is no item
   *     associated with given container
   */
  public OClosableEntry<K, V> acquire(K key) throws InterruptedException {
    checkOpenFilesLimit();

    return doAcquireEntry(key);
  }

  private OClosableEntry<K, V> doAcquireEntry(K key) {
    final OClosableEntry<K, V> entry = data.get(key);

    if (entry == null) return null;

    boolean logOpen = false;
    entry.acquireStateLock();
    try {
      if (entry.isRetired() || entry.isDead()) {
        return null;
      } else if (entry.isClosed()) {
        entry.makeAcquiredFromClosed(entry.get());
        logOpen = true;
      } else if (entry.isOpen()) {
        entry.makeAcquiredFromOpen();
      } else {
        entry.incrementAcquired();
      }
    } finally {
      entry.releaseStateLock();
    }

    if (logOpen) {
      logOpen(entry);
    } else {
      logAcquire(entry);
    }

    assert entry.get().isOpen();
    return entry;
  }

  public OClosableEntry<K, V> tryAcquire(K key) throws InterruptedException {
    final boolean ok = tryCheckOpenFilesLimit();
    if (!ok) {
      return null;
    }

    return doAcquireEntry(key);
  }

  /**
   * Checks if containers limit of open files is reached.
   *
   * <p>In such case execution of threads which add or acquire items is stopped and they wait till
   * buffers will be emptied and nubmer of open files will be inside limit.
   */
  private void checkOpenFilesLimit() throws InterruptedException {
    CountDownLatch ol = openLatch.get();
    if (ol != null) ol.await();

    while (openFiles.get() > openLimit) {
      final CountDownLatch latch = new CountDownLatch(1);

      // make other threads to wait till we evict entries and close evicted open files
      if (openLatch.compareAndSet(null, latch)) {
        while (openFiles.get() > openLimit) {
          emptyBuffers();
        }

        latch.countDown();
        openLatch.set(null);
      } else {
        ol = openLatch.get();

        if (ol != null) ol.await();
      }
    }
  }

  private boolean tryCheckOpenFilesLimit() throws InterruptedException {
    CountDownLatch ol = openLatch.get();
    if (ol != null) ol.await();

    while (openFiles.get() > openLimit) {
      final CountDownLatch latch = new CountDownLatch(1);

      // make other threads to wait till we evict entries and close evicted open files
      if (openLatch.compareAndSet(null, latch)) {
        emptyBuffers();

        final boolean result = openFiles.get() <= openLimit;
        latch.countDown();
        openLatch.set(null);

        return result;
      } else {
        ol = openLatch.get();

        if (ol != null) ol.await();
      }
    }

    return true;
  }

  /**
   * Releases item acquired by call of {@link #acquire(Object)} method. After this call container is
   * free to close given item if limit of open files exceeded and this item is rarely used.
   *
   * @param entry Entry to release
   */
  public void release(OClosableEntry<K, V> entry) {
    if (entry != null) {
      entry.releaseAcquired();
    }
  }

  /**
   * Returns item without acquiring it. State of item is not guarantied in such case.
   *
   * @param key Key associated with required item.
   * @return Item associated with given key.
   */
  public V get(K key) {
    final OClosableEntry<K, V> entry = data.get(key);
    if (entry != null) return entry.get();

    return null;
  }

  /** Clears all content. */
  public void clear() {
    lruLock.lock();
    try {
      data.clear();
      openFiles.set(0);

      for (int n = 0; n < NUMBER_OF_READ_BUFFERS; n++) {
        final AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[n];
        for (int i = 0; i < READ_BUFFER_SIZE; i++) {
          buffer[i].set(null);
        }

        readBufferReadCount[n] = 0;
        readBufferWriteCount[n].set(0);
        readBufferDrainAtWriteCount[n].set(0);
      }

      stateBuffer.clear();

      while (lruList.poll() != null) ;
    } finally {
      lruLock.unlock();
    }
  }

  /**
   * Closes item related to passed in key. Item will be closed if it exists and is not acquired.
   *
   * @param key Key related to item that has going to be closed.
   * @return <code>true</code> if item was closed and <code>false</code> otherwise.
   */
  public boolean close(K key) {
    emptyBuffers();

    final OClosableEntry<K, V> entry = data.get(key);
    if (entry == null) return true;

    if (entry.makeClosed()) {
      countClosedFiles();

      return true;
    }

    return false;
  }

  boolean checkAllLRUListItemsInMap() {
    lruLock.lock();
    try {
      emptyWriteBuffer();
      emptyReadBuffers();

      for (OClosableEntry<K, V> entry : lruList) {
        boolean result = data.containsValue(entry);
        if (!result) return false;
      }

    } finally {
      lruLock.unlock();
    }

    return true;
  }

  boolean checkLRUSize() {
    return lruList.size() <= openLimit;
  }

  boolean checkLRUSizeEqualsToCapacity() {
    return lruList.size() == openLimit;
  }

  boolean checkAllOpenItemsInLRUList() {
    lruLock.lock();
    try {
      emptyWriteBuffer();
      emptyReadBuffers();

      for (OClosableEntry<K, V> entry : data.values()) {
        boolean contains = false;

        if (!entry.get().isOpen()) continue;

        for (OClosableEntry<K, V> lruEntry : lruList) {
          if (lruEntry == entry) {
            contains = true;
          }
        }

        if (!contains) return false;
      }
    } finally {
      lruLock.unlock();
    }

    return true;
  }

  boolean checkNoClosedItemsInLRUList() {
    lruLock.lock();
    try {
      emptyWriteBuffer();
      emptyReadBuffers();

      for (OClosableEntry<K, V> entry : data.values()) {
        boolean contains = false;

        if (entry.get().isOpen()) continue;

        for (OClosableEntry<K, V> lruEntry : lruList) {
          if (lruEntry == entry) {
            contains = true;
          }
        }

        if (contains) return false;
      }
    } finally {
      lruLock.unlock();
    }

    return true;
  }

  /**
   * Read content of write buffer and adds/removes LRU entries to update internal statistic. Method
   * has to be wrapped by LRU lock.
   */
  private void emptyWriteBuffer() {
    Runnable task = stateBuffer.poll();
    while (task != null) {
      task.run();
      task = stateBuffer.poll();
    }
  }

  /**
   * Read content of all read buffers and reorder elements inside of LRU list to update internal
   * statistic. Method has to be wrapped by LRU lock.
   */
  private void emptyReadBuffers() {
    for (int n = 0; n < NUMBER_OF_READ_BUFFERS; n++) {
      AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[n];

      long writeCount = readBufferDrainAtWriteCount[n].get();
      long counter = readBufferReadCount[n];

      while (true) {
        final int bufferIndex = (int) (counter & READ_BUFFER_INDEX_MASK);
        final AtomicReference<OClosableEntry<K, V>> eref = buffer[bufferIndex];
        final OClosableEntry<K, V> entry = eref.get();

        if (entry == null) break;

        applyRead(entry);
        counter++;

        eref.lazySet(null);
      }

      readBufferReadCount[n] = counter;
      readBufferDrainAtWriteCount[n].lazySet(writeCount);
    }
  }

  void emptyBuffers() {
    lruLock.lock();
    try {
      emptyWriteBuffer();
      emptyReadBuffers();

      evict();
    } finally {
      lruLock.unlock();
    }
  }

  /**
   * Put the entry to the tail of LRU list if entry is not in "retired" or "acquired" state.
   *
   * @param entry Entry to process.
   */
  private void logOpen(OClosableEntry<K, V> entry) {
    afterWrite(new LogOpen(entry));

    countOpenFiles();
  }

  /**
   * Put the entry at the tail of LRU list if if entry is not in "retired" or "acquired" state.
   *
   * @param entry LRU entry
   */
  private void logAdd(OClosableEntry<K, V> entry) {
    afterWrite(new LogAdd(entry));

    countOpenFiles();
  }

  /**
   * Put entry at the tail of LRU list if entry is already inside of LRU list.
   *
   * @param entry LRU entry
   */
  private void logAcquire(OClosableEntry<K, V> entry) {
    afterRead(entry);
  }

  /**
   * Remove LRU entry from the LRU list.
   *
   * @param entry LRU entry.
   */
  private void logRemoved(OClosableEntry<K, V> entry) {
    afterWrite(new LogRemoved(entry));
  }

  /**
   * Method is used to log operations which change content of the container. Such changes should be
   * flushed immediately to update content of LRU list.
   *
   * @param task Task which contains code is used to manipulate LRU list
   */
  private void afterWrite(Runnable task) {
    stateBuffer.add(task);
    drainStatus.lazySet(DrainStatus.REQUIRED);
    tryToDrainBuffers();
  }

  /**
   * Method is used to log operations which do not change LRU list content but affect order of items
   * inside of LRU list. Such changes may be delayed till buffer will be full.
   *
   * @param entry Entry which was affected by operation.
   */
  private void afterRead(OClosableEntry<K, V> entry) {
    final int bufferIndex = readBufferIndex();
    final long writeCount = putEntryInReadBuffer(entry, bufferIndex);
    drainReadBuffersIfNeeded(bufferIndex, writeCount);
  }

  /**
   * Adds entry to the read buffer with selected index and returns amount of writes to this buffer
   * since creation of this container.
   *
   * @param entry LRU entry to add.
   * @param bufferIndex Index of buffer
   * @return Amount of writes to the buffer since creation of this container.
   */
  private long putEntryInReadBuffer(OClosableEntry<K, V> entry, int bufferIndex) {
    // next index to write for this buffer
    AtomicLong writeCounter = readBufferWriteCount[bufferIndex];
    final long counter = writeCounter.get();

    // we do not use CAS operations to limit contention between threads
    // it is normal that because of duplications of indexes some of items will be lost
    writeCounter.lazySet(counter + 1);

    final AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[bufferIndex];
    AtomicReference<OClosableEntry<K, V>> bufferEntry =
        buffer[(int) (counter & READ_BUFFER_INDEX_MASK)];
    bufferEntry.lazySet(entry);

    return counter + 1;
  }

  /**
   * @param bufferIndex Read buffer index
   * @param writeCount Amount of writes performed for given buffer
   */
  private void drainReadBuffersIfNeeded(int bufferIndex, long writeCount) {
    // amount of writes to the buffer at the last time when buffer was flushed
    final AtomicLong lastDrainWriteCount = readBufferDrainAtWriteCount[bufferIndex];
    final boolean bufferOverflow = (writeCount - lastDrainWriteCount.get()) > READ_BUFFER_THRESHOLD;

    if (drainStatus.get().shouldBeDrained(bufferOverflow)) {
      tryToDrainBuffers();
    }
  }

  private void tryToDrainBuffers() {
    if (lruLock.tryLock()) {
      try {
        // optimization to avoid to call tryLock if it is not needed
        drainStatus.lazySet(DrainStatus.IN_PROGRESS);
        drainBuffers();
      } finally {
        // cas operation because we do not want to overwrite REQUIRED status and to avoid false
        // optimization of
        // drain buffer by IN_PROGRESS status
        drainStatus.compareAndSet(DrainStatus.IN_PROGRESS, DrainStatus.IDLE);
        lruLock.unlock();
      }
    }
  }

  private void drainBuffers() {
    drainWriteBuffer();
    drainReadBuffers();
  }

  private void drainReadBuffers() {
    final long threadId = Thread.currentThread().getId();
    for (long n = threadId; n < threadId + NUMBER_OF_READ_BUFFERS; n++) {
      drainReadBuffer((int) (n & READ_BUFFERS_MASK));
    }
  }

  private void drainReadBuffer(int bufferIndex) {
    // amount of writes to the buffer at the moment
    final long bufferWriteCount = readBufferWriteCount[bufferIndex].get();
    final AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[bufferIndex];
    // position of previous flush
    long bufferCounter = readBufferReadCount[bufferIndex];

    for (int n = 0; n < READ_BUFFER_DRAIN_THRESHOLD; n++) {
      final int entryIndex = (int) (bufferCounter & READ_BUFFER_INDEX_MASK);
      final AtomicReference<OClosableEntry<K, V>> bufferEntry = buffer[entryIndex];
      final OClosableEntry<K, V> entry = bufferEntry.get();
      if (entry == null) break;

      bufferCounter++;
      applyRead(entry);

      // GC optimization
      bufferEntry.lazySet(null);
    }

    readBufferReadCount[bufferIndex] = bufferCounter;
    readBufferDrainAtWriteCount[bufferIndex].lazySet(bufferWriteCount);
  }

  private void applyRead(OClosableEntry<K, V> entry) {
    if (lruList.contains(entry)) {
      lruList.moveToTheTail(entry);
    }
    evict();
  }

  private void drainWriteBuffer() {
    for (int i = 0; i < WRITE_BUFFER_DRAIN_THRESHOLD; i++) {
      Runnable task = stateBuffer.poll();
      if (task == null) break;

      task.run();
    }
  }

  private void countOpenFiles() {
    openFiles.incrementAndGet();
  }

  private void countClosedFiles() {
    openFiles.decrementAndGet();
  }

  /**
   * Finds closest power of two for given integer value. Idea is simple duplicate the most
   * significant bit to the lowest bits for the smallest number of iterations possible and then
   * increment result value by 1.
   *
   * @param value Integer the most significant power of 2 should be found.
   * @return The most significant power of 2.
   */
  private static int closestPowerOfTwo(int value) {
    int n = value - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= (1 << 30)) ? 1 << 30 : n + 1;
  }

  private static int readBufferIndex() {
    // partition buffers between threads
    final long threadId = Thread.currentThread().getId();
    return (int) (threadId & READ_BUFFERS_MASK);
  }

  private void evict() {
    final long start = Orient.instance().getProfiler().startChrono();

    final int initialSize = lruList.size();
    int closedFiles = 0;

    while (lruList.size() > openLimit) {
      // we may only close items in open state so we "peek" them first
      Iterator<OClosableEntry<K, V>> iterator = lruList.iterator();

      boolean entryClosed = false;

      while (iterator.hasNext()) {
        OClosableEntry<K, V> entry = iterator.next();
        if (entry.makeClosed()) {
          closedFiles++;
          iterator.remove();
          entryClosed = true;

          countClosedFiles();
          break;
        }
      }

      // there are no items in open state stop eviction
      if (!entryClosed) break;
    }

    if (closedFiles > 0) {
      OLogManager.instance()
          .debug(
              this,
              "Reached maximum of opened files %d (max=%d), closed %d files. Consider to raise this limit by increasing the global setting '%s' and the OS limit on opened files per processor",
              initialSize,
              openLimit,
              closedFiles,
              OGlobalConfiguration.OPEN_FILES_LIMIT.getKey());
    }

    Orient.instance()
        .getProfiler()
        .stopChrono(
            "disk.closeFiles",
            "Close the opened files because reached the configured limit",
            start);
  }

  private class LogAdd implements Runnable {
    private final OClosableEntry<K, V> entry;

    private LogAdd(OClosableEntry<K, V> entry) {
      this.entry = entry;
    }

    @Override
    public void run() {
      // despite of the fact that status can be change it is safe to proceed because it means
      // that LogRemove entree will be after LogAdd entree (we call markRetired firs and then only
      // log entry removal)
      if (!entry.isDead() && !entry.isRetired()) {
        lruList.moveToTheTail(entry);
        evict();
      }
    }
  }

  private class LogRemoved implements Runnable {
    private final OClosableEntry<K, V> entry;

    private LogRemoved(OClosableEntry<K, V> entry) {
      this.entry = entry;
    }

    @Override
    public void run() {
      if (entry.isRetired()) {
        lruList.remove(entry);
        entry.makeDead();
      }
    }
  }

  private class LogOpen implements Runnable {
    private final OClosableEntry<K, V> entry;

    private LogOpen(OClosableEntry<K, V> entry) {
      this.entry = entry;
    }

    @Override
    public void run() {
      if (!entry.isRetired() && !entry.isDead()) {
        lruList.moveToTheTail(entry);
        evict();
      }
    }
  }

  private enum DrainStatus {
    IDLE {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return readBufferOverflow;
      }
    },
    IN_PROGRESS {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return false;
      }
    },
    REQUIRED {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return true;
      }
    };

    abstract boolean shouldBeDrained(boolean readBufferOverflow);
  }
}
