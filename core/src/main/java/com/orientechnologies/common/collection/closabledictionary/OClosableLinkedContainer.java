package com.orientechnologies.common.collection.closabledictionary;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Container for the elements which may be in open/closed state.
 * But only limited amount of elements are hold by given container may be in open state.
 * <p>
 * Elements are added in this container in open state,as result after addition of some elements other
 * rarely used elements will be closed.
 * <p>
 * When you want to use elements from container you should acquire them {@link #acquire(Object)}. So element still will be inside of container
 * but in acquired state. As result it will not be automatically closed when container will try to close rarely used items.
 * <p>
 * Container uses LRU eviction policy to choose item to be closed.
 * <p>
 * When you finish to work with element from container you should release (@link {@link #release(OClosableEntry)}) element back to container.
 *
 * @param <K> Key associated with entry stored inside of container.
 * @param <V> Value which may be in open/closed stated and associated with key.
 */
public class OClosableLinkedContainer<K, V extends OClosableItem> {
  /**
   * The number of CPUs
   */
  private static final int NCPU = Runtime.getRuntime().availableProcessors();

  /**
   * The number of read buffers to use.
   */
  private static final int NUMBER_OF_READ_BUFFERS = closestPowerOfTwo(NCPU);

  /**
   * Mask value for indexing into the read buffers.
   */
  private static final int READ_BUFFERS_MASK = NUMBER_OF_READ_BUFFERS - 1;

  /**
   * The number of pending read operations before attempting to drain.
   */
  private static final int READ_BUFFER_THRESHOLD = 32;

  /**
   * The maximum number of read operations to perform per amortized drain.
   */
  private static final int READ_BUFFER_DRAIN_THRESHOLD = 2 * READ_BUFFER_THRESHOLD;

  /**
   * The maximum number of write operations to perform per amortized drain.
   */
  private static final int WRITE_BUFFER_DRAIN_THRESHOLD = 32;

  /**
   * The maximum number of pending reads per buffer.
   */
  private static final int READ_BUFFER_SIZE = 2 * READ_BUFFER_DRAIN_THRESHOLD;

  /**
   * Mask value for indexing into the read buffer.
   */
  private static final int READ_BUFFER_INDEX_MASK = READ_BUFFER_SIZE - 1;

  /**
   * Last indexes of buffers on which buffer flush procedure was stopped
   */
  private final long[] readBufferReadCount = new long[NUMBER_OF_READ_BUFFERS];

  /**
   * Next indexes of buffers on which threads will write new entries during logging of acquire operations
   */
  private final AtomicLong[] readBufferWriteCount;

  /**
   * Values of {@link #readBufferWriteCount} on which buffers were flushed.
   */
  private final AtomicLong[] readBufferDrainAtWriteCount;

  /**
   * Read buffers are used to lot {@link #acquire(Object)} operations when item is not switched from closed to open states.
   * So in cases when amount of items inside of {@link #lruList} is not going to be changed, but only information about recency
   * of items should be modified.
   */
  private final AtomicReference<OClosableEntry<K, V>>[][] readBuffers;

  /**
   * Lock which wraps all buffer flush operations and as result protects changes of {@link #lruList}.
   */
  private final Lock lruLock = new ReentrantLock();

  /**
   * LRU list to updated statistic of recency of contained items.
   */
  private final OClosableLRUList<K, V> lruList = new OClosableLRUList<K, V>();

  /**
   * Main source of truth of container if value is absent in this field it is absent in container.
   */
  private final ConcurrentHashMap<K, OClosableEntry<K, V>> data = new ConcurrentHashMap<K, OClosableEntry<K, V>>();

  /**
   * Buffer which contains operation which includes changes of states from closed to open, and from any state to retired.
   * In other words this buffer contains information about operations which affect amount of items inside of {@link #lruList} and
   * this operations can not be lost.
   */
  private final ConcurrentLinkedQueue<Runnable> stateBuffer = new ConcurrentLinkedQueue<Runnable>();

  /**
   * Maximum amount of open items inside of container.
   */
  private final int lruCapacity;

  private final AtomicReference<DrainStatus> drainStatus = new AtomicReference<DrainStatus>(DrainStatus.IDLE);

  public OClosableLinkedContainer(final int lruCapacity) {
    this.lruCapacity = lruCapacity;

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

  public void add(K key, V item) {
    final OClosableEntry<K, V> closableEntry = new OClosableEntry<K, V>(item);
    final OClosableEntry<K, V> oldEntry = data.putIfAbsent(key, closableEntry);

    if (oldEntry != null) {
      throw new IllegalStateException("Item with key " + key + " already exists");
    }

    logAdd(closableEntry);
  }

  public V remove(K key) {
    final OClosableEntry<K, V> removed = data.remove(key);

    if (removed != null) {
      removed.makeRetired();
      logRemoved(removed);
      return removed.get();
    }

    return null;
  }

  public OClosableEntry<K, V> acquire(K key) {
    final OClosableEntry<K, V> entry = data.get(key);

    if (entry == null)
      return null;

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

  public void release(OClosableEntry<K, V> entry) {
    entry.releaseAcquired();
  }

  public OClosableItem get(K key) {
    final OClosableEntry<K, V> entry = data.get(key);
    if (entry != null)
      return entry.get();

    return null;
  }

  public void clear() {
    lruLock.lock();
    try {
      data.clear();

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

      while (lruList.poll() != null)
        ;
    } finally {
      lruLock.unlock();
    }
  }

  public Set<K> keySet() {
    return Collections.unmodifiableSet(data.keySet());
  }

  boolean checkAllLRUListItemsInMap() {
    lruLock.lock();
    try {
      emptyWriteBuffer();
      emptyReadBuffers();

      for (OClosableEntry<K, V> entry : lruList) {
        boolean result = data.containsValue(entry);
        if (!result)
          return false;
      }

    } finally {
      lruLock.unlock();
    }

    return true;
  }

  boolean checkLRUSize() {
    return lruList.size() <= lruCapacity;
  }

  boolean checkLRUSizeEqualsToCapacity() {
    return lruList.size() == lruCapacity;
  }

  boolean checkAllOpenItemsInLRUList() {
    lruLock.lock();
    try {
      emptyWriteBuffer();
      emptyReadBuffers();

      for (OClosableEntry<K, V> entry : data.values()) {
        boolean contains = false;

        if (!entry.get().isOpen())
          continue;

        for (OClosableEntry<K, V> lruEntry : lruList) {
          if (lruEntry == entry) {
            contains = true;
          }
        }

        if (!contains)
          return false;
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

        if (entry.get().isOpen())
          continue;

        for (OClosableEntry<K, V> lruEntry : lruList) {
          if (lruEntry == entry) {
            contains = true;
          }
        }

        if (contains)
          return false;
      }
    } finally {
      lruLock.unlock();
    }

    return true;
  }

  private void emptyWriteBuffer() {
    Runnable task = stateBuffer.poll();
    while (task != null) {
      task.run();
      task = stateBuffer.poll();
    }
  }

  private void emptyReadBuffers() {
    for (int n = 0; n < NUMBER_OF_READ_BUFFERS; n++) {
      AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[n];

      long writeCount = readBufferDrainAtWriteCount[n].get();
      long counter = readBufferReadCount[n];

      while (true) {
        final int bufferIndex = (int) (counter & READ_BUFFER_INDEX_MASK);
        final AtomicReference<OClosableEntry<K, V>> eref = buffer[bufferIndex];
        final OClosableEntry<K, V> entry = eref.get();

        if (entry == null)
          break;

        applyRead(entry);

        counter++;
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
    } finally {
      lruLock.unlock();
    }

  }

  private void logOpen(OClosableEntry<K, V> entry) {
    afterWrite(new LogOpen(entry));
  }

  /**
   * Put the entry at the tail of LRU list if it is absent
   *
   * @param entry LRU entry
   */
  private void logAdd(OClosableEntry<K, V> entry) {
    afterWrite(new LogAdd(entry));
  }

  /**
   * Put entry at the tail of LRU list
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

  private void afterWrite(Runnable task) {
    stateBuffer.add(task);
    drainStatus.lazySet(DrainStatus.REQUIRED);
    tryToDrainBuffers();
  }

  private void afterRead(OClosableEntry<K, V> entry) {
    final int bufferIndex = readBufferIndex();
    final long writeCount = putEntryInReadBuffer(entry, bufferIndex);
    drainReadBuffersIfNeeded(bufferIndex, writeCount);
  }

  private long putEntryInReadBuffer(OClosableEntry<K, V> entry, int bufferIndex) {
    AtomicLong writeCounter = readBufferWriteCount[bufferIndex];
    final long counter = writeCounter.get();

    writeCounter.lazySet(counter + 1);
    final AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[bufferIndex];
    AtomicReference<OClosableEntry<K, V>> bufferEntry = buffer[(int) (counter & READ_BUFFER_INDEX_MASK)];
    bufferEntry.lazySet(entry);

    return counter + 1;
  }

  private void drainReadBuffersIfNeeded(int bufferIndex, long writeCount) {
    final AtomicLong lastDrainWriteCount = readBufferDrainAtWriteCount[bufferIndex];
    final boolean bufferOverflow = (writeCount - lastDrainWriteCount.get()) > READ_BUFFER_THRESHOLD;
    if (drainStatus.get().shouldBeDrained(bufferOverflow)) {
      tryToDrainBuffers();
    }
  }

  private void tryToDrainBuffers() {
    if (lruLock.tryLock()) {
      try {
        drainStatus.lazySet(DrainStatus.IN_PROGRESS);
        drainBuffers();
      } finally {
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
    final long bufferWriteCount = readBufferWriteCount[bufferIndex].get();
    final AtomicReference<OClosableEntry<K, V>>[] buffer = readBuffers[bufferIndex];
    long bufferCounter = readBufferReadCount[bufferIndex];

    for (int n = 0; n < READ_BUFFER_DRAIN_THRESHOLD; n++) {
      final int entryIndex = (int) (bufferCounter & READ_BUFFER_INDEX_MASK);
      final AtomicReference<OClosableEntry<K, V>> bufferEntry = buffer[entryIndex];
      final OClosableEntry<K, V> entry = bufferEntry.get();
      if (entry == null)
        break;

      bufferCounter++;
      applyRead(entry);
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
      if (task == null)
        break;

      task.run();
    }
  }

  /**
   * Finds closest power of two for given integer value. Idea is simple duplicate the most significant bit to the lowest bits for
   * the smallest number of iterations possible and then increment result value by 1.
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
    final long threadId = Thread.currentThread().getId();
    return (int) (threadId & READ_BUFFERS_MASK);
  }

  private void evict() {
    while (lruList.size() > lruCapacity) {
      Iterator<OClosableEntry<K, V>> iterator = lruList.iterator();

      boolean entryClosed = false;

      while (iterator.hasNext()) {
        OClosableEntry<K, V> entry = iterator.next();
        if (entry.makeClosed(entry.get())) {
          iterator.remove();
          entryClosed = true;
          break;
        }
      }

      if (!entryClosed)
        break;
    }
  }

  private class LogAdd implements Runnable {
    private final OClosableEntry<K, V> entry;

    private LogAdd(OClosableEntry<K, V> entry) {
      this.entry = entry;
    }

    @Override
    public void run() {
      //despite of the fact that status can be change it is safe to proceed because it means
      //that LogRemove entree will be after LogAdd entree (we call markRetired firs and then only log entry removal)
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
    }, IN_PROGRESS {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return false;
      }
    }, REQUIRED {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return true;
      }
    };

    abstract boolean shouldBeDrained(boolean readBufferOverflow);
  }
}
