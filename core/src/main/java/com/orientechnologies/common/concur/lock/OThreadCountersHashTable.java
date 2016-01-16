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

package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 8/20/14
 */
public final class OThreadCountersHashTable implements OOrientStartupListener, OOrientShutdownListener {
  private static final int                                        SEED             = 362498820;

  private static final int                                        NCPU             = Runtime.getRuntime().availableProcessors();
  private static final int                                        DEFAULT_SIZE     = 1 << (32 - Integer
                                                                                       .numberOfLeadingZeros((NCPU << 2) - 1));

  public static final int                                         THRESHOLD        = 10;
  private final boolean                                           deadThreadsAreAllowed;

  private volatile ThreadLocal<HashEntry>                         hashEntry        = new ThreadLocal<HashEntry>();

  private volatile int                                            activeTableIndex = 0;

  private final AtomicReference<AtomicReference<EntryHolder>[]>[] tables;
  private final AtomicInteger[]                                   busyCounters;

  private final AtomicBoolean                                     tablesAreBusy    = new AtomicBoolean(false);

  public OThreadCountersHashTable() {
    this(DEFAULT_SIZE, false);
  }

  public OThreadCountersHashTable(int initialSize, boolean deadThreadsAreAllowed) {
    this.deadThreadsAreAllowed = deadThreadsAreAllowed;

    AtomicReference<EntryHolder>[] activeTable = new AtomicReference[initialSize << 1];
    AtomicReference<AtomicReference<EntryHolder>[]>[] tables = new AtomicReference[32];

    for (int i = 0; i < activeTable.length; i++)
      activeTable[i] = new AtomicReference<EntryHolder>(new EntryHolder(0, null, false));

    tables[0] = new AtomicReference<AtomicReference<EntryHolder>[]>(activeTable);
    for (int i = 1; i < tables.length; i++)
      tables[i] = new AtomicReference<AtomicReference<EntryHolder>[]>(null);

    AtomicInteger[] counters = new AtomicInteger[32];
    for (int i = 0; i < counters.length; i++)
      counters[i] = new AtomicInteger();

    busyCounters = counters;

    this.tables = tables;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public void increment() {
    HashEntry entry = hashEntry.get();

    if (entry == null) {
      final Thread thread = Thread.currentThread();
      entry = new HashEntry(thread, hashCodesByThreadId(thread.getId()));

      assert search(entry.thread) == null;
      insert(entry);
      assert search(entry.thread).thread == thread;

      hashEntry.set(entry);
    }

    entry.threadCounter++;
  }

  public void decrement() {
    final HashEntry entry = hashEntry.get();

    assert entry != null;

    entry.threadCounter--;
  }

  public boolean isEmpty() {
    int activeTableIndex;
    do {
      activeTableIndex = this.activeTableIndex;

      for (int i = 0; i <= activeTableIndex; i++) {
        if (i != activeTableIndex) {
          while (busyCounters[i].get() != 0)
            ;
        }

        final AtomicReference<EntryHolder>[] table = tables[i].get();
        if (tableCountersNotEmpty(table))
          return false;
      }

    } while (this.activeTableIndex != activeTableIndex);

    return true;
  }

  private boolean tableCountersNotEmpty(AtomicReference<EntryHolder>[] table) {
    for (AtomicReference<EntryHolder> entryHolderRef : table) {
      final EntryHolder entryHolder = entryHolderRef.get();

      if (!entryIsEmpty(entryHolder) && entryHolder.entry.threadCounter > 0)
        return true;
    }
    return false;
  }

  private boolean entryIsEmpty(EntryHolder entryHolder) {
    if (deadThreadsAreAllowed)
      return entryHolder.entry == null;

    return entryHolder.entry == null || !entryHolder.entry.thread.isAlive();
  }

  HashEntry search(Thread thread) {
    int[] hashCodes = hashCodesByThreadId(thread.getId());
    int activeTableIndex;
    do {
      activeTableIndex = this.activeTableIndex;
      for (int i = 0; i <= activeTableIndex; i++) {
        final AtomicReference<EntryHolder>[] table = tables[i].get();

        if (i != activeTableIndex) {
          while (busyCounters[i].get() != 0)
            ;
        }

        final HashEntry entry = searchInTables(thread, hashCodes, table);
        if (entry != null)
          return entry;
      }
    } while (activeTableIndex != this.activeTableIndex);

    return null;
  }

  private static HashEntry searchInTables(Thread thread, int[] hashCodes, AtomicReference<EntryHolder>[] tables) {
    while (true) {
      final int firstTableIndex = firstSubTableIndex(hashCodes, tables.length);
      final EntryHolder firstEntryHolderRnd1 = tables[firstTableIndex].get();

      if (firstEntryHolderRnd1.entry != null && firstEntryHolderRnd1.entry.thread == thread)
        return firstEntryHolderRnd1.entry;

      final int secondTableIndex = secondSubTableIndex(hashCodes, tables.length);
      final EntryHolder secondEntryHolderRnd1 = tables[secondTableIndex].get();

      if (secondEntryHolderRnd1.entry != null && secondEntryHolderRnd1.entry.thread == thread)
        return secondEntryHolderRnd1.entry;

      final EntryHolder firstEntryHolderRnd2 = tables[firstTableIndex].get();
      if (firstEntryHolderRnd2.entry != null && firstEntryHolderRnd2.entry.thread == thread)
        return firstEntryHolderRnd2.entry;

      final EntryHolder secondEntryHolderRnd2 = tables[secondTableIndex].get();

      if (secondEntryHolderRnd2.entry != null && secondEntryHolderRnd2.entry.thread == thread)
        return secondEntryHolderRnd2.entry;

      if (!checkCounter(firstEntryHolderRnd1.counter, secondEntryHolderRnd1.counter, firstEntryHolderRnd2.counter,
          secondEntryHolderRnd2.counter)) {
        return null;
      }
    }
  }

  private FindResult find(Thread thread, int[] hashCodes, AtomicReference<EntryHolder>[] tables) {
    while (true) {
      FindResult result = null;

      final int firstTableIndex = firstSubTableIndex(hashCodes, tables.length);
      final EntryHolder firstEntryHolderRnd1 = tables[firstTableIndex].get();

      final int secondTableIndex = secondSubTableIndex(hashCodes, tables.length);
      final EntryHolder secondEntryHolderRnd1 = tables[secondTableIndex].get();

      if (firstEntryHolderRnd1.markedForRelocation) {
        helpRelocate(firstTableIndex, false, tables);
        continue;
      }

      if (firstEntryHolderRnd1.entry != null && firstEntryHolderRnd1.entry.thread == thread)
        result = new FindResult(true, true, firstEntryHolderRnd1, secondEntryHolderRnd1);

      if (secondEntryHolderRnd1.markedForRelocation) {
        helpRelocate(secondTableIndex, false, tables);
        continue;
      }

      if (secondEntryHolderRnd1.entry != null && secondEntryHolderRnd1.entry.thread == thread) {
        assert result == null;
        result = new FindResult(true, false, firstEntryHolderRnd1, secondEntryHolderRnd1);
      }

      if (result != null)
        return result;

      final EntryHolder firstEntryHolderRnd2 = tables[firstTableIndex].get();
      final EntryHolder secondEntryHolderRnd2 = tables[secondTableIndex].get();

      if (firstEntryHolderRnd2.markedForRelocation) {
        helpRelocate(firstTableIndex, false, tables);
        continue;
      }

      if (firstEntryHolderRnd2.entry != null && firstEntryHolderRnd2.entry.thread == thread)
        result = new FindResult(true, true, firstEntryHolderRnd2, secondEntryHolderRnd2);

      if (secondEntryHolderRnd2.markedForRelocation) {
        helpRelocate(secondTableIndex, false, tables);
        continue;
      }

      if (secondEntryHolderRnd2.entry != null && secondEntryHolderRnd2.entry.thread == thread) {
        assert result == null;
        result = new FindResult(true, false, firstEntryHolderRnd1, secondEntryHolderRnd1);
      }

      if (result != null)
        return result;

      if (!checkCounter(firstEntryHolderRnd1.counter, secondEntryHolderRnd1.counter, firstEntryHolderRnd2.counter,
          secondEntryHolderRnd2.counter))
        return new FindResult(false, false, firstEntryHolderRnd2, secondEntryHolderRnd2);
    }
  }

  private static boolean checkCounter(long firstEntryRnd1, long secondEntryRnd1, long firstEntryRnd2, long secondEntryRnd2) {
    return firstEntryRnd2 - firstEntryRnd1 >= 2 && secondEntryRnd2 - secondEntryRnd1 >= 2 && secondEntryRnd2 - firstEntryRnd1 >= 3;
  }

  void insert(Thread thread) {
    HashEntry entry = new HashEntry(thread, hashCodesByThreadId(thread.getId()));
    insert(entry);
  }

  private void insert(final HashEntry newEntry) {
    while (true) {
      final int activeTableIndex = this.activeTableIndex;
      final AtomicReference<EntryHolder>[] table = tables[activeTableIndex].get();

      final AtomicInteger counter = busyCounters[activeTableIndex];

      counter.getAndIncrement();
      boolean result = insertInTables(newEntry, table);
      counter.getAndDecrement();

      if (!result) {
        if (!rehash())
          LockSupport.parkNanos(10);
      } else {

        return;
      }

    }
  }

  private boolean insertInTables(final HashEntry newEntry, AtomicReference<EntryHolder>[] tables) {
    while (true) {
      final FindResult result = find(newEntry.thread, newEntry.hashCodes, tables);

      assert !result.found;

      if (entryIsEmpty(result.firstEntryHolder)) {
        final int firstTableIndex = firstSubTableIndex(newEntry.hashCodes, tables.length);
        final EntryHolder holder = result.firstEntryHolder;

        if (tables[firstTableIndex].compareAndSet(holder, new EntryHolder(holder.counter, newEntry, false)))
          return true;
      }

      if (entryIsEmpty(result.secondEntryHolder)) {
        final int secondTableIndex = secondSubTableIndex(newEntry.hashCodes, tables.length);
        final EntryHolder holder = result.secondEntryHolder;

        if (tables[secondTableIndex].compareAndSet(holder, new EntryHolder(holder.counter, newEntry, false)))
          return true;
      }

      if (!relocate(firstSubTableIndex(newEntry.hashCodes, tables.length), tables))
        return false;
    }
  }

  private boolean rehash() {
    if (!tablesAreBusy.compareAndSet(false, true))
      return false;

    AtomicReference<EntryHolder>[] activeTable = tables[activeTableIndex].get();
    AtomicReference<EntryHolder>[] newActiveTable = new AtomicReference[activeTable.length << 1];

    for (int i = 0; i < newActiveTable.length; i++)
      newActiveTable[i] = new AtomicReference<EntryHolder>(new EntryHolder(0, null, false));

    tables[activeTableIndex + 1].set(newActiveTable);
    activeTableIndex++;

    tablesAreBusy.set(false);
    return true;
  }

  private boolean relocate(int entryIndex, AtomicReference<EntryHolder>[] tables) {
    int startLevel = 0;
    final int tableSize = tables.length >> 1;

    path_discovery: while (true) {
      if (startLevel >= THRESHOLD)
        startLevel = 0;

      boolean found = false;

      final int[] route = new int[10];
      int depth = startLevel;
      do {
        EntryHolder entryHolder = tables[entryIndex].get();

        while (entryHolder.markedForRelocation) {
          helpRelocate(entryIndex, false, tables);
          entryHolder = tables[entryIndex].get();
        }

        if (!entryIsEmpty(entryHolder)) {
          route[depth] = entryIndex;

          if (entryIndex < tableSize)
            entryIndex = secondSubTableIndex(entryHolder.entry.hashCodes, tables.length);
          else
            entryIndex = firstSubTableIndex(entryHolder.entry.hashCodes, tables.length);

          depth++;
        } else
          found = true;
      } while (!found && depth < THRESHOLD);

      if (found) {
        for (int i = depth - 1; i >= 0; i--) {
          final int index = route[i];

          EntryHolder entryHolder = tables[index].get();

          if (entryHolder.markedForRelocation) {
            helpRelocate(index, false, tables);
            entryHolder = tables[index].get();
          }

          if (entryIsEmpty(entryHolder))
            continue;

          final int destinationIndex = index < tableSize ? secondSubTableIndex(entryHolder.entry.hashCodes, tables.length)
              : firstSubTableIndex(entryHolder.entry.hashCodes, tables.length);

          EntryHolder destinationEntry = tables[destinationIndex].get();

          if (!entryIsEmpty(destinationEntry)) {
            startLevel = i + 1;
            entryIndex = destinationIndex;
            continue path_discovery;
          }

          if (!helpRelocate(index, true, tables)) {
            startLevel = i + 1;
            entryIndex = destinationIndex;
            continue path_discovery;
          }
        }
      }

      return found;
    }
  }

  private boolean helpRelocate(int entryIndex, boolean initiator, AtomicReference<EntryHolder>[] tables) {
    final int tableSize = tables.length >> 1;

    while (true) {
      EntryHolder src = tables[entryIndex].get();

      while (initiator && !src.markedForRelocation) {
        if (entryIsEmpty(src))
          return true;

        tables[entryIndex].compareAndSet(src, new EntryHolder(src.counter, src.entry, true));
        src = tables[entryIndex].get();
      }

      if (!src.markedForRelocation)
        return true;

      final int destinationIndex = entryIndex < tableSize ? secondSubTableIndex(src.entry.hashCodes, tables.length)
          : firstSubTableIndex(src.entry.hashCodes, tables.length);

      final EntryHolder destinationHolder = tables[destinationIndex].get();

      if (entryIsEmpty(destinationHolder)) {
        final long newCounter = destinationHolder.counter > src.counter ? destinationHolder.counter + 1 : src.counter + 1;

        if (src != tables[entryIndex].get())
          continue;

        if (tables[destinationIndex].compareAndSet(destinationHolder, new EntryHolder(newCounter, src.entry, false))) {
          tables[entryIndex].compareAndSet(src, new EntryHolder(src.counter + 1, null, false));
          return true;
        } else
          continue;
      }

      if (destinationHolder.entry == src.entry) {
        tables[entryIndex].compareAndSet(src, new EntryHolder(src.counter + 1, null, false));
        return true;
      }

      tables[entryIndex].compareAndSet(src, new EntryHolder(src.counter, src.entry, false));
      return false;
    }
  }

  @Override
  public void onShutdown() {
    hashEntry = null;
  }

  @Override
  public void onStartup() {
    if (hashEntry == null)
      hashEntry = new ThreadLocal<HashEntry>();
  }

  private static int secondSubTableIndex(int[] hashCodes, int size) {
    final int subTableSize = size >> 1;
    return (hashCodes[1] & (subTableSize - 1)) + subTableSize;
  }

  private static int firstSubTableIndex(int[] hashCodes, int size) {
    return hashCodes[0] & ((size >> 1) - 1);
  }

  private static int[] hashCodesByThreadId(final long threadId) {
    final byte[] serializedId = new byte[8];
    OLongSerializer.INSTANCE.serializeNative(threadId, serializedId, 0);

    final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedId, SEED);
    return new int[] { (int) (hashCode & 0xFFFFFFFFL), (int) (hashCode >>> 32) };
  }

  static final class HashEntry {
    private final Thread  thread;
    private final int[]   hashCodes;

    private volatile long p0            = 0, p1 = 1, p2 = 2, p3 = 3, p4 = 4, p5 = 5, p6 = 6, p7 = 7;
    private volatile long threadCounter = 0;
    private volatile long p8            = 0, p9 = 1, p10 = 2, p11 = 3, p12 = 4, p13 = 5, p14 = 6;

    private HashEntry(Thread thread, int[] hashCodes) {
      this.thread = thread;
      this.hashCodes = hashCodes;
    }

    public Thread getThread() {
      return thread;
    }

    @Override
    public String toString() {
      modCounters();

      return "HashEntry{" + "thread=" + thread + ", hashCodes=" + Arrays.toString(hashCodes) + ", p0=" + p0 + ", p1=" + p1
          + ", p2=" + p2 + ", p3=" + p3 + ", p4=" + p4 + ", p5=" + p5 + ", p6=" + p6 + ", p7=" + p7 + ", threadCounter="
          + threadCounter + ", p8=" + p8 + ", p9=" + p9 + ", p10=" + p10 + ", p11=" + p11 + ", p12=" + p12 + ", p13=" + p13
          + ", p14=" + p14 + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      HashEntry hashEntry = (HashEntry) o;

      if (p1 != hashEntry.p1)
        return false;
      if (p10 != hashEntry.p10)
        return false;
      if (p11 != hashEntry.p11)
        return false;
      if (p12 != hashEntry.p12)
        return false;
      if (p13 != hashEntry.p13)
        return false;
      if (p14 != hashEntry.p14)
        return false;
      if (p2 != hashEntry.p2)
        return false;
      if (p3 != hashEntry.p3)
        return false;
      if (p4 != hashEntry.p4)
        return false;
      if (p5 != hashEntry.p5)
        return false;
      if (p6 != hashEntry.p6)
        return false;
      if (p7 != hashEntry.p7)
        return false;
      if (p8 != hashEntry.p8)
        return false;
      if (p9 != hashEntry.p9)
        return false;
      if (p0 != hashEntry.p0)
        return false;
      if (threadCounter != hashEntry.threadCounter)
        return false;
      if (!Arrays.equals(hashCodes, hashEntry.hashCodes))
        return false;
      if (thread != null ? !thread.equals(hashEntry.thread) : hashEntry.thread != null)
        return false;

      return true;
    }

    private void modCounters() {
      final Random random = new Random();
      p0 = random.nextLong();
      p1 = random.nextLong();
      p2 = random.nextLong();
      p3 = random.nextLong();
      p4 = random.nextLong();
      p5 = random.nextLong();
      p6 = random.nextLong();
      p7 = random.nextLong();
      p8 = random.nextLong();
      p9 = random.nextLong();
      p10 = random.nextLong();
      p11 = random.nextLong();
      p12 = random.nextLong();
      p13 = random.nextLong();
      p14 = random.nextLong();
    }

    @Override
    public int hashCode() {
      int result = thread != null ? thread.hashCode() : 0;
      result = 31 * result + (hashCodes != null ? Arrays.hashCode(hashCodes) : 0);
      result = 31 * result + (int) (p0 ^ (p0 >>> 32));
      result = 31 * result + (int) (p1 ^ (p1 >>> 32));
      result = 31 * result + (int) (p2 ^ (p2 >>> 32));
      result = 31 * result + (int) (p3 ^ (p3 >>> 32));
      result = 31 * result + (int) (p4 ^ (p4 >>> 32));
      result = 31 * result + (int) (p5 ^ (p5 >>> 32));
      result = 31 * result + (int) (p6 ^ (p6 >>> 32));
      result = 31 * result + (int) (p7 ^ (p7 >>> 32));
      result = 31 * result + (int) (threadCounter ^ (threadCounter >>> 32));
      result = 31 * result + (int) (p8 ^ (p8 >>> 32));
      result = 31 * result + (int) (p9 ^ (p9 >>> 32));
      result = 31 * result + (int) (p10 ^ (p10 >>> 32));
      result = 31 * result + (int) (p11 ^ (p11 >>> 32));
      result = 31 * result + (int) (p12 ^ (p12 >>> 32));
      result = 31 * result + (int) (p13 ^ (p13 >>> 32));
      result = 31 * result + (int) (p14 ^ (p14 >>> 32));
      return result;
    }
  }

  private static final class EntryHolder {
    private final long      counter;
    private final HashEntry entry;
    private final boolean   markedForRelocation;

    private EntryHolder(long counter, HashEntry entry, boolean markedForRelocation) {
      this.counter = counter;
      this.entry = entry;
      this.markedForRelocation = markedForRelocation;
    }
  }

  private static final class FindResult {
    private final boolean     found;
    private final boolean     firstTable;
    private final EntryHolder firstEntryHolder;
    private final EntryHolder secondEntryHolder;

    private FindResult(boolean found, boolean firstTable, EntryHolder firstEntryHolder, EntryHolder secondEntryHolder) {
      this.found = found;
      this.firstTable = firstTable;
      this.firstEntryHolder = firstEntryHolder;
      this.secondEntryHolder = secondEntryHolder;
    }
  }
}
