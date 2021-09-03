/**
 * **************************************************************************** Copyright (c)
 * 2012-2013, Pedro Ramalhete, Andreia Correia All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of Concurrency Freaks nor the names of its contributors may be used to endorse
 * or promote products derived from this software without specific prior written permission.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * *****************************************************************************
 */
package com.orientechnologies.common.concur.lock;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 *
 *
 * <h1>Scalable Read-Write Lock </h1>
 *
 * A Read-Write Lock that is scalable with the number of threads doing Read. Uses a
 * two-state-machine for the Readers, and averages two synchronized operations. <br>
 * Although this mechanism was independently designed and implemented by the authors, the idea is
 * very similar to the algorithm C-RW-WP described in this paper: <a
 * href="http://blogs.oracle.com/dave/resource/ppopp13-dice-NUMAAwareRWLocks.pdf">NUMA-Aware
 * Reader-Writer locks</a> <br>
 * Relative to the paper, there are two differences: The threads have no particular order, which
 * means this implementation is <b>not</b> NUMA-aware; Threads attempting a read-lock for the first
 * time are added to a list and removed when the thread terminates, following the mechanism
 * described below. To manage the adding and removal of new Reader threads, we use a
 * ConcurrentLinkedQueue instance named {@code readersStateList} containing all the references to
 * ReadersEntry (Reader's states), which the Writer scans to determine if the Readers have completed
 * or not. After a thread terminates, the {@code finalize()} of the associated {@code ReaderEntry}
 * instance will be called, which will remove the Reader's state reference from the {@code
 * readersStateList}, to avoid memory leaking. Advantages:
 *
 * <ul>
 *   <li>Implements {@code java.util.concurrent.locks.ReadWriteLock}
 *   <li>When there are very few Writes, the performance scales with the number of Reader threads
 *   <li>No need to call initialization/cleanup functions per thread
 *   <li>No limitation on the number of concurrent threads
 * </ul>
 *
 * Disadvantages:
 *
 * <ul>
 *   <li>Not Reentrant
 *   <li>Has Writer-Preference
 *   <li>Memory footprint increases with number of threads by sizeof(ReadersEntry) x O(N_threads)
 *   <li>Does not support {@code lockInterruptibly()}
 *   <li>Does not support {@code newCondition()}
 * </ul>
 *
 * For scenarios with few writes, the average case for {@code sharedLock()} is two synchronized
 * calls: an {@code AtomicInteger.set()} on a cache line that is held in exclusive mode by the core
 * where the current thread is running, and an {@code AtomicLong.get()} on a shared cache line.<br>
 * This means that when doing several sequential calls of sharedLock()/unlock() on the same
 * instance, the performance penalty will be small because the accessed variables will most likely
 * be in L1/L2 cache.
 *
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */
public class ScalableRWLock implements ReadWriteLock, java.io.Serializable {

  private static final long serialVersionUID = -7552055681918630764L;

  // Definition of an invalid thread-id (must be negative)
  private static final int SRWL_STATE_NOT_WRITING = 0;
  private static final int SRWL_STATE_WRITING = 1;
  private static final int SRWL_STATE_NOT_READING = 0;
  private static final int SRWL_STATE_READING = 1;
  private static final AtomicInteger[] dummyArray = new AtomicInteger[0];

  /**
   * List of Reader's states that the Writer will scan when attempting to acquire the lock in
   * write-mode
   */
  private final transient ConcurrentLinkedQueue<AtomicInteger> readersStateList;

  /**
   * The thread-id of the Writer currently holding the lock in write-mode, or SRWL_INVALID_TID if
   * there is no Writer holding or attempting to acquire the lock in write mode.
   */
  private final transient StampedLock stampedLock;

  /**
   * Thread-local reference to the current thread's ReadersEntry instance. It's from this instance
   * that the current Reader thread is able to determine where to store its own state, and the
   * number of reentrant read lock loops for that particular thread.
   */
  private final transient ThreadLocal<ReadersEntry> entry;

  private final transient AtomicReference<AtomicInteger[]> readersStateArrayRef;

  /** The lock returned by method {@link ScalableRWLock#readLock}. */
  private final InnerReadLock readerLock;

  /** The lock returned by method {@link ScalableRWLock#writeLock}. */
  private final InnerWriteLock writerLock;

  /**
   * Inner class that makes use of finalize() to remove the Reader's state from the
   * ConcurrentLinkedQueue {@code readersStateList}
   */
  final class ReadersEntry {
    public final AtomicInteger state;

    public ReadersEntry(AtomicInteger state) {
      this.state = state;
    }

    @SuppressWarnings({"checkstyle:NoFinalizer"})
    protected void finalize() throws Throwable {
      removeState(state);
      super.finalize();
    }
  }

  /** Read-only lock */
  final class InnerReadLock implements Lock {
    public void lock() {
      sharedLock();
    }

    public void unlock() {
      sharedUnlock();
    }

    public boolean tryLock() {
      return sharedTryLock();
    }

    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException();
      return sharedTryLockNanos(unit.toNanos(timeout));
    }

    public void lockInterruptibly() throws InterruptedException {
      // Not supported
      throw new UnsupportedOperationException();
    }

    public Condition newCondition() {
      // Not supported
      throw new UnsupportedOperationException();
    }
  }

  /** Write-only lock */
  final class InnerWriteLock implements Lock {
    public void lock() {
      exclusiveLock();
    }

    public void unlock() {
      exclusiveUnlock();
    }

    public boolean tryLock() {
      return exclusiveTryLock();
    }

    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException();
      return exclusiveTryLockNanos(unit.toNanos(timeout));
    }

    public void lockInterruptibly() throws InterruptedException {
      // Not supported
      throw new UnsupportedOperationException();
    }

    public Condition newCondition() {
      // Not supported
      throw new UnsupportedOperationException();
    }
  }

  /** Default constructor */
  public ScalableRWLock() {
    // States of the Readers, one entry in the list per thread
    readersStateList = new ConcurrentLinkedQueue<AtomicInteger>();

    stampedLock = new StampedLock();

    // Default value for "entry" is null which is ok because, the thread
    // calling the constructor may never attempt to read-lock this
    // instance and, therefore, there is not point in allocating an
    // instance of ReadersEntry for it.
    entry = new ThreadLocal<ReadersEntry>();

    readersStateArrayRef = new AtomicReference<AtomicInteger[]>(null);

    readerLock = new ScalableRWLock.InnerReadLock();
    writerLock = new ScalableRWLock.InnerWriteLock();
  }

  public Lock readLock() {
    return readerLock;
  }

  public Lock writeLock() {
    return writerLock;
  }

  /**
   * This function should be called only from ReadersEntry.finalize()
   *
   * @param state The reader's state that we wish to remove from the ConcurrentLinkedQueue
   */
  protected void removeState(AtomicInteger state) {
    readersStateList.remove(state);
    readersStateArrayRef.set(null);
    // Paranoia: just in case someone forgot to call sharedUnlock()
    // and there is a Writer waiting on that state
    state.set(SRWL_STATE_NOT_READING);
  }

  /**
   * Creates a new ReadersEntry instance for the current thread and its associated AtomicInteger to
   * store the state of the Reader
   *
   * @return Returns a reference to the newly created instance of {@code ReadersEntry}
   */
  private ReadersEntry addState() {
    final AtomicInteger state = new AtomicInteger(SRWL_STATE_NOT_READING);
    final ReadersEntry newEntry = new ReadersEntry(state);
    entry.set(newEntry);
    readersStateList.add(state);
    readersStateArrayRef.set(null);
    return newEntry;
  }

  /**
   * Acquires the read lock.
   *
   * <p>Acquires the read lock if the write lock is not held by another thread and returns
   * immediately.
   *
   * <p>If the write lock is held by another thread then the current thread yields until the write
   * lock is released.
   */
  public void sharedLock() {
    ReadersEntry localEntry = entry.get();
    // Initialize a new Reader-state for this thread if needed
    if (localEntry == null) {
      localEntry = addState();
    }

    final AtomicInteger currentReadersState = localEntry.state;
    // The "optimistic" code path takes only two synchronized calls:
    // a set() on a cache line that should be held in exclusive mode
    // by the current thread, and a get() on a cache line that is shared.
    while (true) {
      currentReadersState.set(SRWL_STATE_READING);
      if (!stampedLock.isWriteLocked()) {
        // Acquired lock in read-only mode
        return;
      } else {
        // Go back to SRWL_STATE_NOT_READING to avoid blocking a Writer
        currentReadersState.set(SRWL_STATE_NOT_READING);
        // Some (other) thread is holding the write-lock, we must wait
        while (stampedLock.isWriteLocked()) {
          Thread.yield();
        }
      }
    }
  }

  /**
   * Attempts to release the read lock.
   *
   * <p>If the current thread is the holder of this lock then the {@code reentrantReaderCount} is
   * decremented. If the {@code reentrantReaderCount} is now zero then the lock is released. If the
   * current thread is not the holder of this lock then {@link IllegalMonitorStateException} is
   * thrown.
   *
   * @throws IllegalMonitorStateException if the current thread does not hold this lock.
   */
  public void sharedUnlock() {
    final ReadersEntry localEntry = entry.get();
    if (localEntry == null) {
      // ERROR: Tried to unlock a non read-locked lock
      throw new IllegalMonitorStateException();
    } else {
      localEntry.state.set(SRWL_STATE_NOT_READING);
      return;
    }
  }

  /**
   * Acquires the write lock.
   *
   * <p>Acquires the write lock if neither the read nor write lock are held by another thread and
   * returns immediately, setting the write lock {@code reentrantWriterCount} to one.
   *
   * <p>If the current thread already holds the write lock then the {@code reentrantWriterCount} is
   * incremented by one and the method returns immediately.
   *
   * <p>If the lock is held by another thread, then the current thread yields and lies dormant until
   * the write lock has been acquired, at which time the {@code reentrantWriterCount} is set to one.
   */
  public void exclusiveLock() {
    // Try to acquire the lock in write-mode
    stampedLock.writeLock();

    // We can only do this after writerOwner has been set to the current thread
    AtomicInteger[] localReadersStateArray = readersStateArrayRef.get();
    if (localReadersStateArray == null) {
      // Set to dummyArray before scanning the readersStateList to impose
      // a linearizability condition
      readersStateArrayRef.set(dummyArray);
      // Copy readersStateList to an array
      localReadersStateArray = readersStateList.toArray(new AtomicInteger[readersStateList.size()]);
      readersStateArrayRef.compareAndSet(dummyArray, localReadersStateArray);
    }

    // Scan the array of Reader states
    for (AtomicInteger readerState : localReadersStateArray) {
      while (readerState != null && readerState.get() == SRWL_STATE_READING) {
        Thread.yield();
      }
    }
  }

  /**
   * Attempts to release the write lock.
   *
   * <p>If the current thread is the holder of this lock then the {@code reentrantWriterCount} is
   * decremented. If {@code reentrantWriterCount} is now zero then the lock is released. If the
   * current thread is not the holder of this lock then {@link IllegalMonitorStateException} is
   * thrown.
   *
   * @throws IllegalMonitorStateException if the current thread does not hold this lock.
   */
  public void exclusiveUnlock() {
    if (!stampedLock.isWriteLocked()) {
      // ERROR: tried to unlock a non write-locked instance
      throw new IllegalMonitorStateException();
    }

    stampedLock.asWriteLock().unlock();
  }

  /**
   * Acquires the read lock only if the write lock is not held by another thread at the time of
   * invocation.
   *
   * <p>Acquires the read lock if the write lock is not held by another thread and returns
   * immediately with the value {@code true}.
   *
   * <p>If the write lock is held by another thread then this method will return immediately with
   * the value {@code false}.
   *
   * @return {@code true} if the read lock was acquired
   */
  public boolean sharedTryLock() {
    ReadersEntry localEntry = entry.get();
    // Initialize a new Reader-state for this thread if needed
    if (localEntry == null) {
      localEntry = addState();
    }

    final AtomicInteger currentReadersState = localEntry.state;
    currentReadersState.set(SRWL_STATE_READING);
    if (!stampedLock.isWriteLocked()) {
      // Acquired lock in read-only mode
      return true;
    } else {
      // Go back to SRWL_STATE_NOT_READING and quit
      currentReadersState.set(SRWL_STATE_NOT_READING);
      return false;
    }
  }

  /**
   * Acquires the read lock if the write lock is not held by another thread within the given waiting
   * time.
   *
   * <p>Acquires the read lock if the write lock is not held by another thread and returns
   * immediately with the value {@code true}.
   *
   * <p>If the write lock is held by another thread then the current thread yields execution until
   * one of two things happens:
   *
   * <ul>
   *   <li>The read lock is acquired by the current thread; or
   *   <li>The specified waiting time elapses.
   * </ul>
   *
   * <p>If the read lock is acquired then the value {@code true} is returned.
   *
   * @param nanosTimeout the time to wait for the read lock in nanoseconds
   * @return {@code true} if the read lock was acquired
   */
  public boolean sharedTryLockNanos(long nanosTimeout) {
    final long lastTime = System.nanoTime();
    ReadersEntry localEntry = entry.get();
    // Initialize a new Reader-state for this thread if needed
    if (localEntry == null) {
      localEntry = addState();
    }

    final AtomicInteger currentReadersState = localEntry.state;
    while (true) {
      currentReadersState.set(SRWL_STATE_READING);
      if (!stampedLock.isWriteLocked()) {
        // Acquired lock in read-only mode
        return true;
      } else {
        // Go back to SRWL_STATE_NOT_READING to avoid blocking a Writer
        // and then check if this is a downgrade.
        currentReadersState.set(SRWL_STATE_NOT_READING);

        if (nanosTimeout <= 0) return false;
        if (System.nanoTime() - lastTime < nanosTimeout) {
          Thread.yield();
        } else {
          return false;
        }
      }
    }
  }

  /**
   * Acquires the write lock only if it is not held by another thread at the time of invocation.
   *
   * <p>Acquires the write lock if the write lock is not held by another thread and returns
   * immediately with the value {@code true} if and only if no other thread is attempting a read
   * lock, setting the write lock {@code writerLoop} count to one.
   *
   * <p>If the current thread already holds this lock then the {@code reentrantWriterCount} count is
   * incremented by one and the method returns {@code true}.
   *
   * <p>If the write lock is held by another thread then this method will return immediately with
   * the value {@code false}.
   *
   * @return {@code true} if the write lock was free and was acquired by the current thread, or the
   *     write lock was already held by the current thread; and {@code false} otherwise.
   */
  public boolean exclusiveTryLock() {
    // Try to acquire the lock in write-mode
    if (stampedLock.tryWriteLock() == 0) {
      return false;
    }

    // We can only do this after writerOwner has been set to the current thread
    AtomicInteger[] localReadersStateArray = readersStateArrayRef.get();
    if (localReadersStateArray == null) {
      // Set to dummyArray before scanning the readersStateList to impose
      // a linearizability condition
      readersStateArrayRef.set(dummyArray);
      // Copy readersStateList to an array
      localReadersStateArray = readersStateList.toArray(new AtomicInteger[readersStateList.size()]);
      readersStateArrayRef.compareAndSet(dummyArray, localReadersStateArray);
    }

    // Scan the array of Reader states
    for (AtomicInteger readerState : localReadersStateArray) {
      if (readerState != null && readerState.get() == SRWL_STATE_READING) {
        // There is at least one ongoing Reader so give up
        stampedLock.asWriteLock().unlock();
        return false;
      }
    }

    return true;
  }

  /**
   * Acquires the write lock if it is not held by another thread within the given waiting time.
   *
   * <p>Acquires the write lock if the write lock is not held by another thread and returns
   * immediately with the value {@code true} if and only if no other thread is attempting a read
   * lock, setting the write lock {@code reentrantWriterCount} to one. If another thread is
   * attempting a read lock, this function <b>may yield until the read lock is released</b>.
   *
   * <p>If the current thread already holds this lock then the {@code reentrantWriterCount} is
   * incremented by one and the method returns {@code true}.
   *
   * <p>If the write lock is held by another thread then the current thread yields and lies dormant
   * until one of two things happens:
   *
   * <ul>
   *   <li>The write lock is acquired by the current thread; or
   *   <li>The specified waiting time elapses
   * </ul>
   *
   * <p>If the write lock is acquired then the value {@code true} is returned and the write lock
   * {@code reentrantWriterCount} is set to one.
   *
   * @param nanosTimeout the time to wait for the write lock in nanoseconds
   * @return {@code true} if the lock was free and was acquired by the current thread, or the write
   *     lock was already held by the current thread; and {@code false} if the waiting time elapsed
   *     before the lock could be acquired.
   */
  public boolean exclusiveTryLockNanos(long nanosTimeout) throws InterruptedException {
    final long lastTime = System.nanoTime();
    // Try to acquire the lock in write-mode
    if (stampedLock.tryWriteLock(nanosTimeout, TimeUnit.NANOSECONDS) == 0) {
      return false;
    }

    // We can only do this after writerOwner has been set to the current thread
    AtomicInteger[] localReadersStateArray = readersStateArrayRef.get();
    if (localReadersStateArray == null) {
      // Set to dummyArray before scanning the readersStateList to impose
      // a linearizability condition
      readersStateArrayRef.set(dummyArray);
      // Copy readersStateList to an array
      localReadersStateArray = readersStateList.toArray(new AtomicInteger[readersStateList.size()]);
      readersStateArrayRef.compareAndSet(dummyArray, localReadersStateArray);
    }

    // Scan the array of Reader states
    for (AtomicInteger readerState : localReadersStateArray) {
      while (readerState != null && readerState.get() == SRWL_STATE_READING) {
        if (System.nanoTime() - lastTime < nanosTimeout) {
          Thread.yield();
        } else {
          // Time has expired and there is at least one ongoing Reader so give up
          stampedLock.asWriteLock().unlock();
          return false;
        }
      }
    }

    return true;
  }
}
