/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
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
package com.orientechnologies.orient.core.storage.cache.chm.readbuffer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * A striped, non-blocking, bounded buffer.
 *
 * @param <E> the type of elements maintained by this buffer
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class BoundedBuffer<E> extends StripedBuffer<E> {
  /*
   * A circular ring buffer stores the elements being transferred by the producers to the consumer.
   * The monotonically increasing count of reads and writes allow indexing sequentially to the next
   * element location based upon a power-of-two sizing.
   *
   * The producers race to read the counts, check if there is available capacity, and if so then try
   * once to CAS to the next write count. If the increment is successful then the producer lazily
   * publishes the element. The producer does not retry or block when unsuccessful due to a failed
   * CAS or the buffer being full.
   *
   * The consumer reads the counts and takes the available elements. The clearing of the elements
   * and the next read count are lazily set.
   *
   * This implementation is striped to further increase concurrency by rehashing and dynamically
   * adding new buffers when contention is detected, up to an internal maximum. When rehashing in
   * order to discover an available buffer, the producer may retry adding its element to determine
   * whether it found a satisfactory buffer or if resizing is necessary.
   */

  /** The maximum number of elements per buffer. */
  private static final int BUFFER_SIZE = 128;

  // Assume 4-byte references and 64-byte cache line (16 elements per line)
  private static final int SPACED_SIZE = BUFFER_SIZE << 4;
  private static final int SPACED_MASK = SPACED_SIZE - 1;
  private static final int OFFSET = 16;

  @Override
  protected Buffer<E> create(final E e) {
    return new RingBuffer<>(e);
  }

  static final class RingBuffer<E> implements Buffer<E> {
    private final AtomicReferenceArray<E> buffer;
    private final AtomicLong readCounter = new AtomicLong();
    private final AtomicLong writeCounter = new AtomicLong();

    @SuppressWarnings({"cast"})
    private RingBuffer(final E e) {
      buffer = new AtomicReferenceArray<>(SPACED_SIZE);
      buffer.lazySet(0, e);
    }

    @Override
    public int offer(final E e) {
      final long head = readCounter.get();
      final long tail = writeCounter.get();
      final long size = (tail - head);
      if (size >= SPACED_SIZE) {
        return Buffer.FULL;
      }
      if (writeCounter.compareAndSet(tail, tail + OFFSET)) {
        final int index = (int) (tail & SPACED_MASK);
        buffer.lazySet(index, e);
        return Buffer.SUCCESS;
      }
      return Buffer.FAILED;
    }

    @Override
    public void drainTo(final Consumer<E> consumer) {
      long head = readCounter.get();
      final long tail = writeCounter.get();

      final long size = (tail - head);
      if (size == 0) {
        return;
      }
      do {
        final int index = (int) (head & SPACED_MASK);
        final E e = buffer.get(index);
        if (e == null) {
          // not published yet
          break;
        }
        buffer.lazySet(index, null);
        consumer.accept(e);
        head += OFFSET;
      } while (head != tail);

      readCounter.lazySet(head);
    }

    @Override
    public int reads() {
      return (int) readCounter.get() / OFFSET;
    }

    @Override
    public int writes() {
      return (int) writeCounter.get() / OFFSET;
    }
  }
}
