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

import java.util.function.Consumer;

/**
 * A multiple-producer / single-consumer buffer that rejects new elements if it is full or fails
 * spuriously due to contention. Unlike a queue and stack, a buffer does not guarantee an ordering
 * of elements in either FIFO or LIFO order. Beware that it is the responsibility of the caller to
 * ensure that a consumer has exclusive read access to the buffer. This implementation does
 * <em>not</em> include fail-fast behavior to guard against incorrect consumer usage.
 *
 * @param <E> the type of elements maintained by this buffer
 * @author ben.manes@gmail.com (Ben Manes)
 */
public interface Buffer<E> {
  int FULL = 1;
  int SUCCESS = 0;
  int FAILED = -1;

  /**
   * Inserts the specified element into this buffer if it is possible to do so immediately without
   * violating capacity restrictions. The addition is allowed to fail spuriously if multiple threads
   * insert concurrently.
   *
   * @param e the element to add
   * @return {@code 1} if the buffer is full, {@code -1} if the CAS failed, or {@code 0} if added
   */
  int offer(E e);

  /**
   * Drains the buffer, sending each element to the consumer for processing. The caller must ensure
   * that a consumer has exclusive read access to the buffer.
   *
   * @param consumer the action to perform on each element
   */
  void drainTo(Consumer<E> consumer);

  /**
   * Returns the number of elements residing in the buffer.
   *
   * @return the number of elements in this buffer
   */
  default int size() {
    return writes() - reads();
  }

  /**
   * Returns the number of elements that have been read from the buffer.
   *
   * @return the number of elements read from this buffer
   */
  int reads();

  /**
   * Returns the number of elements that have been written to the buffer.
   *
   * @return the number of elements written to this buffer
   */
  int writes();
}
