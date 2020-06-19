/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.cloudbees.syslog.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Maintains a cached version of the {@code Object} that it holds and handle the renewal of this
 * object upon expiration.
 *
 * <p>Greatly inspired by the {@code CachedData} sample provided in the javadoc of {@link
 * java.util.concurrent.locks.ReentrantReadWriteLock}.
 *
 * <p>{@code Object} is created implementing the {@link #newObject()} method.
 *
 * <p>Sample to get an {@code InetAddress} refreshed against a DNS every 10 seconds:
 *
 * <pre><code>
 * CachingReference myRemoteServerAddress = new CachingReference&lt;InetAddress&gt;(10, TimeUnit.SECONDS) {
 *     protected InetAddress newObject() {
 *         try {
 *             return InetAddress.getByName(myRemoteServerHostname);
 *         } catch () {
 *             throw new RuntimeException("Exception resolving '" + myRemoteServerHostname + "'", e);
 *         }
 *     }
 * }
 * </code></pre>
 *
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public abstract class CachingReference<E> {
  private final ReadWriteLock rwl = new ReentrantReadWriteLock();
  private long lastCreationInNanos;
  private long timeToLiveInNanos;
  private E object;

  public CachingReference(long timeToLiveInNanos) {
    this.timeToLiveInNanos = timeToLiveInNanos;
  }

  public CachingReference(long timeToLive, TimeUnit timeToLiveUnit) {
    this(TimeUnit.NANOSECONDS.convert(timeToLive, timeToLiveUnit));
  }

  /** @return the newly created object. */
  protected abstract E newObject();

  /** @return the up to date version of the {@code Object} hold by this reference. */
  public E get() {
    rwl.readLock().lock();
    try {
      if ((System.nanoTime() - lastCreationInNanos) > timeToLiveInNanos) {
        // Must release read lock before acquiring write lock
        rwl.readLock().unlock();
        rwl.writeLock().lock();
        try {
          // Recheck state because another thread might have
          // acquired write lock and changed state before we did.
          if ((System.nanoTime() - lastCreationInNanos) > timeToLiveInNanos) {
            object = newObject();
            lastCreationInNanos = System.nanoTime();
          }
        } finally {
          // Downgrade by acquiring read lock before releasing write lock
          rwl.readLock().lock();
          rwl.writeLock().unlock();
        }
      }
      return object;
    } finally {
      rwl.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return "CachingReference[" + this.object + "]";
  }
}
