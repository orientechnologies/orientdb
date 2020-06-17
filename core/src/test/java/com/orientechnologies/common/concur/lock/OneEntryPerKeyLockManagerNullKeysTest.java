/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.common.concur.lock;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import org.junit.Before;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class OneEntryPerKeyLockManagerNullKeysTest {

  private OOneEntryPerKeyLockManager<String> manager;

  @Before
  public void before() {
    manager = new OOneEntryPerKeyLockManager<String>(true, -1, 100);
  }

  @Test
  public void testNullKeysInCollectionBatch() {
    final List<String> keys = new ArrayList<String>();
    keys.add(null);
    keys.add("key");
    keys.add(null);

    final Lock[] locks = manager.acquireExclusiveLocksInBatch(keys);
    assertEquals(keys.size(), locks.length);
    assertEquals(2, wrapper(locks[0]).getLockCount());
    assertEquals(2, wrapper(locks[1]).getLockCount());
    assertEquals(1, wrapper(locks[2]).getLockCount());

    for (Lock lock : locks) lock.unlock();
    assertEquals(0, wrapper(locks[0]).getLockCount());
    assertEquals(0, wrapper(locks[1]).getLockCount());
    assertEquals(0, wrapper(locks[2]).getLockCount());
  }

  @Test
  public void testNullKeysInArrayBatch() {
    final String[] keys = new String[] {null, "key", null};

    final Lock[] locks = manager.acquireExclusiveLocksInBatch(keys);
    assertEquals(keys.length, locks.length);
    assertEquals(2, wrapper(locks[0]).getLockCount());
    assertEquals(2, wrapper(locks[1]).getLockCount());
    assertEquals(1, wrapper(locks[2]).getLockCount());

    for (Lock lock : locks) lock.unlock();
    assertEquals(0, wrapper(locks[0]).getLockCount());
    assertEquals(0, wrapper(locks[1]).getLockCount());
    assertEquals(0, wrapper(locks[2]).getLockCount());
  }

  @Test
  public void testNullKeyExclusive() {
    manager.acquireExclusiveLock(null);
    final Lock lock = manager.acquireExclusiveLock(null);
    assertEquals(2, wrapper(lock).getLockCount());
    lock.unlock();
    assertEquals(1, wrapper(lock).getLockCount());
    manager.releaseExclusiveLock(null);
    assertEquals(0, wrapper(lock).getLockCount());
  }

  @Test
  public void testNullKeyShared() {
    manager.acquireSharedLock(null);
    final Lock lock = manager.acquireSharedLock(null);
    assertEquals(2, wrapper(lock).getLockCount());
    lock.unlock();
    assertEquals(1, wrapper(lock).getLockCount());
    manager.releaseSharedLock(null);
    assertEquals(0, wrapper(lock).getLockCount());
  }

  private static OOneEntryPerKeyLockManager.CountableLockWrapper wrapper(Lock lock) {
    return (OOneEntryPerKeyLockManager.CountableLockWrapper) lock;
  }
}
