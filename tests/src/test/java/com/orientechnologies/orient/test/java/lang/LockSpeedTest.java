/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.test.java.lang;

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.resource.OSharedResourceExternal;

public class LockSpeedTest {
  private static final long MAX = 10000000;

  public static final void main(String[] args) {

    OSharedResourceExternal lock = new OSharedResourceExternal();
    OModificationLock storageLock = new OModificationLock();

    long timer = System.currentTimeMillis();
    for (int i = 0; i < MAX; ++i) {
    }
    final long fixed = System.currentTimeMillis() - timer;

    for (int i = 0; i < MAX; ++i) {
      lock.acquireSharedLock();
      lock.releaseSharedLock();
    }

    System.out.println("Read Locks: " + (System.currentTimeMillis() - timer - fixed));
    timer = System.currentTimeMillis();

    for (int i = 0; i < MAX; ++i) {
      lock.acquireExclusiveLock();
      lock.releaseExclusiveLock();
    }

    System.out.println("Write Locks: " + (System.currentTimeMillis() - timer - fixed));
    timer = System.currentTimeMillis();

    for (int i = 0; i < MAX; ++i) {
      synchronized (lock) {
      }
    }
    System.out.println("Simple Locks: " + (System.currentTimeMillis() - timer - fixed));

    timer = System.currentTimeMillis();

    for (int i = 0; i < MAX; ++i) {
      storageLock.requestModificationLock();
      storageLock.releaseModificationLock();
    }

    System.out.println("Storage Locks: " + (System.currentTimeMillis() - timer - fixed));
  }
}
