/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.common.concur.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This lock is intended to be used inside of storage to request lock on any data modifications.
 * 
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @author Sylvain Spinelli <a href="mailto:sylvain.spinelli@gmail.com">Sylvain Spinelli</a>
 * @since 15.06.12
 */
public class OModificationLock extends ReentrantReadWriteLock {

  protected volatile boolean throwException;

  /**
   * Tells the lock that thread is going to perform data modifications in storage. This method allows to perform several data
   * modifications in parallel.
   */
  public void requestModificationLock() {
    if (isWriteLockedByCurrentThread())
      return;
    if (throwException) {
      if (!readLock().tryLock()) {
        throw new OModificationOperationProhibitedException();
      }
    } else {
      readLock().lock();
    }
  }

  /**
   * Tells the lock that thread is finished to perform modifications in storage.
   */
  public void releaseModificationLock() {
    if (isWriteLockedByCurrentThread())
      return;
    readLock().unlock();
  }

  /**
   * After this method finished it's execution, all threads that are going to perform data modifications in storage should wait till
   * {@link #allowModifications()} method will be called. This method will wait till all ongoing modifications will be finished.
   */
  public void prohibitModifications() {
    writeLock().lock();
  }

  /**
   * After this method finished it's execution, all threads that are going to perform data modifications in storage should wait till
   * {@link #allowModifications()} method will be called. This method will wait till all ongoing modifications will be finished.
   * 
   * @param iThrowException
   *          If <code>true</code> {@link OModificationOperationProhibitedException} exception will be thrown on
   *          {@link #requestModificationLock()} call.
   */

  public void prohibitModifications(boolean iThrowException) {
    throwException = iThrowException;
    writeLock().lock();
  }

  /**
   * After this method finished execution all threads that are waiting to perform data modifications in storage will be awaken and
   * will be allowed to continue their execution.
   */
  public void allowModifications() {
    throwException = false;
    writeLock().unlock();
  }

}
