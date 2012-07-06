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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This lock is intended to be used inside of storage to request lock on any data modifications.
 * Writes can be prohibited from one thread, but then allowed from other thread.
 *
 * IMPORTANT ! Prohibit/allow changes methods are not reentrant.
 *
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 15.06.12
 */
public class OModificationLock {
	private volatile boolean veto = false;
	private volatile boolean throwException = false;

	private final ConcurrentLinkedQueue<Thread> waiters = new ConcurrentLinkedQueue<Thread>();
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Tells the lock that thread is going to perform data modifications in storage.
	 * This method allows to perform several data modifications in parallel.
	 */
	public void requestModificationLock() {
		lock.readLock().lock();
		if(!veto)
			return;

		if(throwException) {
			lock.readLock().unlock();
			throw new OModificationOperationProhibitedException("Modification requests are prohibited");
		}

		boolean wasInterrupted = false;
		Thread thread = Thread.currentThread();
		waiters.add(thread);

		while(veto) {
			LockSupport.park(this);
			if (Thread.interrupted())
			   wasInterrupted = true;
		}

		waiters.remove(thread);
		if(wasInterrupted)
			thread.interrupt();
	}

	/**
	 * Tells the lock that thread is finished to perform to perform modifications in storage.
	 */
	public void releaseModificationLock() {
		lock.readLock().unlock();
	}

	/**
	 * After this method finished it's execution, all threads that are going to perform data modifications in storage
	 * should wait till {@link #allowModifications()} method will be called.
	 * This method will wait till all ongoing modifications will be finished.
	 */
	public void prohibitModifications() {
		lock.writeLock().lock();
		try {
			throwException = false;
			veto = true;
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * After this method finished it's execution, all threads that are going to perform data modifications in storage
	 * should wait till {@link #allowModifications()} method will be called.
	 * This method will wait till all ongoing modifications will be finished.
	 *
	 * @param throwException If <code>true</code> {@link OModificationOperationProhibitedException} exception will be thrown
	 *                       on  {@link #requestModificationLock()} call.
	 */

	public void prohibitModifications(boolean throwException) {
		lock.writeLock().lock();
		try {
			this.throwException = throwException;
			veto = true;
		} finally {
			lock.writeLock().unlock();
		}
	}


	/**
	 * After this method finished execution all threads that are waiting to perform data modifications in storage
	 * will be awaken and will be allowed to continue their execution.
	 */
	public void allowModifications() {
    veto = false;
		
		for(Thread thread : waiters)
			LockSupport.unpark(thread);
	}

}