/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.concur.resource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.concur.OTimeoutException;

/**
 * Shared resource. Sub classes can acquire and release shared and exclusive locks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSharedResourceTimeout {
	protected final ReadWriteLock	lock	= new ReentrantReadWriteLock();
	protected int									timeout;

	public OSharedResourceTimeout(final int timeout) {
		this.timeout = timeout;
	}

	protected void acquireSharedLock() throws OTimeoutException {
		try {
			if (lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS))
				// OK
				return;
		} catch (InterruptedException e) {
		}
		throw new OTimeoutException("Timeout on acquiring shared lock against resource: " + this);
	}

	protected void releaseSharedLock() {
		lock.readLock().unlock();
	}

	protected void acquireExclusiveLock() throws OTimeoutException {
		try {
			if (lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS))
				// OK
				return;
		} catch (InterruptedException e) {
		}
		throw new OTimeoutException("Timeout on acquiring exclusive lock against resource: " + this);
	}

	protected void releaseExclusiveLock() {
		lock.writeLock().unlock();
	}
}
