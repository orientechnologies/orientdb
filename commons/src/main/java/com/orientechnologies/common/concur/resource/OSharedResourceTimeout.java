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

/**
 * Shared resource. Sub classes can acquire and release shared and exclusive locks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSharedResourceTimeout {
	protected ReadWriteLock	lock	= new ReentrantReadWriteLock();
	protected int						timeout;

	public OSharedResourceTimeout(final int timeout) {
		this.timeout = timeout;
	}

	protected void acquireSharedLock() throws InterruptedException {
		if (!lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS))
			throw new InterruptedException("Timeout on acquiring shared resource");
	}

	protected void releaseSharedLock() {
		lock.readLock().unlock();
	}

	protected void acquireExclusiveLock() throws InterruptedException {
		if (!lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS))
			throw new InterruptedException("Timeout on acquiring shared resource");
	}

	protected void releaseExclusiveLock() {
		lock.writeLock().unlock();
	}
}
