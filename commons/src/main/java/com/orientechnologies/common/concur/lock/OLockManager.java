/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OLockManager<RESOURCE_TYPE, REQUESTER_TYPE> {
	public enum LOCK {
		SHARED, EXCLUSIVE
	}

	protected long																	acquireTimeout;
	protected HashMap<RESOURCE_TYPE, CountableLock>	map	= new HashMap<RESOURCE_TYPE, CountableLock>();

	@SuppressWarnings("serial")
	protected static class CountableLock extends ReentrantReadWriteLock {
		protected int	countLocks	= 0;

		public CountableLock(final boolean iFair) {
			super(false);
		}
	}

	public OLockManager(final int iAcquireTimeout) {
		acquireTimeout = iAcquireTimeout;
	}

	public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType) {
		acquireLock(iRequester, iResourceId, iLockType, acquireTimeout);
	}

	public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType, long iTimeout) {
		CountableLock lock;
		synchronized (map) {
			lock = map.get(iResourceId);
			if (lock == null) {
				lock = new CountableLock(iTimeout > 0);
				map.put(iResourceId, lock);
			}
			lock.countLocks++;
		}
		try {
			if (iTimeout <= 0) {
				if (iLockType == LOCK.SHARED)
					lock.readLock().lock();
				else
					lock.writeLock().lock();
			} else {
				try {
					if (iLockType == LOCK.SHARED) {
						if (!lock.readLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
							throw new OLockException("Timeout on acquiring resource '" + iResourceId + "' because is locked from another thread");
					} else {
						if (!lock.writeLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
							throw new OLockException("Timeout on acquiring resource '" + iResourceId + "' because is locked from another thread");
					}
				} catch (InterruptedException e) {
					throw new OLockException("Thread interrupted while waiting for resource '" + iResourceId + "'");
				}
			}
		} catch (RuntimeException e) {
			synchronized (map) {
				lock.countLocks--;
				if (lock.countLocks == 0)
					map.remove(iResourceId);
			}
			throw e;
		}

	}

	public void releaseLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType)
			throws OLockException {
		final CountableLock lock;
		synchronized (map) {
			lock = map.get(iResourceId);
			if (lock == null)
				throw new OLockException("Error on releasing a non acquired lock by the requester '" + iRequester
						+ "' against the resource: '" + iResourceId + "'");

			lock.countLocks--;
			if (lock.countLocks == 0)
				map.remove(iResourceId);
		}
		if (iLockType == LOCK.SHARED)
			lock.readLock().unlock();
		else
			lock.writeLock().unlock();

	}

	public void clear() {
		synchronized (map) {
			map.clear();
		}
	}

	public void setAcquireTimeout(long iAcquireTimeout) {
		acquireTimeout = iAcquireTimeout;
	}

	// For tests purposes.
	public int getCountCurrentLocks() {
		synchronized (map) {
			return map.size();
		}
	}

}
