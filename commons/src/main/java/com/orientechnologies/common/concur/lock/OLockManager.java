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

import com.orientechnologies.common.profiler.OProfiler;

/**
 * Manages the locks at record level.
 * <p>
 * For each acquired lock, a {@link LockEntry} or a {@link OSharedLockEntry} is created and is inserted in the right place depending
 * of the situation:
 * <ul>
 * 
 * <li>When the resource is not already locked, a {@link LockEntry} is created and added to the main linked list that references all
 * the resources currrently locked (list managed by {@link OLockManager#firstLiveLock} and {@link LockEntry#nextLiveLock}. The locks
 * referenced in this main list are in {@link LOCK_STATUS#LIVE} status.
 * 
 * <li>When a requester ask for a shared lock and the resource is already shared locked by an other requester, a
 * {@link OSharedLockEntry} is created and appended to the {@link OSharedLockEntry#nextSharedLock} list.
 * 
 * <li>When a requester want to lock a resource but is already incompatibly locked by an other requester (shared + exclusive or
 * exclusive + exclusive), the {@link LockEntry} created is appended a waiting stack for this resource :
 * {@link LockEntry#nextWaiter}. This created lock is first set in the {@link LOCK_STATUS#WILLWAIT} status and will be set in
 * {@link LOCK_STATUS#WAITING} status. If the timeout is reached, and the lock is always held by an other requester, the lock will
 * be set in the {@link LOCK_STATUS#EXPIRED} status.
 * 
 * <li>When a requester ask for a shared or exclusive lock in a recursive manner on the same resource (reentrancy), the counters
 * {@link OSharedLockEntry#countSharedLocks} and {@link LockEntry#countExclLocks} are incremented. In this special case, no
 * {@link LockEntry} nor {@link OSharedLockEntry} are created.
 * 
 * </ul>
 * 
 * <p>
 * On release, the resource live lock is found from the main list and the counter is decremented (
 * {@link OSharedLockEntry#countSharedLocks} or {@link LockEntry#countExclLocks}). If these 2 counters are 0, then this lock is
 * removed from the main list and:
 * <ul>
 * <li>if a sharedLock is setted ({@link OSharedLockEntry#nextSharedLock} not null) this shared lock is promoted as the live lock.
 * <li>else if a waiting lock is setted ({@link OSharedLockEntry#nextSharedLock} no null) this waiting lock is awaited and setted as
 * live lock.
 * </ul>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Sylvain Spinelli (sylvain.spinelli@kelis.fr)
 */
public class OLockManager<RESOURCE_TYPE, REQUESTER_TYPE> {
	public enum LOCK {
		SHARED, EXCLUSIVE
	}

	/** Statuses for {@link LockEntry}. */
	protected enum LOCK_STATUS {
		/**
		 * This lock is in the header position. This lock is referenced by the main linked list {@link OLockManager#firstLock} or
		 * {@link LockEntry#nextLockEntry}. Only one lock can be LIVE for the same resource.
		 */
		LIVE,

		/**
		 * This lock will wait for the release of the header lock : This lock is referenced by {@link LockEntry#nextWaiter}. This status
		 * is a short time transition status, it will quickly setted as {@link #SLEEPING}.
		 */
		WILLWAIT,

		/** The thread asking for this lock is waiting for a notify() call. */
		WAITING,

		/** The lock has failed because the timeout is expired. */
		EXPIRED
	}

	private static final int														DEFAULT_ACQUIRE_TIMEOUT	= 5000;

	protected final long																acquireTimeout					= DEFAULT_ACQUIRE_TIMEOUT;	// MS

	/**
	 * First entry for the linked list of resources currently locked. Next entries of this list is managed by
	 * {@link LockEntry#nextLiveLock}.
	 */
	protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE>	firstLiveLock;

	/**
	 * Manages the lock for one resource.
	 * 
	 */
	public static class LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> extends OSharedLockEntry<REQUESTER_TYPE> {

		/** The resource currently locked. */
		protected RESOURCE_TYPE															resource;

		/** Lock status. */
		protected volatile LOCK_STATUS											status;

		/**
		 * Count exclusive locks held by this requester for this resource.
		 * <p>
		 * Used for reentrancy : when the same requester acquire a exclusive lock multiple times for the same resource in a nested code.
		 */
		protected int																				countExclLocks;

		/**
		 * Linked list for waiters : these locks for the same resource are held by other requesters and are waiting for the release of
		 * the header lock.
		 */
		protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE>	nextWaiter;

		/**
		 * Next resource currently locked.
		 * 
		 * @see OLockManager#firstLiveLock
		 */
		protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE>	nextLiveLock;

		public LockEntry(LOCK iLockType, REQUESTER_TYPE iRequester, RESOURCE_TYPE iResource, LOCK_STATUS iStatus) {
			super();
			requester = iRequester;
			status = iStatus;
			if (iLockType == LOCK.EXCLUSIVE) {
				countExclLocks = 1;
			} else {
				countSharedLocks = 1;
			}
			resource = iResource;
		}
	}

	public OLockManager() {
	}

	public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType) {
		acquireLock(iRequester, iResourceId, iLockType, acquireTimeout);
	}

	public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType, long iTimeout) {

		// System.out.println("Acquire " + iLockType + " lock for " + iResourceId + " by " + iRequester);

		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> lock = tryToAcquireLock(iRequester, iResourceId, iLockType);
		if (lock.status == LOCK_STATUS.LIVE)
			return;

		// PUT CURRENT THREAD IN WAIT UNTIL TIMEOUT OR UNLOCK BY ANOTHER THREAD THAT UNLOCK THE RESOURCE
		synchronized (lock) {
			lock.status = LOCK_STATUS.WAITING;
			long time = iTimeout > 0 ? System.currentTimeMillis() : 0;
			do {
				// DO...WHILE FOR SPURIOUS WAKEUP
				try {
					lock.wait(iTimeout <= 0 ? Long.MAX_VALUE : iTimeout);
				} catch (InterruptedException e) {
				}
			} while (lock.status == LOCK_STATUS.WAITING && (iTimeout > 0 && time + iTimeout > System.currentTimeMillis()));
			if (lock.status == LOCK_STATUS.WAITING) {
				// THE RESOURCE IS LOCKED.
				lock.status = LOCK_STATUS.EXPIRED;
				throw new OLockException("Resource " + iResourceId + " is locked");
			}
		}

	}

	public synchronized void releaseLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType)
			throws OLockException {
		// System.out.println("Release " + iLockType + " lock for " + iResourceId + " by " + iRequester);
		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> currentLock = removeLockEntry(iResourceId);
		if (currentLock == null)
			throw new OLockException("Error on releasing a non acquired lock by the requester " + iRequester + " on resource: "
					+ iResourceId);
		try {
			if (iLockType == LOCK.SHARED) {
				removeSharedLockEntry(currentLock, iRequester);
			} else {
				if (!iRequester.equals(currentLock.requester))
					throw new OLockException("Error on releasing a non acquired lock by the requester " + iRequester + " on resource: "
							+ iResourceId);
				currentLock.countExclLocks--;
			}
		} catch (OLockException e) {
			// IN CASE Of REENTRANCY, SEARCH A EXPIRED LOCK FOR THIS REQUESTER IN ORDER
			// TO NOT OVERRIDE THE ORIGINAL EXCEPTION BY A BAD ONE.
			while (currentLock != null) {
				if (currentLock.requester.equals(iRequester) && currentLock.status == LOCK_STATUS.EXPIRED)
					return;
				currentLock = currentLock.nextWaiter;
			}
			// EXPIRED LOCK FOR THIS REQUESTER NOT FOUND
			throw e;
		}
		if (currentLock.nextSharedLock != null || currentLock.countExclLocks > 0 || currentLock.countSharedLocks > 0) {
			// THIS LOCK IS STILL IN RUN
			addEntry(currentLock);
		} else {
			// THIS LOCK IS EXPIRED
			LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> nextWaiter = currentLock.nextWaiter;
			if (nextWaiter != null) {
				// PUT NEXT WAITER ON TOP
				boolean wait = false;
				waiting: while (true) {
					if (wait)
						Thread.yield();
					synchronized (nextWaiter) {
						switch (nextWaiter.status) {
						case WILLWAIT:
							// THE nextWaiter WILL BE IN WAITING STATUS VERY QUICKLY, WE TRY AGAIN.
							wait = true;
							continue waiting;
						case WAITING:
							// THE nextWaiter IS WAITING : WE CAN SET IT AS THE LIVE LOCK.
							addEntry(nextWaiter);
							nextWaiter.status = LOCK_STATUS.LIVE;
							// WAKEUP nextWaiter THREAD
							nextWaiter.notify();
							break waiting;
						case LIVE:
							//
							System.out.println("OLockManager LIVE status should nether occured here...");
							addEntry(nextWaiter);
							break waiting;
						case EXPIRED:
							if (nextWaiter.nextWaiter != null) {
								wait = false;
								nextWaiter = nextWaiter.nextWaiter;
								continue waiting;
							} else {
								break waiting;
							}
						}
					}
				}
			}
		}
	}

	protected synchronized LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> tryToAcquireLock(final REQUESTER_TYPE iRequester,
			final RESOURCE_TYPE iResourceId, final LOCK iLockType) {
		OProfiler.getInstance().updateCounter("LockMgr.tryToAcquire", +1);

		// System.out.println("Try acquire " + iLockType + " lock for " + iResourceId + " by " + iRequester);

		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> currentLock = lookForEntry(iResourceId);
		if (currentLock == null)
			return addEntry(new LockEntry<RESOURCE_TYPE, REQUESTER_TYPE>(iLockType, iRequester, iResourceId, LOCK_STATUS.LIVE));

		OSharedLockEntry<REQUESTER_TYPE> curentSharedLock = currentLock;
		do {
			if (curentSharedLock.requester.equals(iRequester)) {
				// REENTRANCE
				if (iLockType == LOCK.EXCLUSIVE) {
					// ASK FOR EXCLUSIVE LOCK BY THE SAME REQUESTER
					if (currentLock.nextSharedLock == null) {
						// NO OTHER REQUESTER, USE THIS LOCK
						currentLock.countExclLocks++;
						return currentLock;
					} else {
						// NEED TO WAIT
						return addWaitingEntry(currentLock, curentSharedLock, iLockType, iRequester, iResourceId);
					}
				} else {
					// ASK FOR SHARED LOCK BY THE SAME REQUESTER : OK
					curentSharedLock.countSharedLocks++;
					return currentLock;
				}
			}
			curentSharedLock = curentSharedLock.nextSharedLock;
		} while (curentSharedLock != null);

		if (currentLock.countExclLocks > 0) {
			// NEED TO WAIT
			return addWaitingEntry(currentLock, null, iLockType, iRequester, iResourceId);
		} else {
			if (iLockType == LOCK.SHARED) {
				addSharedLockEntry(currentLock, iRequester);
				return currentLock;
			} else {
				// EXCLUSIVE LOCK : NEED TO WAIT
				return addWaitingEntry(currentLock, null, iLockType, iRequester, iResourceId);
			}
		}

	}

	public synchronized void clear() {
		firstLiveLock = null;
	}

	protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> lookForEntry(RESOURCE_TYPE iRes) {
		assert (Thread.holdsLock(this));
		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> nextLock = firstLiveLock;
		while (nextLock != null) {
			if (nextLock.resource.equals(iRes))
				return nextLock;
			nextLock = nextLock.nextLiveLock;
		}
		return null;
	}

	protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> addEntry(LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> iLockEntry) {
		assert (Thread.holdsLock(this));
		iLockEntry.nextLiveLock = firstLiveLock;
		firstLiveLock = iLockEntry;
		return iLockEntry;
	}

	protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> addWaitingEntry(LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> iCurrentLock,
			OSharedLockEntry<REQUESTER_TYPE> iCurrentSharedLock, LOCK iLockType, REQUESTER_TYPE iRequester, RESOURCE_TYPE iResource) {
		assert (Thread.holdsLock(this));
		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> newEntry = new LockEntry<RESOURCE_TYPE, REQUESTER_TYPE>(iLockType, iRequester,
				iResource, LOCK_STATUS.WILLWAIT);
		// APPEND THIS NEW ENTRY TO THE STACK
		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> lastWaiter = iCurrentLock;
		while (lastWaiter.nextWaiter != null)
			lastWaiter = lastWaiter.nextWaiter;
		lastWaiter.nextWaiter = newEntry;
		// MOVE ALL SHARED LOCKS FROM THE SAME REQUESTER TO THIS NEW WAITING ENTRY
		if (iCurrentSharedLock != null) {
			newEntry.countSharedLocks += iCurrentSharedLock.countSharedLocks;
			iCurrentSharedLock.countSharedLocks = 1;
			removeSharedLockEntry(iCurrentLock, iCurrentSharedLock.requester);
		}
		return newEntry;
	}

	protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> addSharedLockEntry(LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> iRoot,
			REQUESTER_TYPE iRequester) {
		assert (Thread.holdsLock(this));
		OSharedLockEntry<REQUESTER_TYPE> newLock = new OSharedLockEntry<REQUESTER_TYPE>(iRequester);
		newLock.nextSharedLock = iRoot.nextSharedLock;
		iRoot.nextSharedLock = newLock;
		return iRoot;
	}

	protected LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> removeLockEntry(RESOURCE_TYPE iRes) {
		assert (Thread.holdsLock(this));
		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> previousLock = null;
		LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> nextLock = firstLiveLock;
		while (nextLock != null) {
			if (nextLock.resource.equals(iRes)) {
				if (previousLock == null) {
					firstLiveLock = nextLock.nextLiveLock;
				} else {
					previousLock.nextLiveLock = nextLock.nextLiveLock;
				}
				nextLock.nextLiveLock = null;
				return nextLock;
			}
			previousLock = nextLock;
			nextLock = nextLock.nextLiveLock;
		}
		return null;
	}

	protected void removeSharedLockEntry(LockEntry<RESOURCE_TYPE, REQUESTER_TYPE> iRoot, REQUESTER_TYPE iRequester) {
		assert (Thread.holdsLock(this));
		OSharedLockEntry<REQUESTER_TYPE> previous = null;
		OSharedLockEntry<REQUESTER_TYPE> next = iRoot;
		while (!iRequester.equals(next.requester)) {
			previous = next;
			next = next.nextSharedLock;
			if (next == null) {
				throw new OLockException("Error on releasing a non acquired lock by the requester " + iRequester + " on resource: "
						+ iRoot.resource);
			}
		}

		if (next == iRoot) {
			iRoot.countSharedLocks--;
			if (next.nextSharedLock != null && iRoot.countExclLocks == 0 && iRoot.countSharedLocks == 0) {
				iRoot.requester = iRoot.nextSharedLock.requester;
				iRoot.countSharedLocks = iRoot.nextSharedLock.countSharedLocks;
				iRoot.nextSharedLock = next.nextSharedLock.nextSharedLock;
			}
		} else {
			if (next.countSharedLocks == 1) {
				previous.nextSharedLock = next.nextSharedLock;
			} else {
				next.countSharedLocks--;
			}
		}
	}
}
