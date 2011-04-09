package com.orientechnologies.common.concur.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.profiler.OProfiler;

/**
 * Manages the locks at record level. To optimize speed and space in memory the shared locks map holds the requester threads
 * directly if only one thread is locking the resource. When multiple threads acquire the same resource, then a List is put in place
 * of the single object.<br/>
 * On lock removing the list is maintained even if the client remains only one because the cost to replace the List to the object
 * directly is higher then just remove the item and the probability to add another again is high.
 * <p>
 * Separate Maps are managed for exclusive and shared locks.
 * </p>
 */
@SuppressWarnings("unchecked")
public class OLockManager<RESOURCE_TYPE, REQUESTER_TYPE> {
	public enum LOCK {
		SHARED, EXCLUSIVE
	}

	private static final int														DEFAULT_ACQUIRE_TIMEOUT				= 5000;
	private static final int														DEFAULT_CONCURRENCY_LEVEL			= 3;

	protected final OLockQueue<RESOURCE_TYPE>						lockQueue											= new OLockQueue<RESOURCE_TYPE>();
	protected final Map<RESOURCE_TYPE, Object>					sharedLocks										= new HashMap<RESOURCE_TYPE, Object>();
	protected final Map<RESOURCE_TYPE, REQUESTER_TYPE>	exclusiveLocks								= new HashMap<RESOURCE_TYPE, REQUESTER_TYPE>();
	protected int																				concurrencyLevel							= DEFAULT_CONCURRENCY_LEVEL;
	protected boolean																		downsizeSharedLockRetainList	= true;
	protected final long																acquireTimeout								= DEFAULT_ACQUIRE_TIMEOUT;											// MS

	public OLockManager() {
	}

	public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType) {
		acquireLock(iRequester, iResourceId, iLockType, acquireTimeout);
	}

	public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType,
			final long iTimeout) {
		while (!tryToAcquireLock(iRequester, iResourceId, iLockType))
			lockQueue.waitForResource(iResourceId, iTimeout);

		// if (tryToAcquireLock(iRequester, iResourceId, iLockType))
		// return;
		//
		// // PUT CURRENT THREAD IN WAIT UNTIL TIMEOUT OR UNLOCK BY ANOTHER THREAD THAT UNLOCK THE RESOURCE
		// if (!lockQueue.waitForResource(iResourceId, iTimeout))
		// // TIMEOUT EXPIRED
		// throw new OLockException("Resource " + iResourceId + " is locked");
		//
		// // THREAD UNLOCKED: TRY TO RE-ACQUIRE
		// if (!tryToAcquireLock(iRequester, iResourceId, iLockType))
		// // TIMEOUT EXPIRED
		// throw new OLockException("Resource " + iResourceId + " is locked");
	}

	public synchronized void releaseLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType)
			throws OLockException {
		// System.out.println("Release " + iLockType + " lock for " + iResourceId + " by " + iRequester);
		if (iLockType == LOCK.SHARED) {
			final Object sharedLock = sharedLocks.get(iResourceId);
			if (sharedLock == null)
				throw new OLockException("Error on releasing a non acquired SHARED lock by the requester " + iRequester + " on resource: "
						+ iResourceId);

			downsizeSharedLock(iResourceId, sharedLock);
		} else {
			final REQUESTER_TYPE exclusiveLock = exclusiveLocks.remove(iResourceId);
			if (exclusiveLock == null)
				throw new OLockException("Error on releasing a non acquired EXCLUSIVE lock by the requester " + iRequester
						+ " on resource: " + iResourceId);
		}
		lockQueue.wakeupWaiters(iResourceId);
	}

	public int getConcurrencyLevel() {
		return concurrencyLevel;
	}

	public void setDefaultConcurrencyLevel(final int concurrencyLevel) {
		this.concurrencyLevel = Math.max(2, concurrencyLevel);
	}

	public boolean isDownsizeSharedLockRetainList() {
		return downsizeSharedLockRetainList;
	}

	public void setDownsizeSharedLockRetainList(final boolean downsizeSharedLockRetainList) {
		this.downsizeSharedLockRetainList = downsizeSharedLockRetainList;
	}

	protected synchronized boolean tryToAcquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId,
			final LOCK iLockType) {
		OProfiler.getInstance().updateCounter("LockMgr.tryToAcquire", +1);

		// System.out.println("Try acquire " + iLockType + " lock for " + iResourceId + " by " + iRequester);

		REQUESTER_TYPE client = exclusiveLocks.get(iResourceId);
		if (client != null) {
			if (client.equals(iRequester))
				return true;
			// THE RESOURCE IS ALREADY LOCKED IN EXCLUSIVE MODE
			OProfiler.getInstance().updateCounter("LockMgr.tryToAcquire.locked", +1);
			return false;
		}

		// CHECK IF THERE ALREADY ARE SHARED LOCKS
		final Object sharedLock = sharedLocks.get(iResourceId);
		List<REQUESTER_TYPE> clients;

		if (iLockType == LOCK.SHARED) {
			if (sharedLock == null) {
				// CREATE IT
				sharedLocks.put(iResourceId, iRequester);
				return true;
			}

			if (sharedLock instanceof List<?>) {
				clients = (List<REQUESTER_TYPE>) sharedLock;
			} else {
				// TRANSFORM THE SINGLE CLIENT IN A LIST
				clients = new ArrayList<REQUESTER_TYPE>(concurrencyLevel);
				sharedLocks.put(iResourceId, clients);

				// ADD THE FIRST CLIENT
				clients.add((REQUESTER_TYPE) sharedLock);
			}

			// ADD THE SHARED LOCK
			clients.add(iRequester);
			return true;
		} else {
			if (sharedLock == null) {
				// NO ONE IS LOCKING IN SHARED MODE: ACQUIRE THE EXCLUSIVE LOCK
				// System.out.println("Exclusive lock for " + iResourceId + " by " + iRequester);
				exclusiveLocks.put(iResourceId, iRequester);
				return true;
			}

			// CHECK IF CAN GAIN THE EXCLUSIVE LOCK
			if (sharedLock instanceof List<?>) {
				clients = (List<REQUESTER_TYPE>) sharedLock;
				if (clients.size() == 0) {
					// System.out.println("Exclusive lock for " + iResourceId + " by " + iRequester);
					exclusiveLocks.put(iResourceId, iRequester);
					return true;
				}
				if (clients.size() == 1 && clients.get(0).equals(iRequester)) {
					// EXCALATION FROM SHARED TO EXCLUSIVE LOCK
					// System.out.println("Exclusive lock promoted for " + iResourceId + " by " + iResourceId);
					promoteLock(iRequester, iResourceId, sharedLock);
					return true;
				}
			} else {
				if (sharedLock.equals(iRequester)) {
					// EXCALATION FROM SHARED TO EXCLUSIVE LOCK
					promoteLock(iRequester, iResourceId, sharedLock);
					return true;
				}
			}
		}
		return false;
	}

	private void promoteLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final Object iSharedLock) {
		downsizeSharedLock(iResourceId, iSharedLock);
		exclusiveLocks.put(iResourceId, iRequester);
	}

	private void downsizeSharedLock(final RESOURCE_TYPE iResourceId, final Object iSharedLock) {
		if (downsizeSharedLockRetainList && iSharedLock instanceof List<?>)
			// RETAIN THE LIST FOR FUTURE DOWNGRADE TO SHARED
			((List<REQUESTER_TYPE>) iSharedLock).clear();
		else
			// REMOVE THE SHARED LOCK
			sharedLocks.remove(iResourceId);
	}

	public void clear() {
		sharedLocks.clear();
		exclusiveLocks.clear();
	}
}
