package com.orientechnologies.common.concur.resource;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OSharedResourceAdaptive {
	private static final int	UNLOCKED_WAIT_TIME	= 30;
	private ReadWriteLock			lock								= new ReentrantReadWriteLock();
	private volatile int			users								= 0;
	private volatile boolean	runningWithoutLock	= false;

	protected boolean acquireExclusiveLock() {
		if (users > 1) {
			lock.writeLock().lock();

			if (runningWithoutLock)
				// WAIT UNTIL THE UNIQUE THREAD IS RUNNING WITHOUT LOCK FINISHES
				while (runningWithoutLock)
					try {
						Thread.sleep(UNLOCKED_WAIT_TIME);
					} catch (InterruptedException e) {
					}

			return true;
		}

		runningWithoutLock = true;
		return false;
	}

	protected boolean acquireSharedLock() {
		if (users > 1) {
			lock.readLock().lock();
			return true;
		}

		runningWithoutLock = true;
		return false;
	}

	protected void releaseExclusiveLock(final boolean iLocked) {
		if (iLocked)
			lock.writeLock().unlock();
		else
			runningWithoutLock = false;
	}

	protected void releaseSharedLock(final boolean iLocked) {
		if (iLocked)
			lock.readLock().unlock();
		else
			runningWithoutLock = false;
	}

	public int getUsers() {
		return users;
	}

	public int addUser() {
		// ASSURE TO ACQUIRE THE LOCK FIRST
		lock.writeLock().lock();

		try {
			return ++users;

		} finally {
			lock.writeLock().unlock();
		}
	}

	public int removeUser() {
		if (users < 1)
			throw new IllegalStateException("Can't remove user of the shared resource " + toString() + " because no user is using it");

		try {
			lock.writeLock().lock();

			return --users;

		} finally {
			lock.writeLock().unlock();
		}
	}
}
