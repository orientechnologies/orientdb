package com.orientechnologies.orient.core.storage;

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
public class OStorageModificationLock {
	private volatile boolean veto = false;
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