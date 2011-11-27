package com.orientechnologies.common.concur.resource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OSharedResourceAdaptive {
	private ReadWriteLock	lock	= new ReentrantReadWriteLock();
	private AtomicInteger	users	= new AtomicInteger(0);
	private final boolean	concurrent;

	public OSharedResourceAdaptive(final boolean iConcurrent) {
		this.concurrent = iConcurrent;
	}

	protected void acquireExclusiveLock() {
		if (concurrent)
			lock.writeLock().lock();
	}

	protected void acquireSharedLock() {
		if (concurrent)
			lock.readLock().lock();
	}

	protected void releaseExclusiveLock() {
		if (concurrent)
			lock.writeLock().unlock();
	}

	protected void releaseSharedLock() {
		if (concurrent)
			lock.readLock().unlock();
	}

	public int getUsers() {
		return users.get();
	}

	public int addUser() {
		return users.incrementAndGet();
	}

	public int removeUser() {
		if (users.get() < 1)
			throw new IllegalStateException("Cannot remove user of the shared resource " + toString() + " because no user is using it");

		return users.decrementAndGet();
	}

	public boolean isConcurrent() {
		return concurrent;
	}

}
