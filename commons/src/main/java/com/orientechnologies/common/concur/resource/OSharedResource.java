package com.orientechnologies.common.concur.resource;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OSharedResource {
	protected ReadWriteLock	lock	= new ReentrantReadWriteLock();

	protected void acquireSharedLock() {
		lock.readLock().lock();
	}

	protected void releaseSharedLock() {
		lock.readLock().unlock();
	}

	protected void acquireExclusiveLock() {
		lock.writeLock().lock();
	}

	protected void releaseExclusiveLock() {
		lock.writeLock().unlock();
	}
}
