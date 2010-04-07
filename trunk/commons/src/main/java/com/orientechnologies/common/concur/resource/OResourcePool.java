package com.orientechnologies.common.concur.resource;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.concur.lock.OLockException;

public class OResourcePool<K, V> {
	private final Semaphore							sem;
	private final Queue<V>							resources	= new ConcurrentLinkedQueue<V>();
	private OResourcePoolListener<K, V>	listener;

	public OResourcePool(final int iMaxResources, final OResourcePoolListener<K, V> iListener) {
		listener = iListener;
		sem = new Semaphore(iMaxResources, true);
	}

	public V getResource(K iKey, final long iMaxWaitMillis) throws InterruptedException, OLockException {

		// First, get permission to take or create a resource
		sem.tryAcquire(iMaxWaitMillis, TimeUnit.MILLISECONDS);

		// Then, actually take one if available...
		V res = resources.poll();
		if (res != null)
			return res;

		// ...or create one if none available
		try {
			res = listener.createNewResource(iKey);
			return res;
		} catch (Exception e) {
			// Don't hog the permit if we failed to create a resource!
			sem.release();
			throw new OLockException("Error on creation of the new resource in the pool", e);
		}
	}

	public void returnResource(final V res) {
		resources.add(res);
		sem.release();
	}
}
