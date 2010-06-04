package com.orientechnologies.common.concur.lock;

import java.util.LinkedHashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OSoftThread;

/**
 * Manage the synchronization among Runnable requesters. Each resource can be acquired by only one Runnable instance.
 */
public class OLockQueue<RESOURCE_TYPE> {
	protected final LinkedHashMap<RESOURCE_TYPE, Runnable>	queue	= new LinkedHashMap<RESOURCE_TYPE, Runnable>();

	public OLockQueue() {
	}

	/**
	 * Wait forever until the requested resource is unlocked.
	 */
	public boolean waitForResource(RESOURCE_TYPE iResource) {
		return waitForResource(iResource, 0);
	}

	/**
	 * Wait until the requested resource is unlocked. Put the current thread in sleep until timeout or is waked up by an unlock.
	 */
	public synchronized boolean waitForResource(final RESOURCE_TYPE iResource, final long iTimeout) {
		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this,
					"Thread [" + Thread.currentThread() + "] is waiting for the resource " + iResource + " until " + iTimeout + "ms");

		queue.put(iResource, Thread.currentThread());
		return !OSoftThread.pauseCurrentThread(iTimeout);
	}

	public synchronized void wakeupWaiters(final RESOURCE_TYPE iResource) {
		final Runnable waiter = queue.remove(iResource);
		if (waiter == null)
			return;

		synchronized (waiter) {
			waiter.notifyAll();
		}
	}
}
