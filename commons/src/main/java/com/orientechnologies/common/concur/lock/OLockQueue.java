package com.orientechnologies.common.concur.lock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OSoftThread;

/**
 * Manages the queue of waiters for a resource.
 */
public class OLockQueue<RESOURCE_TYPE> {
	private static final int																		DEFAULT_WAITERS	= 3;
	protected final LinkedHashMap<RESOURCE_TYPE, List<Thread>>	queue						= new LinkedHashMap<RESOURCE_TYPE, List<Thread>>();

	/**
	 * Wait forever until the requested resource is unlocked.
	 */
	public boolean waitForResource(RESOURCE_TYPE iResource) {
		return waitForResource(iResource, 0);
	}

	/**
	 * Wait until the requested resource is unlocked. Put the current thread in sleep until timeout or is waked up by an unlock.
	 * 
	 * @return true if resource has been freed, false if timeout has reached.
	 */
	public boolean waitForResource(final RESOURCE_TYPE iResource, final long iTimeout) {
		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this,
					"Thread [" + Thread.currentThread() + "] is waiting for the resource " + iResource + " until " + iTimeout + "ms");

		// GET OR CREATE THE LIST OF WAITERS
		synchronized (queue) {
			List<Thread> waiters = queue.get(iResource);
			if (waiters == null) {
				waiters = new ArrayList<Thread>(DEFAULT_WAITERS);
				queue.put(iResource, waiters);
			}

			// ADD CURRENT WAITER TO THE LIST
			waiters.add(Thread.currentThread());
		}

		// WAIT UNTIL TIMOUT OR THE RESOURCE IS FREE
		return !OSoftThread.pauseCurrentThread(iTimeout);
	}

	public void wakeupWaiters(final RESOURCE_TYPE iResource) {
		synchronized (queue) {
			final List<Thread> waiters = queue.get(iResource);
			if (waiters == null || waiters.size() == 0)
				return;

			for (Thread w : waiters) {
				// synchronized (w) {
				// System.out.println("Try to wakeup : " + w);
				w.interrupt();
				// }
			}

			queue.remove(iResource);
		}
	}
}
