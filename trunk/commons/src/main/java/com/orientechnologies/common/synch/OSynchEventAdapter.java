package com.orientechnologies.common.synch;

import java.util.LinkedHashMap;

import com.orientechnologies.common.log.OLogManager;

/**
 * Manage the synchronization among Runnable requester. Each resource can be acquired by only one Runnable instance.
 */
@SuppressWarnings("unchecked")
public class OSynchEventAdapter<RESOURCE_TYPE, RESPONSE_TYPE> {
	protected final LinkedHashMap<RESOURCE_TYPE, Object[]>	queue	= new LinkedHashMap<RESOURCE_TYPE, Object[]>();

	public OSynchEventAdapter() {
	}

	public void registerCallbackCurrentThread(final RESOURCE_TYPE iResource) {
		queue.put(iResource, new Object[] { iResource, null });
	}

	/**
	 * Wait forever until the requested resource is unlocked.
	 */
	public RESPONSE_TYPE waitForResource(final RESOURCE_TYPE iResource) {
		return getValue(iResource, 0);
	}

	/**
	 * Wait until the requested resource is unlocked. Put the current thread in sleep until timeout or is waked up by an unlock.
	 */
	public synchronized RESPONSE_TYPE getValue(final RESOURCE_TYPE iResource, final long iTimeout) {
		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(
					this,
					"Thread [" + Thread.currentThread().getId() + "] is waiting for the resource " + iResource
							+ (iTimeout <= 0 ? " forever" : " until " + iTimeout + "ms"));

		synchronized (iResource) {
			try {
				iResource.wait(iTimeout);
			} catch (InterruptedException e) {
			}
		}

		Object[] value = queue.remove(iResource);

		return (RESPONSE_TYPE) (value != null ? value[1] : null);
	}

	public void setValue(final RESOURCE_TYPE iResource, final Object iValue) {
		final Object[] waiter = queue.get(iResource);
		if (waiter == null)
			return;

		synchronized (waiter[0]) {
			waiter[1] = iValue;
			waiter[0].notifyAll();
		}
	}
}
