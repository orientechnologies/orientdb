/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.common.synch;

import java.util.LinkedHashMap;

import com.orientechnologies.common.concur.lock.OLockException;
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
		if (OLogManager.instance().isDebugEnabled()) {
                    OLogManager.instance().debug(
                            this,
                            "Thread [" + Thread.currentThread().getId() + "] is waiting for the resource " + iResource
                                    + (iTimeout <= 0 ? " forever" : " until " + iTimeout + "ms"));
                }

		synchronized (iResource) {
			try {
				iResource.wait(iTimeout);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new OLockException("Thread interrupted while waiting for resource '" + iResource + "'");
			}
		}

		Object[] value = queue.remove(iResource);

		return (RESPONSE_TYPE) (value != null ? value[1] : null);
	}

	public void setValue(final RESOURCE_TYPE iResource, final Object iValue) {
		final Object[] waiter = queue.get(iResource);
		if (waiter == null) {
                    return;
                }

		synchronized (waiter[0]) {
			waiter[1] = iValue;
			waiter[0].notifyAll();
		}
	}
}
