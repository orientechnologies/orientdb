/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.memory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.Orient;

/**
 * This memory warning system will call the listener when we exceed the percentage of available memory specified. There should only
 * be one instance of this object created, since the usage threshold can only be set to one number.
 */
public class OMemoryWatchDog extends Thread {
	private final Collection<Listener>	listeners			= new CopyOnWriteArrayList<Listener>();
	private int													alertTimes		= 0;
	protected ReferenceQueue<Object>		monitorQueue	= new ReferenceQueue<Object>();
	protected SoftReference<Object>			monitorRef		= new SoftReference<Object>(new Object(), monitorQueue);

	public interface Listener {
		/**
		 * Execute a soft free of memory resources.
		 * 
		 * @param iType
		 *          OS or JVM
		 * @param iFreeMemory
		 *          Current used memory
		 * @param iFreeMemoryPercentage
		 *          Max memory
		 */
		public void memoryUsageLow(long iFreeMemory, long iFreeMemoryPercentage);
	}

	/**
	 * Create the memory watch dog with the default memory threshold.
	 * 
	 * @param iThreshold
	 */
	public OMemoryWatchDog() {
		super(Orient.getThreadGroup(), "OrientDB MemoryWatchDog");

		OProfiler.getInstance().registerHookValue("memory.alerts", new OProfilerHookValue() {
			public Object getValue() {
				return alertTimes;
			}
		});

		setDaemon(true);
		start();
	}

	public void run() {
		while (true) {
			try {
				// WAITS FOR THE GC FREE
				monitorQueue.remove();

				// GC is freeing memory!
				alertTimes++;
				long maxMemory = Runtime.getRuntime().maxMemory();
				long freeMemory = Runtime.getRuntime().freeMemory();
				int freeMemoryPer = (int) (freeMemory * 100 / maxMemory);

				OLogManager.instance().debug(this, "Free memory is low %s of %s (%d%%), calling listeners to free memory...",
						OFileUtils.getSizeAsString(freeMemory), OFileUtils.getSizeAsString(maxMemory), freeMemoryPer);

				final long timer = OProfiler.getInstance().startChrono();

				for (Listener listener : listeners) {
					try {
						listener.memoryUsageLow(freeMemory, freeMemoryPer);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				OProfiler.getInstance().stopChrono("OMemoryWatchDog.freeResources", timer);

			} catch (Exception e) {
			} finally {
				// RE-INSTANTIATE THE MONITOR REF
				monitorRef = new SoftReference<Object>(new Object(), monitorQueue);
			}
		}
	}

	public Collection<Listener> getListeners() {
		return listeners;
	}

	public Listener addListener(Listener listener) {
		listeners.add(listener);
		return listener;
	}

	public boolean removeListener(Listener listener) {
		return listeners.remove(listener);
	}

	public static void freeMemory(final long iDelayTime) {
		// INVOKE GC AND WAIT A BIT
		System.gc();
		if (iDelayTime > 0)
			try {
				Thread.sleep(iDelayTime);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
	}
}