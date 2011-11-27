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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog.Listener.TYPE;

/**
 * This memory warning system will call the listener when we exceed the percentage of available memory specified. There should only
 * be one instance of this object created, since the usage threshold can only be set to one number.
 */
public class OMemoryWatchDog {
	private final Collection<Listener>		listeners				= new CopyOnWriteArrayList<Listener>();
	private static final MemoryPoolMXBean	tenuredGenPool	= findTenuredGenPool();
	private int														alertTimes			= 0;

	public interface Listener {
		public enum TYPE {
			OS, JVM
		}

		/**
		 * Execute a soft free of memory resources.
		 * 
		 * @param iType
		 *          OS or JVM
		 * @param iUsedMemory
		 *          Current used memory
		 * @param iMaxMemory
		 *          Max memory
		 */
		public void memoryUsageLow(TYPE iType, long iUsedMemory, long iMaxMemory);

		/**
		 * Execute a hard free of memory resources.
		 * 
		 * @param iType
		 *          OS or JVM
		 * @param iUsedMemory
		 *          Current used memory
		 * @param iMaxMemory
		 *          Max memory
		 */
		public void memoryUsageCritical(TYPE iType, long iUsedMemory, long iMaxMemory);
	}

	/**
	 * Create the memory watch dog with the default memory threshold.
	 * 
	 * @param iThreshold
	 */
	public OMemoryWatchDog(final float iThreshold) {
		OMemoryWatchDog.setPercentageUsageThreshold(iThreshold);

		final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();

		if (memBean instanceof NotificationEmitter) {
			final NotificationEmitter memEmitter = (NotificationEmitter) memBean;
			memEmitter.addNotificationListener(new NotificationListener() {
				public synchronized void handleNotification(Notification n, Object hb) {
					if (n.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
						alertTimes++;
						long maxMemory = tenuredGenPool.getUsage().getMax();
						long usedMemory = tenuredGenPool.getUsage().getUsed();
						long freeMemory = maxMemory - usedMemory;

						OLogManager.instance().debug(this,
								"Free memory is low %s %s%% (used %s of %s), calling listeners to free memory in SOFT way...",
								OFileUtils.getSizeAsString(freeMemory), freeMemory * 100 / maxMemory, OFileUtils.getSizeAsString(usedMemory),
								OFileUtils.getSizeAsString(maxMemory));

						final long timer = OProfiler.getInstance().startChrono();

						for (Listener listener : listeners) {
							try {
								listener.memoryUsageLow(TYPE.JVM, usedMemory, maxMemory);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}

						long threshold;
						do {
							// INVOKE GC AND WAIT A BIT
							freeMemory(300);

							// RECHECK IF MEMORY IS OK NOW
							maxMemory = tenuredGenPool.getUsage().getMax();
							usedMemory = tenuredGenPool.getUsage().getUsed();
							freeMemory = maxMemory - usedMemory;

							threshold = (long) (maxMemory * (1 - OGlobalConfiguration.MEMORY_OPTIMIZE_THRESHOLD.getValueAsFloat()));

							OLogManager.instance().debug(this, "Free memory now is %s %s%% (used %s of %s) with threshold for HARD clean is %s",
									OFileUtils.getSizeAsString(freeMemory), freeMemory * 100 / maxMemory, OFileUtils.getSizeAsString(usedMemory),
									OFileUtils.getSizeAsString(maxMemory), OFileUtils.getSizeAsString(threshold));

							if (freeMemory < threshold) {
								OLogManager
										.instance()
										.debug(
												this,
												"Free memory is low %s %s%% (used %s of %s) while the threshold is %s, calling listeners to free memory in HARD way...",
												OFileUtils.getSizeAsString(freeMemory), freeMemory * 100 / maxMemory,
												OFileUtils.getSizeAsString(usedMemory), OFileUtils.getSizeAsString(maxMemory),
												OFileUtils.getSizeAsString(threshold));

								for (Listener listener : listeners) {
									try {
										listener.memoryUsageCritical(TYPE.JVM, usedMemory, maxMemory);
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}
						} while (freeMemory < threshold);

						OProfiler.getInstance().stopChrono("OMemoryWatchDog.freeResources", timer);
					}
				}
			}, null, null);
		} else
			OLogManager.instance().warn(this,
					"Installed JVM's MemoryMXBean '%s' does not support notifications. This could cause problems with run-time memory usage",
					memBean);

		OProfiler.getInstance().registerHookValue("memory.alerts", new OProfilerHookValue() {
			public Object getValue() {
				return alertTimes;
			}
		});
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

	public static void setPercentageUsageThreshold(double percentage) {
		if (percentage <= 0.0 || percentage > 1.0) {
			throw new IllegalArgumentException("Percentage out of range");
		}
		long maxMemory = tenuredGenPool.getUsage().getMax();
		long warningThreshold = (long) (maxMemory * percentage);
		tenuredGenPool.setUsageThreshold(warningThreshold);
	}

	/**
	 * Tenured Space Pool can be determined by it being of type HEAP and by it being possible to set the usage threshold.
	 */
	private static MemoryPoolMXBean findTenuredGenPool() {
		for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
			// I don't know whether this approach is better, or whether
			// we should rather check for the pool name "Tenured Gen"?
			if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
				return pool;
			}
		}
		throw new AssertionError("Could not find tenured space");
	}

	public static void freeMemory(final long iDelayTime) {
		// INVOKE GC AND WAIT A BIT
		System.gc();
		if (iDelayTime > 0)
			try {
				Thread.sleep(iDelayTime);
			} catch (InterruptedException e) {
			}
	}
}