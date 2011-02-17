package com.orientechnologies.orient.core;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Collection;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.OMemoryWatchDog.Listener.TYPE;

/**
 * This memory warning system will call the listener when we exceed the percentage of available memory specified. There should only
 * be one instance of this object created, since the usage threshold can only be set to one number.
 */
public class OMemoryWatchDog {
	private final Collection<Listener>		listeners				= new ArrayList<Listener>();
	private static final MemoryPoolMXBean	tenuredGenPool	= findTenuredGenPool();
	private int														alertTimes			= 0;

	public interface Listener {
		public enum TYPE {
			OS, JVM
		}

		public void memoryUsageLow(TYPE iType, long iUsedMemory, long iMaxMemory);
	}

	/**
	 * Create the memory watch dog with the default memory threshold.
	 * 
	 * @param iThreshold
	 */
	public OMemoryWatchDog(final float iThreshold) {
		OMemoryWatchDog.setPercentageUsageThreshold(iThreshold);

		final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();

		final NotificationEmitter memEmitter = (NotificationEmitter) memBean;
		memEmitter.addNotificationListener(new NotificationListener() {
			public void handleNotification(Notification n, Object hb) {
				if (n.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
					alertTimes++;
					final long maxMemory = tenuredGenPool.getUsage().getMax();
					final long usedMemory = tenuredGenPool.getUsage().getUsed();

					OLogManager.instance().warn(this, "Low memory (%s of %s), calling listeners to free memory...",
							OFileUtils.getSizeAsString(usedMemory), OFileUtils.getSizeAsString(maxMemory));

					final long timer = OProfiler.getInstance().startChrono();

					try {
						for (Listener listener : listeners) {
							listener.memoryUsageLow(TYPE.JVM, usedMemory, maxMemory);
						}
					} finally {
						OProfiler.getInstance().stopChrono("OMemoryWatchDog.freeResources", timer);
					}
				}
			}
		}, null, null);

		OProfiler.getInstance().registerHookValue("memory.alerts", new OProfilerHookValue() {
			public Object getValue() {
				return alertTimes;
			}
		});
	}

	public Collection<Listener> getListeners() {
		return listeners;
	}

	public boolean addListener(Listener listener) {
		return listeners.add(listener);
	}

	public boolean removeListener(Listener listener) {
		return listeners.remove(listener);
	}

	public static void setPercentageUsageThreshold(double percentage) {
		if (percentage <= 0.0 || percentage > 1.0) {
			throw new IllegalArgumentException("Percentage not in range");
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
}