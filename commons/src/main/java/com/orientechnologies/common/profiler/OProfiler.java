/*
 * Copyright 1999-2005 Luca Garulli (l.garulli--at-orientechnologies.com)
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

package com.orientechnologies.common.profiler;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
 * any instances you want for separate profiling contexts.
 * 
 * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
 * 
 * @author Luca Garulli
 * @copyrights Orient Technologies.com
 */
public class OProfiler implements OProfilerMBean {
	private long												recording	= -1;
	private Map<String, Long>						counters;
	private Map<String, OProfilerEntry>	chronos;
	private Map<String, OProfilerEntry>	stats;
	private Date												lastReset;

	protected static final OProfiler		instance	= new OProfiler();

	// INNER CLASSES:
	public class OProfilerEntry {
		public String	name		= null;
		public long		items		= 0;
		public long		last		= 0;
		public long		min			= 999999999;
		public long		max			= 0;
		public long		average	= 0;
		public long		total		= 0;

		@Override
		public String toString() {
			return "Chrono [average=" + average + ", items=" + items + ", last=" + last + ", max=" + max + ", min=" + min + ", name="
					+ name + ", total=" + total + "]";
		}
	}

	public OProfiler() {
		init();
	}

	public OProfiler(String iRecording) {
		if (iRecording.equalsIgnoreCase("true"))
			startRecording();

		init();
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 */
	public synchronized void updateCounter(final String iStatName, final long iPlus) {
		// CHECK IF STATISTICS ARE ACTIVED
		if (recording < 0)
			return;

		if (iStatName == null)
			return;

		Long stat = counters.get(iStatName);

		long oldValue = stat == null ? 0 : stat.longValue();

		stat = new Long(oldValue + iPlus);

		counters.put(iStatName, stat);
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#getStatistic(java.lang.String)
	 */
	public synchronized long getCounter(final String iStatName) {
		// CHECK IF STATISTICS ARE ACTIVED
		if (recording < 0)
			return -1;

		if (iStatName == null)
			return -1;

		Long stat = counters.get(iStatName);

		if (stat == null)
			return -1;

		return stat.longValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#dump()
	 */
	public synchronized String dump() {
		return "\n" + dumpCounters() + "\n\n" + dumpStats() + "\n\n" + dumpChronos();
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#reset()
	 */
	public synchronized void reset() {
		lastReset = new Date();

		if (counters != null)
			counters.clear();
		if (chronos != null)
			chronos.clear();
		if (stats != null)
			stats.clear();
	}

	public synchronized long startChrono() {
		// CHECK IF CHRONOS ARE ACTIVED
		if (recording < 0)
			return -1;

		return System.currentTimeMillis();
	}

	public long stopChrono(final String iName, final long iStartTime) {
		return updateEntry(chronos, iName, System.currentTimeMillis() - iStartTime);
	}

	public long updateStat(final String iName, final long iValue) {
		return updateEntry(stats, iName, iValue);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#dumpStatistics()
	 */
	public synchronized String dumpCounters() {
		// CHECK IF STATISTICS ARE ACTIVED
		if (recording < 0)
			return "Counters: <no recording>";

		Long stat;
		String statName;
		StringBuilder buffer = new StringBuilder();

		buffer.append("DUMPING COUNTERS (last reset on: " + lastReset.toString() + ")...");

		buffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		buffer.append(String.format("\n%45s | Value                                                             |", "Name"));
		buffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		for (Iterator<String> it = counters.keySet().iterator(); it.hasNext();) {
			statName = it.next();
			stat = counters.get(statName);
			buffer.append(String.format("\n%45s | %d", statName, stat));
		}

		return buffer.toString();
	}

	public String dumpChronos() {
		return dumpEntries(chronos, new StringBuilder("DUMPING CHRONOS (last reset on: " + lastReset.toString() + "). Times in ms..."));
	}

	public String dumpStats() {
		return dumpEntries(stats, new StringBuilder("DUMPING STATISTICS (last reset on: " + lastReset.toString() + "). Times in ms..."));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#getStatistics()
	 */
	public String[] getCountersAsString() {
		String[] output = new String[counters.size()];
		int i = 0;
		for (Entry<String, Long> entry : counters.entrySet()) {
			output[i++] = entry.getKey() + ": " + entry.getValue().toString();
		}
		return output;
	}

	public String[] getChronosAsString() {
		String[] output = new String[chronos.size()];
		int i = 0;
		for (Entry<String, OProfilerEntry> entry : chronos.entrySet()) {
			output[i++] = entry.getKey() + ": " + entry.getValue().toString();
		}

		return output;
	}

	public String[] getStatsAsString() {
		String[] output = new String[stats.size()];
		int i = 0;
		for (Entry<String, OProfilerEntry> entry : stats.entrySet()) {
			output[i++] = entry.getKey() + ": " + entry.getValue().toString();
		}

		return output;
	}

	public Date getLastReset() {
		return lastReset;
	}

	public Set<Entry<String, Long>> getCounters() {
		return counters.entrySet();
	}

	public Set<Entry<String, OProfilerEntry>> getChronos() {
		return chronos.entrySet();
	}

	public Set<Entry<String, OProfilerEntry>> getStats() {
		return stats.entrySet();
	}

	public OProfilerEntry getChrono(final String iChronoName) {
		return chronos.get(iChronoName);
	}

	public boolean isRecording() {
		return recording > -1;
	}

	public OProfilerMBean startRecording() {
		recording = System.currentTimeMillis();
		return this;
	}

	public OProfilerMBean stopRecording() {
		recording = -1;
		return this;
	}

	public static OProfiler getInstance() {
		return instance;
	}

	private void init() {
		counters = new HashMap<String, Long>();
		chronos = new HashMap<String, OProfilerEntry>();
		stats = new HashMap<String, OProfilerEntry>();

		lastReset = new Date();
	}

	private synchronized long updateEntry(Map<String, OProfilerEntry> iValues, final String iName, final long iValue) {
		if (recording < 0)
			return -1;

		OProfilerEntry c = iValues.get(iName);

		if (c == null) {
			// CREATE NEW CHRONO
			c = new OProfilerEntry();
			iValues.put(iName, c);
		}

		c.name = iName;
		c.items++;
		c.last = iValue;
		c.total += c.last;
		c.average = c.total / c.items;

		if (c.last < c.min)
			c.min = c.last;

		if (c.last > c.max)
			c.max = c.last;

		return c.last;
	}

	private synchronized String dumpEntries(final Map<String, OProfilerEntry> iValues, final StringBuilder iBuffer) {
		// CHECK IF CHRONOS ARE ACTIVED
		if (recording < 0)
			return "Chronos: <no recording>";

		OProfilerEntry c;

		iBuffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		iBuffer.append(String.format("\n%45s | %10s %10s %10s %10s %10s %10s |", "Name", "last", "total", "min", "max", "average",
				"items"));
		iBuffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		for (Entry<String, OProfilerEntry> e : iValues.entrySet()) {
			c = e.getValue();
			iBuffer.append(String.format("\n%45s | %10d %10d %10d %10d %10d %10d", e.getKey(), c.last, c.total, c.min, c.max, c.average,
					c.items));
		}
		return iBuffer.toString();
	}
}
