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
import java.util.Map.Entry;
import java.util.Set;

/**
 * Profiling util class. It can: - make statistics - chrono execution time of code blocks
 * <p/>
 * Statistics and chrono may be disabled on construction.
 * 
 * @author Oriet Staff
 * @copyrights Orient Technologies.com
 */
public class OProfiler implements OProfilerMBean {
	private long											recording	= -1;
	private HashMap<String, Long>			statistics;
	private HashMap<String, OChrono>	chronos;
	private Date											lastReset;

	protected static final OProfiler	instance	= new OProfiler();

	// INNER CLASSES:
	public class OChrono {
		public String	name						= null;
		public long		items						= 0;
		public long		lastElapsed			= 0;
		public long		minElapsed			= 999999999;
		public long		maxElapsed			= 0;
		public long		averageElapsed	= 0;
		public long		totalElapsed		= 0;

		@Override
		public String toString() {
			return "Chrono [averageElapsed=" + averageElapsed + ", items=" + items + ", lastElapsed=" + lastElapsed + ", maxElapsed="
					+ maxElapsed + ", minElapsed=" + minElapsed + ", name=" + name + ", totalElapsed=" + totalElapsed + "]";
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

	private void init() {
		statistics = new HashMap<String, Long>();
		chronos = new HashMap<String, OChrono>();

		lastReset = new Date();
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#updateStatistic(java.lang.String, long)
	 */
	public synchronized void updateStatistic(final String iStatName, final long iPlus) {
		// CHECK IF STATISTICS ARE ACTIVED
		if (recording < 0)
			return;

		if (iStatName == null)
			return;

		Long stat = statistics.get(iStatName);

		long oldValue = stat == null ? 0 : stat.longValue();

		stat = new Long(oldValue + iPlus);

		statistics.put(iStatName, stat);
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#getStatistic(java.lang.String)
	 */
	public synchronized long getStatistic(final String iStatName) {
		// CHECK IF STATISTICS ARE ACTIVED
		if (recording < 0)
			return -1;

		if (iStatName == null)
			return -1;

		Long stat = statistics.get(iStatName);

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
		return "\n" + dumpStatistics() + "\n\n" + dumpChronos();
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#reset()
	 */
	public synchronized void reset() {
		lastReset = new Date();

		if (statistics != null)
			statistics.clear();
		if (chronos != null)
			chronos.clear();
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#startChrono()
	 */
	public synchronized long startChrono() {
		// CHECK IF CHRONOS ARE ACTIVED
		if (recording < 0)
			return -1;

		return System.currentTimeMillis();
	}

	// ----------------------------------------------------------------------------
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#stopChrono(java.lang.String, long)
	 */
	public synchronized long stopChrono(final String iName, final long iStartTime) {
		// CHECK IF CHRONOS ARE ACTIVED
		if (recording < 0)
			return -1;

		long now = System.currentTimeMillis();

		OChrono c = chronos.get(iName);

		if (c == null) {
			// CREATE NEW CHRONO
			c = new OChrono();
			chronos.put(iName, c);
		}

		c.name = iName;
		c.items++;
		c.lastElapsed = now - iStartTime;
		c.totalElapsed += c.lastElapsed;
		c.averageElapsed = c.totalElapsed / c.items;

		if (c.lastElapsed < c.minElapsed)
			c.minElapsed = c.lastElapsed;

		if (c.lastElapsed > c.maxElapsed)
			c.maxElapsed = c.lastElapsed;

		return c.lastElapsed;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#dumpStatistics()
	 */
	public synchronized String dumpStatistics() {
		// CHECK IF STATISTICS ARE ACTIVED
		if (recording < 0)
			return "Statistics: <no recording>";

		Long stat;
		String statName;
		StringBuilder buffer = new StringBuilder();

		buffer.append("DUMPING STATISTICS (last reset on: " + lastReset.toString() + ")...");

		buffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		buffer.append(String.format("\n%45s | Value                                                             |", "Name"));
		buffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		for (Iterator<String> it = statistics.keySet().iterator(); it.hasNext();) {
			statName = it.next();
			stat = statistics.get(statName);
			buffer.append(String.format("\n%45s | %d", statName, stat));
		}

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#dumpChronos()
	 */
	public synchronized String dumpChronos() {
		// CHECK IF CHRONOS ARE ACTIVED
		if (recording < 0)
			return "Chronos: <no recording>";

		StringBuilder buffer = new StringBuilder();

		buffer.append("DUMPING CHRONOS (last reset on: " + lastReset.toString() + "). Times in ms...");

		OChrono c;
		String chronoName;

		buffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		buffer.append(String.format("\n%45s | %10s %10s %10s %10s %10s %10s |", "Name", "last", "total", "min", "max", "average",
				"items"));
		buffer.append(String.format("\n%45s +-------------------------------------------------------------------+", ""));
		for (Iterator<String> it = chronos.keySet().iterator(); it.hasNext();) {
			chronoName = it.next();
			c = chronos.get(chronoName);
			buffer.append(String.format("\n%45s | %10d %10d %10d %10d %10d %10d", chronoName, c.lastElapsed, c.totalElapsed,
					c.minElapsed, c.maxElapsed, c.averageElapsed, c.items));
		}
		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#getStatistics()
	 */
	public String[] getStatisticsAsString() {
		String[] output = new String[statistics.size()];
		int i = 0;
		for (Entry<String, Long> entry : statistics.entrySet()) {
			output[i++] = entry.getKey() + ": " + entry.getValue().toString();
		}
		return output;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#getChronos()
	 */
	public String[] getChronosAsString() {
		String[] output = new String[chronos.size()];
		int i = 0;
		for (Entry<String, OChrono> entry : chronos.entrySet()) {
			output[i++] = entry.getKey() + ": " + entry.getValue().toString();
		}

		return output;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#getLastReset()
	 */
	public Date getLastReset() {
		return lastReset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.profiler.ProfileMBean#isStatActive()
	 */
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

	public Set<Entry<String, Long>> getStatistics() {
		return statistics.entrySet();
	}

	public Set<Entry<String, OChrono>> getChronos() {
		return chronos.entrySet();
	}

	public OChrono getChrono(final String iChronoName) {
		return chronos.get(iChronoName);
	}
}
