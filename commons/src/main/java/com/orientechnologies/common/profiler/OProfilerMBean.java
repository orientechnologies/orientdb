package com.orientechnologies.common.profiler;

import java.util.Date;

public interface OProfilerMBean {

	// ----------------------------------------------------------------------------
	public void updateCounter(String iStatName, long iPlus);

	// ----------------------------------------------------------------------------
	public long getCounter(String iStatName);

	public String dump();

	public String dumpCounters();

	// ----------------------------------------------------------------------------
	/**
	 * Resetta i contatori statistici e i chrono.
	 */
	public void reset();

	// ----------------------------------------------------------------------------
	/**
	 * Fa partire un nuovo chrono.
	 */
	public long startChrono();

	// ----------------------------------------------------------------------------
	/**
	 * Ferma un chrono con nome <iName>.
	 */
	public long stopChrono(String iName, long iStartTime);

	public String dumpChronos();

	public String[] getCountersAsString();

	public String[] getChronosAsString();

	public Date getLastReset();

	public boolean isRecording();

	public OProfilerMBean startRecording();

	public OProfilerMBean stopRecording();
}