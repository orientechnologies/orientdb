package com.orientechnologies.common.test;

public class SpeedTestData {
	protected static final int	TIME_WAIT						= 200;
	protected long							cycles							= 1;
	protected long							cyclesDone					= 0;
	protected final static int	DUMP_PERCENT				= 10;

	protected String						currentTestName;
	protected long							currentTestTimer;
	protected long							currentTestFreeMemory;
	protected long							currentTestTotalMemory;
	protected long							currentTestMaxMemory;
	protected SpeedTestGroup		testGroup;
	protected Object[]					configuration;
	protected boolean						printResults				= true;

	protected long							partialTimer				= 0;
	protected int								partialTimerCounter	= 0;
	private long								cyclesElapsed;

	protected SpeedTestData() {
	}

	protected SpeedTestData(final long iCycles) {
		cycles = iCycles;
	}

	protected SpeedTestData(final SpeedTestGroup iGroup) {
		setTestGroup(iGroup);
	}

	public SpeedTestData config(final Object... iArgs) {
		configuration = iArgs;
		return this;
	}

	public void go(final SpeedTest iTarget) {
		currentTestName = iTarget.getClass().getSimpleName();

		try {
			if (SpeedTestData.executeInit(iTarget, configuration))
				executeTest(iTarget, configuration);
		} finally {
			SpeedTestData.executeDeinit(iTarget, configuration);

			collectResults(takeTimer());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.test.SpeedTest#startTimer(java.lang.String)
	 */
	public void startTimer(final String iName) {
		Runtime.getRuntime().runFinalization();
		Runtime.getRuntime().gc();

		try {
			Thread.sleep(TIME_WAIT);
		} catch (InterruptedException e) {
		}

		currentTestName = iName;
		currentTestFreeMemory = Runtime.getRuntime().freeMemory();
		currentTestTotalMemory = Runtime.getRuntime().totalMemory();
		currentTestMaxMemory = Runtime.getRuntime().maxMemory();

		System.out.println("-> Started the test of '" + currentTestName + "' (" + cycles + " cycles)");

		currentTestTimer = System.currentTimeMillis();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.test.SpeedTest#takeTimer()
	 */
	public long takeTimer() {
		return System.currentTimeMillis() - currentTestTimer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.test.SpeedTest#collectResults(long)
	 */
	public void collectResults(final long elapsed) {
		Runtime.getRuntime().runFinalization();
		Runtime.getRuntime().gc();

		final long nowFreeMemory = Runtime.getRuntime().freeMemory();
		final long nowTotalMemory = Runtime.getRuntime().totalMemory();
		final long nowMaxMemory = Runtime.getRuntime().maxMemory();

		final long freeMemory = nowFreeMemory - currentTestFreeMemory;
		final long totalMemory = nowTotalMemory - currentTestTotalMemory;
		final long maxMemory = nowMaxMemory - currentTestMaxMemory;

		if (printResults) {
			System.out.println();
			System.out.println("   Completed the test of '" + currentTestName + "' in " + elapsed + " ms. Memory used: " + freeMemory);
			System.out.println("   Cycles done.........: " + cyclesDone + "/" + cycles);
			System.out.println("   Cycles Elapsed......: " + cyclesElapsed + " ms");
			System.out.println("   Elapsed.............: " + elapsed + " ms");
			System.out.println("   Medium cycle elapsed: " + (float) elapsed / cyclesDone);
			System.out.println("   Cycles per second...: " + (float) cyclesDone / elapsed * 1000);
			System.out.println("   Free memory diff....: " + freeMemory + " (" + currentTestFreeMemory + "->" + nowFreeMemory + ")");
			System.out.println("   Total memory diff...: " + totalMemory + " (" + currentTestTotalMemory + "->" + nowTotalMemory + ")");
			System.out.println("   Max memory diff.....: " + maxMemory + " (" + currentTestMaxMemory + "->" + nowMaxMemory + ")");
			System.out.println();
		}

		if (testGroup != null) {
			testGroup.setResult("Execution time", currentTestName, elapsed);
			testGroup.setResult("Free memory", currentTestName, freeMemory);
		}

		currentTestFreeMemory = freeMemory;
		currentTestTotalMemory = totalMemory;
		currentTestMaxMemory = maxMemory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.test.SpeedTest#printSnapshot()
	 */
	public long printSnapshot() {
		final long e = takeTimer();

		StringBuilder buffer = new StringBuilder();
		buffer.append("Partial timer #");
		buffer.append(++partialTimerCounter);
		buffer.append(" elapsed: ");
		buffer.append(e);
		buffer.append(" ms");

		if (partialTimer > 0) {
			buffer.append(" (from last partial: ");
			buffer.append(e - partialTimer);
			buffer.append(" ms)");
		}

		System.out.println(buffer);

		partialTimer = e;

		return partialTimer;
	}

	public long getCycles() {
		return cycles;
	}

	public SpeedTestData setCycles(final long cycles) {
		this.cycles = cycles;
		return this;
	}

	public SpeedTestGroup getTestGroup() {
		return testGroup;
	}

	public SpeedTestData setTestGroup(final SpeedTestGroup testGroup) {
		this.testGroup = testGroup;
		return this;
	}

	public Object[] getConfiguration() {
		return configuration;
	}

	protected static boolean executeInit(final SpeedTest iTarget, final Object... iArgs) {
		try {
			iTarget.init();
			return true;
		} catch (Throwable t) {
			System.err.println("Exception caught when executing INIT: " + iTarget.getClass().getSimpleName());
			t.printStackTrace();
			return false;
		}
	}

	protected long executeTest(final SpeedTest iTarget, final Object... iArgs) {
		try {
			startTimer(iTarget.getClass().getSimpleName());

			cyclesElapsed = 0;

			long previousLapTimerElapsed = 0;
			long lapTimerElapsed = 0;
			int delta;
			lapTimerElapsed = System.nanoTime();

			for (cyclesDone = 0; cyclesDone < cycles; ++cyclesDone) {
				iTarget.beforeCycle();

				iTarget.cycle();

				iTarget.afterCycle();

				if (cycles > DUMP_PERCENT && (cyclesDone + 1) % (cycles / DUMP_PERCENT) == 0) {
					lapTimerElapsed = (System.nanoTime() - lapTimerElapsed) / 1000000;

					cyclesElapsed += lapTimerElapsed;

					delta = (int) (previousLapTimerElapsed > 0 ? lapTimerElapsed * 100 / previousLapTimerElapsed - 100 : 0);

					System.out.print(String.format("\n%3d%% lap elapsed: %7dms, total: %7dms, delta: %+3d%%, forecast: %7dms",
							(cyclesDone + 1) * 100 / cycles, lapTimerElapsed, cyclesElapsed, delta, cyclesElapsed * cycles / cyclesDone));

					previousLapTimerElapsed = lapTimerElapsed;
					lapTimerElapsed = System.nanoTime();
				}
			}

			return takeTimer();

		} catch (Throwable t) {
			System.err.println("Exception caught when executing CYCLE test: " + iTarget.getClass().getSimpleName());
			t.printStackTrace();
		}
		return -1;
	}

	protected static void executeDeinit(final SpeedTest iTarget, final Object... iArgs) {
		try {
			iTarget.deinit();
		} catch (Throwable t) {
			System.err.println("Exception caught when executing DEINIT: " + iTarget.getClass().getSimpleName());
			t.printStackTrace();
		}
	}

	public long getCyclesDone() {
		return cyclesDone;
	}
}
