package com.orientechnologies.common.test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.math.BigDecimal;

public class SpeedTestData {
  protected static final int TIME_WAIT = 200;
  protected static final int DUMP_PERCENT = 10;
  protected long cycles = 1;
  protected long cyclesDone = 0;
  protected String currentTestName;
  protected long currentTestTimer;

  protected long currentTestHeapCommittedMemory;
  protected long currentTestHeapUsedMemory;
  protected long currentTestHeapMaxMemory;

  protected long currentTestNonHeapCommittedMemory;
  protected long currentTestNonHeapUsedMemory;
  protected long currentTestNonHeapMaxMemory;

  protected SpeedTestGroup testGroup;
  protected Object[] configuration;
  protected boolean printResults = true;

  protected long partialTimer = 0;
  protected int partialTimerCounter = 0;
  private long cyclesElapsed;

  protected SpeedTestData() {}

  protected SpeedTestData(final long iCycles) {
    cycles = iCycles;
  }

  protected SpeedTestData(final SpeedTestGroup iGroup) {
    setTestGroup(iGroup);
  }

  protected static boolean executeInit(final SpeedTest iTarget, final Object... iArgs) {
    try {
      iTarget.init();
      return true;
    } catch (Throwable t) {
      System.err.println(
          "Exception caught when executing INIT: " + iTarget.getClass().getSimpleName());
      t.printStackTrace();
      return false;
    }
  }

  protected static void executeDeinit(final SpeedTest iTarget, final Object... iArgs) {
    try {
      iTarget.deinit();
    } catch (Throwable t) {
      System.err.println(
          "Exception caught when executing DEINIT: " + iTarget.getClass().getSimpleName());
      t.printStackTrace();
    }
  }

  public SpeedTestData config(final Object... iArgs) {
    configuration = iArgs;
    return this;
  }

  public void go(final SpeedTest iTarget) {
    currentTestName = iTarget.getClass().getSimpleName();

    try {
      if (SpeedTestData.executeInit(iTarget, configuration)) executeTest(iTarget, configuration);
    } finally {
      collectResults(takeTimer());

      SpeedTestData.executeDeinit(iTarget, configuration);
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

    final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
    final MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

    currentTestName = iName;

    currentTestHeapCommittedMemory = heapMemoryUsage.getCommitted();
    currentTestHeapUsedMemory = heapMemoryUsage.getUsed();
    currentTestHeapMaxMemory = heapMemoryUsage.getMax();

    currentTestNonHeapCommittedMemory = nonHeapMemoryUsage.getCommitted();
    currentTestNonHeapUsedMemory = nonHeapMemoryUsage.getUsed();
    currentTestNonHeapMaxMemory = nonHeapMemoryUsage.getMax();

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

    final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
    final MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
    final int objectsPendingFinalizationCount = memoryMXBean.getObjectPendingFinalizationCount();

    final long nowHeapCommittedMemory = heapMemoryUsage.getCommitted();
    final long nowHeapUsedMemory = heapMemoryUsage.getUsed();
    final long nowHeapMaxMemory = heapMemoryUsage.getMax();

    final long heapCommittedMemory = nowHeapCommittedMemory - currentTestHeapCommittedMemory;
    final long heapUsedMemory = nowHeapUsedMemory - currentTestHeapUsedMemory;
    final long heapMaxMemory = nowHeapMaxMemory - currentTestHeapMaxMemory;

    final long nowNonHeapCommittedMemory = nonHeapMemoryUsage.getCommitted();
    final long nowNonHeapUsedMemory = nonHeapMemoryUsage.getUsed();
    final long nowNonHeapMaxMemory = nonHeapMemoryUsage.getMax();

    final long nonHeapCommittedMemory =
        nowNonHeapCommittedMemory - currentTestNonHeapCommittedMemory;
    final long nonHeapUsedMemory = nowNonHeapUsedMemory - currentTestNonHeapUsedMemory;
    final long nonHeapMaxMemory = nowNonHeapMaxMemory - currentTestNonHeapMaxMemory;

    if (printResults) {
      System.out.println();
      System.out.println(
          "   Completed the test of '"
              + currentTestName
              + "' in "
              + elapsed
              + " ms. Heap memory used: "
              + nowHeapUsedMemory
              + " bytes. Non heap memory used: "
              + nowNonHeapUsedMemory
              + " .");
      System.out.println("   Cycles done.......................: " + cyclesDone + "/" + cycles);
      System.out.println("   Cycles Elapsed....................: " + cyclesElapsed + " ms");
      System.out.println("   Elapsed...........................: " + elapsed + " ms");
      System.out.println(
          "   Medium cycle elapsed:.............: "
              + (cyclesDone > 0 && elapsed > 0
                  ? new BigDecimal((float) elapsed / cyclesDone).toPlainString()
                  : 0));
      System.out.println(
          "   Cycles per second.................: "
              + new BigDecimal((float) cyclesDone / elapsed * 1000).toPlainString());
      System.out.println(
          "   Committed heap memory diff........: "
              + heapCommittedMemory
              + " ("
              + currentTestHeapCommittedMemory
              + "->"
              + nowHeapCommittedMemory
              + ")");
      System.out.println(
          "   Used heap memory diff.............: "
              + heapUsedMemory
              + " ("
              + currentTestHeapUsedMemory
              + "->"
              + nowHeapUsedMemory
              + ")");
      System.out.println(
          "   Max heap memory diff..............: "
              + heapMaxMemory
              + " ("
              + currentTestHeapMaxMemory
              + "->"
              + nowHeapMaxMemory
              + ")");
      System.out.println(
          "   Committed non heap memory diff....: "
              + nonHeapCommittedMemory
              + " ("
              + currentTestNonHeapCommittedMemory
              + "->"
              + nowNonHeapCommittedMemory
              + ")");
      System.out.println(
          "   Used non heap memory diff.........: "
              + nonHeapUsedMemory
              + " ("
              + currentTestNonHeapUsedMemory
              + "->"
              + nowNonHeapUsedMemory
              + ")");
      System.out.println(
          "   Max non heap memory diff..........: "
              + nonHeapMaxMemory
              + " ("
              + currentTestNonHeapMaxMemory
              + "->"
              + nowNonHeapMaxMemory
              + ")");
      System.out.println(
          "   Objects pending finalization......: " + objectsPendingFinalizationCount);

      System.out.println();
    }

    if (testGroup != null) {
      testGroup.setResult("Execution time", currentTestName, elapsed);
      testGroup.setResult("Free memory", currentTestName, heapCommittedMemory);
    }

    currentTestHeapCommittedMemory = heapCommittedMemory;
    currentTestHeapUsedMemory = heapUsedMemory;
    currentTestHeapMaxMemory = heapMaxMemory;

    currentTestNonHeapCommittedMemory = nonHeapCommittedMemory;
    currentTestNonHeapUsedMemory = nonHeapUsedMemory;
    currentTestNonHeapMaxMemory = nonHeapMaxMemory;
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

  public long getCyclesDone() {
    return cyclesDone;
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

          delta =
              (int)
                  (previousLapTimerElapsed > 0
                      ? lapTimerElapsed * 100 / previousLapTimerElapsed - 100
                      : 0);

          System.out.print(
              String.format(
                  "\n%3d%% lap elapsed: %7dms, total: %7dms, delta: %+3d%%, forecast: %7dms",
                  (cyclesDone + 1) * 100 / cycles,
                  lapTimerElapsed,
                  cyclesElapsed,
                  delta,
                  cyclesElapsed * cycles / cyclesDone));

          previousLapTimerElapsed = lapTimerElapsed;
          lapTimerElapsed = System.nanoTime();
        }
      }

      return takeTimer();

    } catch (Throwable t) {
      System.err.println(
          "Exception caught when executing CYCLE test: " + iTarget.getClass().getSimpleName());
      t.printStackTrace();
    }
    return -1;
  }
}
