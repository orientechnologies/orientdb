package com.orientechnologies.common.test;

public abstract class SpeedTestThread extends Thread implements SpeedTest {
  protected final int threadId;
  protected final SpeedTestData data;
  protected final SpeedTestMultiThreads owner;

  protected SpeedTestThread(final SpeedTestMultiThreads iParent, final int threadId) {
    this(iParent, threadId, 1);
  }

  protected SpeedTestThread(final SpeedTestMultiThreads iParent, final int threadId, long iCycles) {
    this.owner = iParent;
    this.threadId = threadId;
    this.data = new SpeedTestData(iCycles);
  }

  public void setCycles(long iCycles) {
    data.cycles = iCycles;
  }

  @Override
  public void run() {
    data.printResults = false;
    data.go(this);
  }

  public void init() throws Exception {}

  public void deinit() throws Exception {}

  public void afterCycle() throws Exception {}

  public void beforeCycle() throws Exception {}

  public int getThreadId() {
    return threadId;
  }
}
