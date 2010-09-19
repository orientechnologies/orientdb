package com.orientechnologies.common.test;

public abstract class SpeedTestThread extends Thread implements SpeedTest {
  protected SpeedTestData         data;
  protected SpeedTestMultiThreads owner;

  protected SpeedTestThread() {
    data = new SpeedTestData();
  }

  protected SpeedTestThread(long iCycles) {
    data = new SpeedTestData(iCycles);
  }

  public void setCycles(long iCycles) {
    data.cycles = iCycles;
  }

  public void setOwner(SpeedTestMultiThreads iOwner) {
    owner = iOwner;
  }

  @Override
  public void run() {
    data.printResults = false;
    data.go(this);
  }

  public void init() throws Exception {
  }

  public void deinit() throws Exception {
  }

  public void afterCycle() throws Exception {
  }

  public void beforeCycle() throws Exception {
  }
}
