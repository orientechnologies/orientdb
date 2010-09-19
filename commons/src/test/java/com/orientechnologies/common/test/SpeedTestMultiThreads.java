package com.orientechnologies.common.test;

import org.testng.annotations.Test;

@Test(enabled = false)
public abstract class SpeedTestMultiThreads extends SpeedTestAbstract {
  protected Class<? extends SpeedTestThread> threadClass;
  protected int                              threads;
  protected long                             threadCycles;

  protected SpeedTestMultiThreads(long iCycles, int iThreads, Class<? extends SpeedTestThread> iThreadClass) {
    super(1);
    threadClass = iThreadClass;
    threads = iThreads;
    threadCycles = iCycles;
  }

  @Override
  public void cycle() throws InterruptedException {
    SpeedTestThread[] ts = new SpeedTestThread[threads];
    SpeedTestThread t;
    for (int i = 0; i < threads; ++i)
      try {
        t = threadClass.newInstance();
        ts[i] = t;

        t.setOwner(this);
        t.setCycles(threadCycles / threads);
        t.start();
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }

    for (int i = 0; i < threads; ++i) {
      ts[i].join();
    }
  }
}
