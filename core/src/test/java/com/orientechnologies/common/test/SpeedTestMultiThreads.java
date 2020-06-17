package com.orientechnologies.common.test;

import java.lang.reflect.Constructor;

public abstract class SpeedTestMultiThreads extends SpeedTestAbstract {
  protected final Class<? extends SpeedTestThread> threadClass;
  protected final int threads;
  protected long threadCycles;

  protected SpeedTestMultiThreads(
      long iCycles, int iThreads, Class<? extends SpeedTestThread> iThreadClass) {
    super(1);
    threadClass = iThreadClass;
    threads = iThreads;
    threadCycles = iCycles;
  }

  public int getThreads() {
    return threads;
  }

  @Override
  public void cycle() throws InterruptedException {
    final SpeedTestThread[] ts = new SpeedTestThread[threads];
    SpeedTestThread t;
    for (int i = 0; i < threads; ++i)
      try {
        final Constructor<? extends SpeedTestThread> c =
            threadClass.getConstructor(SpeedTestMultiThreads.class, Integer.TYPE);
        t = c.newInstance(this, i);
        ts[i] = t;

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
