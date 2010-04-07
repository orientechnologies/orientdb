package com.orientechnologies.common.test;

import org.testng.annotations.Test;

@Test(enabled = false)
public abstract class SpeedTestMultiThreads extends SpeedTestAbstract {
	protected Class<? extends SpeedTestThread>	threadClass;
	protected int																threads;
	protected volatile int											activeThreads	= 0;
	protected long															threadCycles;

	protected SpeedTestMultiThreads(long iCycles, int iThreads, Class<? extends SpeedTestThread> iThreadClass) {
		super(1);
		threadClass = iThreadClass;
		threads = iThreads;
		threadCycles = iCycles;
	}

	public void cycle() {
		SpeedTestThread t;
		for (int i = 0; i < threads; ++i)
			try {
				t = threadClass.newInstance();
				t.setOwner(this);
				t.setCycles(threadCycles / threads);
				t.start();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}

		while (activeThreads > 0) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		}
	}

	public synchronized void startThread(SpeedTestThread speedTestThread) {
		activeThreads++;
	}

	public synchronized void endThread(SpeedTestThread speedTestThread) {
		activeThreads--;
	}
}
