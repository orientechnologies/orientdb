package com.orientechnologies.orient.core;

import java.util.Date;
import java.util.TimerTask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrientScheduleTaskTest {

  @Before
  public void beforeClass() {
    Orient.instance().startup();
  }

  @After
  public void afterClass() {
    Orient.instance().shutdown();
  }

  @Test
  public void testScheduleTask() {
    TestTask task = new TestTask();
    TimerTask timer = Orient.instance().scheduleTask(task, 0, 20);
    task.waitFor(2, 30);
    timer.cancel();
  }

  @Test
  public void testScheduleTaskInPast() {
    TestTask task = new TestTask();
    TimerTask timer =
        Orient.instance().scheduleTask(task, new Date(System.currentTimeMillis() - 100), 20);
    task.waitFor(2, 30);
    timer.cancel();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testScheduleWithNegativeDelay() {
    TestTask task = new TestTask();
    Orient.instance().scheduleTask(task, -1, 20);
  }

  @Test
  public void testScheduleOneShotTask() {
    TestTask task = new TestTask();
    TimerTask timer = Orient.instance().scheduleTask(task, 0, 0);
    task.waitFor(1, 30);
    try {
      Thread.sleep(50);
    } catch (InterruptedException ignored) {
    }
    Assert.assertEquals(1, task.getRuns());
    timer.cancel();
  }

  private static class TestTask implements Runnable {

    private int runs = 0;

    @Override
    public synchronized void run() {
      runs = runs + 1;
      notifyAll();
    }

    public synchronized int getRuns() {
      return runs;
    }

    public synchronized void waitFor(int runs, long timeout) {
      long end = System.currentTimeMillis() + timeout;
      while (this.runs < runs) {
        long wait = end - System.currentTimeMillis();
        if (wait <= 0) {
          Assert.fail("Timeout waiting for " + runs + " runs, current runs: " + this.runs);
        }
        try {
          wait(wait);
        } catch (InterruptedException ignored) {
        }
      }
    }
  }
}
