package com.orientechnologies.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class OSQLDumperTest {
  @Test
  public void testNoQuery() {
    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();
    Assert.assertTrue(queries.isEmpty());
  }

  @Test
  public void testHalfQueryOne() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setName("d fdad <query> dfda");

    thread.start();

    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();
    latch.countDown();
    thread.join();

    Assert.assertTrue(queries.isEmpty());
  }

  @Test
  public void testHalfQueryTwo() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setName("d fdad </query> dfda");

    thread.start();

    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    latch.countDown();
    thread.join();

    Assert.assertTrue(queries.isEmpty());
  }

  @Test
  public void testSingleQuery() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setName("d fdad <query>select * form s</query>");

    thread.start();

    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    latch.countDown();
    thread.join();

    Assert.assertEquals(1, queries.size());
    Assert.assertEquals("select * form s", queries.iterator().next());
  }

  @Test
  public void testThreeQueries() throws Exception {
    final CountDownLatch[] latches = new CountDownLatch[3];
    for (int i = 0; i < 3; i++) {
      latches[i] = new CountDownLatch(1);
    }

    final Thread[] threads = new Thread[3];
    for (int i = 0; i < 3; i++) {
      final int n = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            latches[n].await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      threads[i].setName(" sdfd < <quer <query>select from s" + i + "</query> </q");
      threads[i].start();
    }

    final Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    for (int i = 0; i < 3; i++) {
      latches[i].countDown();
      threads[i].join();
    }

    final List<String> result = new ArrayList<String>();

    result.add("select from s0");
    result.add("select from s1");
    result.add("select from s2");

    for (String query : queries) {
      Assert.assertTrue(result.remove(query));
    }

    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testHalfCommandOne() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setName("d fdad <command> dfda");

    thread.start();

    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();
    latch.countDown();
    thread.join();

    Assert.assertTrue(queries.isEmpty());
  }

  @Test
  public void testHalfCommandTwo() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setName("d fdad </command> dfda");

    thread.start();

    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    latch.countDown();
    thread.join();

    Assert.assertTrue(queries.isEmpty());
  }

  @Test
  public void testSingleCommand() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setName("d fdad <command>select * form s</command>");

    thread.start();

    Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    latch.countDown();
    thread.join();

    Assert.assertEquals(1, queries.size());
    Assert.assertEquals("select * form s", queries.iterator().next());
  }

  @Test
  public void testThreeCommands() throws Exception {
    final CountDownLatch[] latches = new CountDownLatch[3];
    for (int i = 0; i < 3; i++) {
      latches[i] = new CountDownLatch(1);
    }

    final Thread[] threads = new Thread[3];
    for (int i = 0; i < 3; i++) {
      final int n = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            latches[n].await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      threads[i].setName(" sdfd < <quer <command>select from s" + i + "</command> </q");
      threads[i].start();
    }

    final Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    for (int i = 0; i < 3; i++) {
      latches[i].countDown();
      threads[i].join();
    }

    final List<String> result = new ArrayList<String>();

    result.add("select from s0");
    result.add("select from s1");
    result.add("select from s2");

    for (String query : queries) {
      Assert.assertTrue(result.remove(query));
    }

    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testThreeCommandsThreeQueries() throws Exception {
    final CountDownLatch[] latches = new CountDownLatch[6];
    for (int i = 0; i < 6; i++) {
      latches[i] = new CountDownLatch(1);
    }

    final Thread[] threads = new Thread[6];
    for (int i = 0; i < 6; i++) {
      final int n = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            latches[n].await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      if (i < 3)
        threads[i].setName(" sdfd < <quer <command>select from s" + i + "</command> </q");
      else
        threads[i].setName(" sdfd < <quer <query>select from s" + i + "</query> </q");

      threads[i].start();
    }

    final Collection<String> queries = OSQLDumper.dumpAllSQLQueries();

    for (int i = 0; i < 6; i++) {
      latches[i].countDown();
      threads[i].join();
    }

    final List<String> result = new ArrayList<String>();

    result.add("select from s0");
    result.add("select from s1");
    result.add("select from s2");
    result.add("select from s3");
    result.add("select from s4");
    result.add("select from s5");

    for (String query : queries) {
      Assert.assertTrue(result.remove(query));
    }

    Assert.assertTrue(result.isEmpty());
  }
}
