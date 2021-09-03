package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.Orient;

public abstract class BareBoneBase2ClientTest extends BareBoneBase1ClientTest {
  protected static Object LOCK = new Object();

  protected abstract void dbClient2(BareBonesServer[] servers);

  public void testReplication() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    final BareBonesServer[] servers = new BareBonesServer[1];
    try {
      // Start the first DB server.
      Thread dbServer1 =
          new Thread() {
            @Override
            public void run() {
              servers[0] = dbServer(DB1_DIR, getDatabaseName(), "asynch-dserver-config-0.xml");
            }
          };
      dbServer1.start();
      dbServer1.join();

      // Start the first DB client.
      Thread dbClient1 =
          new Thread() {
            @Override
            public void run() {
              dbClient1(servers);
            }
          };
      dbClient1.start();

      // Start the first DB client.
      Thread dbClient2 =
          new Thread() {
            @Override
            public void run() {
              dbClient2(servers);
            }
          };
      dbClient2.start();

      dbClient1.join();
      dbClient2.join();

    } finally {
      endTest(servers);
    }
  }

  protected static void sleep(final int i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException xcpt) {
      xcpt.printStackTrace();
    }
  }

  protected static void pause() {
    try {
      System.out.println("Waking up the neighbor");
      LOCK.notifyAll();
      System.out.println("Going to sleep");
      LOCK.wait();
      System.out.println("Awakening");
    } catch (InterruptedException xcpt) {
      xcpt.printStackTrace();
    }
  }
}
