package com.orientechnologies.orient.test.database.auto;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class PoolTester2 {

  /**
   * @param args
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException {

    ExecutorService executor = Executors.newFixedThreadPool(20);

    OGraphDatabasePool pool = new OGraphDatabasePool("remote:localhost/demo", "admin", "admin");
    pool.setup(2, 30, 10000, 30000);

    for (int i = 0; i < 200; i++) {
      MyRunnable runnable = new MyRunnable(i, pool);
      executor.execute(runnable);
    }

    Thread.sleep(40000);

    for (int i = 0; i < 200; i++) {
      MyRunnable runnable = new MyRunnable(i, pool);
      executor.execute(runnable);
    }

    // This will make the executor accept no new threads
    // and finish all existing threads in the queue
    executor.shutdown();

    Thread.sleep(60000);

  }

  static class MyRunnable implements Runnable {

    private int                id;
    private OGraphDatabasePool pool;

    public MyRunnable(int id, OGraphDatabasePool pool) {
      this.id = id;
      this.pool = pool;
    }

    @Override
    public void run() {
      OGraphDatabase conn = null;
      try {
        conn = pool.acquire();
        System.out.println("Opened " + id + " connection " + conn);

        this.doStuff(conn);

        // do stuff
      } finally {
        conn.close();
        System.out.println("Closed " + id + " connection");
      }

    }

    private void doStuff(OGraphDatabase conn) {
      try {
        OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM V");
        List<ODocument> result = conn.command(query).execute();
        if (result.size() > 0) {
          ODocument doc = result.get(0);
        }
      } catch (OException e) {
        System.err.println("Query failed");
      }
    }
  }
}
