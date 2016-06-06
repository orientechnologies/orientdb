package com.orientechnologies.orient.core.db.document;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.*;

@Test
public class ODatabaseDocumentSTUsageTest {
  public void testShareBetweenThreads() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:ODatabaseDocumentSTUsageTest");
    db.create();
    db.close();

    db.open("admin", "admin");

    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    Future<Object> future = singleThread.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        db.open("admin", "admin");
        return null;
      }
    });

    try {
      future.get();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
    }
  }
}
