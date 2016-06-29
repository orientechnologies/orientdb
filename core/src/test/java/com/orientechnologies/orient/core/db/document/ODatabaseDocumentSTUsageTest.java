package com.orientechnologies.orient.core.db.document;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ODatabaseDocumentSTUsageTest {

  @Test
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
