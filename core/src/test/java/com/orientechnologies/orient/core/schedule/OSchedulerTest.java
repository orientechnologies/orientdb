package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests cases for the Scheduler component.
 * 
 * @author Luca Garulli
 */
public class OSchedulerTest {

  @Test
  public void scheduleEverySecWithDbClosed() throws Exception {

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:scheduler");
    db.create();

    final AtomicInteger counter = new AtomicInteger(0);

    try {
      final OFunction f = db.getMetadata().getFunctionLibrary().createFunction("testFunction");
      f.setCallback(new OCallable<Object, Map<Object, Object>>() {
        @Override
        public Object call(Map<Object, Object> args) {
          return counter.incrementAndGet();
        }
      });

      db.getMetadata().getScheduler()
          .scheduleEvent(new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(f).build());
    } finally {
      db.close();
    }

    Thread.sleep(5000);

    Assert.assertTrue(counter.get() >= 4);
    db.open("admin", "admin").drop();
  }

  @Test
  public void eventLifecycle() throws Exception {

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:scheduler");
    db.create();

    try {
      final AtomicInteger counter = new AtomicInteger(0);

      final OFunction f = db.getMetadata().getFunctionLibrary().createFunction("testFunction");
      f.setCallback(new OCallable<Object, Map<Object, Object>>() {
        @Override
        public Object call(Map<Object, Object> args) {
          return counter.incrementAndGet();
        }
      });

      db.getMetadata().getScheduler()
          .scheduleEvent(new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(f).build());

      Thread.sleep(2000);

      db.getMetadata().getScheduler().removeEvent("test");
      db.getMetadata().getScheduler().removeEvent("test");

      Thread.sleep(3000);

      db.getMetadata().getScheduler().removeEvent("test");

      Assert.assertTrue(counter.get() >= 1);
      Assert.assertTrue(counter.get() <= 3);

    } finally {
      db.drop();
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:scheduler");
    db.create();

    final AtomicInteger counter = new AtomicInteger(0);
    try {

      final OFunction f = db.getMetadata().getFunctionLibrary().createFunction("testFunction");
      f.setCallback(new OCallable<Object, Map<Object, Object>>() {
        @Override
        public Object call(Map<Object, Object> args) {
          return counter.incrementAndGet();
        }
      });

      db.getMetadata().getScheduler()
          .scheduleEvent(new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(f).build());
    } finally {
      db.close();
    }

    Thread.sleep(1000);

    final ODatabaseDocumentTx db2 = new ODatabaseDocumentTx("memory:scheduler");
    db2.open("admin", "admin");
    try {
      Thread.sleep(4000);
    } finally {
      db2.drop();
    }
    Assert.assertTrue(counter.get() >= 4);
  }
}
