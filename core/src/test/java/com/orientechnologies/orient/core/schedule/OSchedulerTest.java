package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests cases for the Scheduler component.
 * 
 * @author Luca Garulli
 */
public class OSchedulerTest {

  @Test
  public void scheduleSQLFunction() throws Exception {

    final ODatabaseDocumentTx db = initDatabase();
    try {

      Thread.sleep(5000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 4 && count <= 5);
    } finally {
      db.drop();
    }
  }

  @Test
  public void scheduleWithDbClosed() throws Exception {

    ODatabaseDocumentTx db = initDatabase();
    db.close();

    Thread.sleep(5000);

    db = openDatabase();
    Long count = getLogCounter(db);

    Assert.assertTrue(count >= 4);
    openDatabase().drop();
  }

  @Test
  public void eventLifecycle() throws Exception {

    final ODatabaseDocumentTx db = initDatabase();
    try {
      Thread.sleep(2000);

      db.getMetadata().getScheduler().removeEvent("test");
      db.getMetadata().getScheduler().removeEvent("test");

      Thread.sleep(3000);

      db.getMetadata().getScheduler().removeEvent("test");

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 1 && count <= 3);

    } finally {
      db.drop();
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {

    final ODatabaseDocumentTx db = initDatabase();
    db.close();

    Thread.sleep(1000);

    final ODatabaseDocumentTx db2 = new ODatabaseDocumentTx("memory:scheduler");
    db2.open("admin", "admin");
    try {

      Thread.sleep(4000);
      Long count = getLogCounter(db2);
      Assert.assertTrue(count >= 4);

    } finally {
      db2.drop();
    }
  }

  private ODatabaseDocumentTx initDatabase() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:scheduler");
    db.create();

    db.getMetadata().getSchema().createClass("scheduler_log");

    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("testFunction");
    func.setLanguage("SQL");
    func.setCode("insert into scheduler_log set timestamp = sysdate()");
    func.save();

    db.getMetadata().getScheduler()
        .scheduleEvent(new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(func).build());

    return db;
  }

  private ODatabaseDocumentTx openDatabase() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:scheduler");
    db.open("admin", "admin");
    return db;
  }

  private Long getLogCounter(final ODatabaseDocumentTx db) {
    db.activateOnCurrentThread();
    List<ODocument> result = (List<ODocument>) db.command(new OCommandSQL("select count(*) from scheduler_log")).execute();
    return result.get(0).field("count");
  }
}
