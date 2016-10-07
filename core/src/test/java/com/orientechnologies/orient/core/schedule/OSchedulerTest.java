package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests cases for the Scheduler component.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSchedulerTest {

  @Test
  public void scheduleSQLFunction() throws Exception {

    final ODatabaseDocumentTx db = initDatabase();
    try {
      createLogEvent(db);

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
    createLogEvent(db);
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
      createLogEvent(db);

      Thread.sleep(2000);

      db.getMetadata().getScheduler().removeEvent("test");

      assertThat(db.getMetadata().getScheduler().getEvents()).isEmpty();

      assertThat(db.getMetadata().getScheduler().getEvent("test")).isNull();

      //remove again
      db.getMetadata().getScheduler().removeEvent("test");

      Thread.sleep(3000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 1 && count <= 3);

    } finally {
      db.drop();
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {

    final ODatabaseDocumentTx db = initDatabase();
    createLogEvent(db);
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

  @Test
  public void eventBySQL() throws Exception {
    final ODatabaseDocumentTx db = initDatabase();
    try {
      OFunction func = createFunction(db);

      // CREATE NEW EVENT
      db.command(new OCommandSQL("insert into oschedule set name = 'test', function = ?, rule = \"0/1 * * * * ?\""))
          .execute(func.getId());

      Thread.sleep(4500);

      long count = getLogCounter(db);

      Assert.assertTrue(count >= 4);

      // UPDATE
      db.command(new OCommandSQL("update oschedule set rule = \"0/2 * * * * ?\" where name = 'test'")).execute(func.getId());

      Thread.sleep(4000);

      long newCount = getLogCounter(db);

      Assert.assertTrue(newCount - count > 1);
      Assert.assertTrue(newCount - count <= 2);

      // DELETE
      db.command(new OCommandSQL("delete from oschedule where name = 'test'")).execute(func.getId());

      Thread.sleep(3000);

      count = newCount;

      newCount = getLogCounter(db);

      Assert.assertTrue(newCount - count <= 1);

    } finally {
      db.drop();
    }
  }

  private ODatabaseDocumentTx initDatabase() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:scheduler");
    db.create();
    return db;
  }

  private void createLogEvent(ODatabaseDocumentTx db) {
    OFunction func = createFunction(db);

    Map<Object, Object> args = new HashMap<Object, Object>();
    args.put("note", "test");
    db.getMetadata().getScheduler().scheduleEvent(
        new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(func).setArguments(args).build());
  }

  private OFunction createFunction(ODatabaseDocumentTx db) {
    db.getMetadata().getSchema().createClass("scheduler_log");

    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("logEvent");
    func.setLanguage("SQL");
    func.setCode("insert into scheduler_log set timestamp = sysdate(), note = :note");
    final List<String> pars = new ArrayList<String>();
    pars.add("note");
    func.setParameters(pars);
    func.save();
    return func;
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
