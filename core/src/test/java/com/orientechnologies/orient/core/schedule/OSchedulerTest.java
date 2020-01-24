package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.executor.OResult;
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
 * @author Enrico Risa
 */
public class OSchedulerTest {

  @Test
  public void scheduleSQLFunction() throws Exception {

    OrientDB context = initContext();
    final ODatabaseSession db = initDatabase(context);
    try {
      createLogEvent(db);

      Thread.sleep(2000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 2 && count <= 3);
    } finally {
      db.close();
      context.close();
    }
  }

  @Test
  public void scheduleWithDbClosed() throws Exception {

    OrientDB context = initContext();
    ODatabaseSession db = initDatabase(context);
    createLogEvent(db);
    db.close();

    Thread.sleep(2000);

    db = initDatabase(context);
    Long count = getLogCounter(db);

    Assert.assertTrue(count >= 2);

    db.close();
    context.close();
  }

  @Test
  public void eventLifecycle() throws Exception {

    OrientDB context = initContext();
    final ODatabaseSession db = initDatabase(context);
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
      db.close();
      context.close();
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {

    OrientDB context = initContext();
    final ODatabaseSession db = initDatabase(context);
    createLogEvent(db);
    db.close();

    Thread.sleep(1000);

    final ODatabaseSession db2 = initDatabase(context);
    try {

      Thread.sleep(2000);
      Long count = getLogCounter(db2);
      Assert.assertTrue(count >= 2);

    } finally {
      db2.close();
      context.close();
    }
  }

  @Test
  public void eventBySQL() throws Exception {

    OrientDB context = initContext();
    final ODatabaseSession db = initDatabase(context);
    try {
      OFunction func = createFunction(db);

      // CREATE NEW EVENT
      db.command("insert into oschedule set name = 'test', function = ?, rule = \"0/1 * * * * ?\"", func.getId()).close();

      Thread.sleep(2500);

      long count = getLogCounter(db);

      Assert.assertTrue(count >= 2);

      // UPDATE
      db.command("update oschedule set rule = \"0/2 * * * * ?\" where name = 'test'", func.getId()).close();

      Thread.sleep(4000);

      long newCount = getLogCounter(db);

      Assert.assertTrue(newCount - count > 1);
//      Assert.assertTrue(newCount - count <= 2);

      // DELETE
      db.command("delete from oschedule where name = 'test'", func.getId()).close();

      Thread.sleep(3000);

      count = newCount;

      newCount = getLogCounter(db);

      Assert.assertTrue(newCount - count <= 1);

    } finally {
      db.close();
      context.close();
    }
  }

  private OrientDB initContext() {

    return new OrientDB("embedded:.", OrientDBConfig.defaultConfig());

  }

  private ODatabaseSession initDatabase(OrientDB context) {

    context.createIfNotExists("scheduler", ODatabaseType.MEMORY);

    return context.open("scheduler", "admin", "admin");
  }

  private void createLogEvent(ODatabaseSession db) {
    OFunction func = createFunction(db);

    Map<Object, Object> args = new HashMap<Object, Object>();
    args.put("note", "test");
    db.getMetadata().getScheduler().scheduleEvent(
        new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(func).setArguments(args).build());
  }

  private OFunction createFunction(ODatabaseSession db) {
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

  private Long getLogCounter(final ODatabaseSession db) {
    db.activateOnCurrentThread();
    OResult result = db.query("select count(*) as count from scheduler_log").stream().findFirst().get();
    return result.getProperty("count");
  }
}

