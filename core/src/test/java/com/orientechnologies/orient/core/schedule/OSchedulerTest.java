package com.orientechnologies.orient.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests cases for the Scheduler component.
 *
 * @author Enrico Risa
 */
public class OSchedulerTest {

  @Test
  public void scheduleSQLFunction() throws Exception {
    OrientDB context = createContext();

    try {
      ODatabaseSession db = context.cachedPool("test", "admin", "admin").acquire();
      createLogEvent(db);

      Thread.sleep(2000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 2 && count <= 3);
    } finally {
      context.close();
    }
  }

  @Test
  public void scheduleWithDbClosed() throws Exception {
    OrientDB context = createContext();
    ODatabaseSession db = context.open("test", "admin", "admin");
    createLogEvent(db);
    db.close();

    Thread.sleep(2000);

    db = context.open("test", "admin", "admin");
    Long count = getLogCounter(db);

    Assert.assertTrue(count >= 2);

    db.close();
    context.close();
  }

  @Test
  public void eventLifecycle() throws Exception {
    OrientDB context = createContext();
    try {
      ODatabaseSession db = context.cachedPool("test", "admin", "admin").acquire();
      createLogEvent(db);

      Thread.sleep(2000);

      db.getMetadata().getScheduler().removeEvent("test");

      assertThat(db.getMetadata().getScheduler().getEvents()).isEmpty();

      assertThat(db.getMetadata().getScheduler().getEvent("test")).isNull();

      // remove again
      db.getMetadata().getScheduler().removeEvent("test");

      Thread.sleep(3000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 1 && count <= 3);

    } finally {
      context.close();
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {

    OrientDB context = createContext();
    final ODatabaseSession db = context.open("test", "admin", "admin");
    createLogEvent(db);
    db.close();

    Thread.sleep(1000);

    final ODatabaseSession db2 = context.open("test", "admin", "admin");
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
  public void testScheduleEventWithMultipleActiveDatabaseConnections() {
    OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder().addConfig(OGlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!orientDb.exists("test")) {
      orientDb.create("test", ODatabaseType.MEMORY);
    }

    ODatabasePool pool = orientDb.cachedPool("test", "admin", "admin");
    ODatabaseSession db = pool.acquire();

    assertEquals(db, ODatabaseRecordThreadLocal.instance().getIfDefined());
    createLogEvent(db);
    assertEquals(db, ODatabaseRecordThreadLocal.instance().getIfDefined());

    orientDb.close();
  }

  @Test
  public void eventBySQL() throws Exception {

    OrientDB context = createContext();
    final ODatabaseSession db = context.open("test", "admin", "admin");
    try {
      OFunction func = createFunction(db);

      // CREATE NEW EVENT
      db.command(
              "insert into oschedule set name = 'test', function = ?, rule = \"0/1 * * * * ?\"",
              func.getId())
          .close();

      Thread.sleep(2500);

      long count = getLogCounter(db);

      Assert.assertTrue(count >= 2);

      // UPDATE
      db.command("update oschedule set rule = \"0/2 * * * * ?\" where name = 'test'", func.getId())
          .close();

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

  private OrientDB createContext() {
    OrientDB orientDB = new OrientDB("embedded:.", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists("test", ODatabaseType.MEMORY);
    Orient.instance()
        .registerThreadDatabaseFactory(
            new TestScheduleDatabaseFactory(orientDB, "test", "admin", "admin"));
    return orientDB;
  }

  private void createLogEvent(ODatabaseSession db) {
    OFunction func = createFunction(db);

    Map<Object, Object> args = new HashMap<Object, Object>();
    args.put("note", "test");
    db.getMetadata()
        .getScheduler()
        .scheduleEvent(
            new OScheduledEventBuilder()
                .setName("test")
                .setRule("0/1 * * * * ?")
                .setFunction(func)
                .setArguments(args)
                .build());
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
    OResult result =
        db.query("select count(*) as count from scheduler_log").stream().findFirst().get();
    return result.getProperty("count");
  }

  private static class TestScheduleDatabaseFactory implements ODatabaseThreadLocalFactory {

    private final OrientDB context;
    private final String database;
    private final String username;
    private final String password;

    public TestScheduleDatabaseFactory(
        OrientDB context, String database, String username, String password) {
      this.context = context;
      this.database = database;
      this.username = username;
      this.password = password;
    }

    @Override
    public ODatabaseDocumentInternal getThreadDatabase() {
      return (ODatabaseDocumentInternal) context.cachedPool(database, username, password).acquire();
    }
  }
}
