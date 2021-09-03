package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerHookConfiguration;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by tglman on 23/05/17. */
public class RemoteTransactionHookTest {

  private static final String SERVER_DIRECTORY = "./target/hook-transaction";
  private OServer server;
  private OrientDB orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    OServerHookConfiguration hookConfig = new OServerHookConfiguration();
    hookConfig.clazz = CountCallHookServer.class.getName();
    server.getHookManager().addHook(hookConfig);
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        RemoteTransactionHookTest.class.getSimpleName());
    database = orientDB.open(RemoteTransactionHookTest.class.getSimpleName(), "admin", "admin");
    database.createClass("SomeTx");
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

  @Test
  @Ignore
  public void testCalledInTx() {
    CountCallHook calls = new CountCallHook(database);
    database.registerHook(calls);

    database.begin();
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "some");
    database.save(doc);
    database.command("insert into SomeTx set name='aa' ").close();
    OResultSet res = database.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, (Long) res.next().getProperty("count"));
    res.close();
    database.command("delete from SomeTx where name='aa'").close();
    database.commit();

    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    //    assertEquals(1, calls.getBeforeUpdate());
    //    assertEquals(1, calls.getAfterUpdate());
    //    assertEquals(1, calls.getBeforeDelete());
    //    assertEquals(1, calls.getAfterDelete());
  }

  @Test
  public void testCalledInClientTx() {
    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseDocument database = orientDB.open("test", "admin", "admin");
    CountCallHook calls = new CountCallHook(database);
    database.registerHook(calls);
    database.createClassIfNotExist("SomeTx");
    database.begin();
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "some");
    database.save(doc);
    database.command("insert into SomeTx set name='aa' ").close();
    OResultSet res = database.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, (Long) res.next().getProperty("count"));
    res.close();
    database.command("delete from SomeTx where name='aa'").close();
    database.commit();

    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    assertEquals(1, calls.getBeforeUpdate());
    assertEquals(1, calls.getAfterUpdate());
    assertEquals(1, calls.getBeforeDelete());
    assertEquals(1, calls.getAfterDelete());
    database.close();
    orientDB.close();
    this.database.activateOnCurrentThread();
  }

  @Test
  public void testCalledInTxServer() {
    database.begin();
    CountCallHookServer calls = CountCallHookServer.instance;
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "some");
    database.save(doc);
    database.command("insert into SomeTx set name='aa' ").close();
    OResultSet res = database.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, (Long) res.next().getProperty("count"));
    res.close();
    database.command("delete from SomeTx where name='aa'").close();
    database.commit();
    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    assertEquals(1, calls.getBeforeUpdate());
    assertEquals(1, calls.getAfterUpdate());
    assertEquals(1, calls.getBeforeDelete());
    assertEquals(1, calls.getAfterDelete());
  }

  public static class CountCallHookServer extends CountCallHook {
    public CountCallHookServer(ODatabaseDocument database) {
      super(database);
      instance = this;
    }

    public static CountCallHookServer instance;
  }

  public static class CountCallHook extends ODocumentHookAbstract {
    private int beforeCreate = 0;
    private int beforeUpdate = 0;
    private int beforeDelete = 0;
    private int afterUpdate = 0;
    private int afterCreate = 0;
    private int afterDelete = 0;

    public CountCallHook(ODatabaseDocument database) {
      super(database);
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
    }

    @Override
    public RESULT onRecordBeforeCreate(ODocument iDocument) {
      beforeCreate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterCreate(ODocument iDocument) {
      afterCreate++;
    }

    @Override
    public RESULT onRecordBeforeUpdate(ODocument iDocument) {
      beforeUpdate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterUpdate(ODocument iDocument) {
      afterUpdate++;
    }

    @Override
    public RESULT onRecordBeforeDelete(ODocument iDocument) {
      beforeDelete++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterDelete(ODocument iDocument) {
      afterDelete++;
    }

    public int getAfterCreate() {
      return afterCreate;
    }

    public int getAfterDelete() {
      return afterDelete;
    }

    public int getAfterUpdate() {
      return afterUpdate;
    }

    public int getBeforeCreate() {
      return beforeCreate;
    }

    public int getBeforeDelete() {
      return beforeDelete;
    }

    public int getBeforeUpdate() {
      return beforeUpdate;
    }
  }
}
