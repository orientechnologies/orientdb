package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;

public class AsyncIndexIT extends BareBoneBase2ServerTest {
  private static final OLogger logger = OLogManager.instance().logger(AsyncIndexIT.class);

  @Override
  protected String getDatabaseName() {
    return "AsyncIndexIT";
  }

  protected void dbClient1(BareBonesServer[] servers) {
    OrientDB orientDB = servers[0].getServer().getContext();
    if (!orientDB.exists(getDatabaseName())) {
      orientDB.execute(
          "create database ? plocal users(admin identified by 'admin' role admin)",
          getDatabaseName());
    }
    ODatabaseDocument graph = orientDB.open(getDatabaseName(), "admin", "admin");
    try {
      graph.command("create class SMS").close();
      graph.command("create property SMS.type string").close();
      graph.command("create property SMS.lang string").close();
      graph.command("create property SMS.source integer").close();
      graph.command("create property SMS.content string").close();
      graph.command("alter property SMS.lang min 2").close();
      graph.command("alter property SMS.lang max 2").close();
      graph.command("create index sms_keys ON SMS (type, lang) unique").close();

      graph
          .command(
              "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is a"
                  + " test')")
          .close();
      try {
        graph
            .command(
                "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is"
                    + " a test')")
            .close();
        Assert.fail("violated unique index was not raised");
      } catch (ORecordDuplicatedException e) {
      }

      final OResultSet result = graph.query("select count(*) as count from SMS");

      Assert.assertEquals(1, ((Number) result.next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      logger.info("Shutting down db1");
      graph.close();
    }

    // CHECK ON THE OTHER NODE
    OrientDB orientDB2 = servers[1].getServer().getContext();
    orientDB2.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
    ODatabaseDocument graph2 = orientDB.open(getDatabaseName(), "admin", "admin");
    try {
      try {
        graph2
            .command(
                "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is"
                    + " a test')")
            .close();
        Assert.fail("violated unique index was not raised");
      } catch (ORecordDuplicatedException e) {
      }

      final OResultSet result = graph2.query("select count(*) as count from SMS");

      Assert.assertEquals(1, ((Number) result.next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      logger.info("Shutting down db2");
      graph2.close();
    }
  }

  @Override
  protected void dbClient2(BareBonesServer[] servers) {}
}
