package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;

public class AsyncIndexRemoteIT extends BareBoneBase3ServerTest {
  private static final OLogger logger = OLogManager.instance().logger(AsyncIndexRemoteIT.class);

  @Override
  protected String getDatabaseName() {
    return "AsyncIndexIT";
  }

  protected void dbClient1(BareBonesServer[] servers) {
    OrientDB orientDB = new OrientDB("remote:localhost:2424", OrientDBConfig.defaultConfig());
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

      final Iterable<OElement> result =
          graph.command(new OSQLSynchQuery<OElement>("select count(*) from SMS")).execute();

      Assert.assertEquals(1, ((Number) result.iterator().next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      logger.info("Shutting down db1");
      graph.close();
      orientDB.close();
    }

    // CHECK ON THE 2ND NODE
    OrientDB orientDB2 = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig());
    ODatabaseDocument graph2 = orientDB2.open(getDatabaseName(), "admin", "admin");

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

      final Iterable<OElement> result =
          graph2.command(new OSQLSynchQuery<OElement>("select count(*) from SMS")).execute();

      Assert.assertEquals(1, ((Number) result.iterator().next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      logger.info("Shutting down db2");
      graph2.close();
      orientDB2.close();
    }

    // CHECK ON THE 2ND NODE
    OrientDB orientDB3 = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig());
    ODatabaseDocument graph3 = orientDB3.open(getDatabaseName(), "admin", "admin");
    try {
      try {
        graph3
            .command(
                "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is"
                    + " a test')")
            .close();
        Assert.fail("violated unique index was not raised");
      } catch (ORecordDuplicatedException e) {
      }

      final Iterable<OElement> result =
          graph3.command(new OSQLSynchQuery<OElement>("select count(*) from SMS")).execute();

      Assert.assertEquals(1, ((Number) result.iterator().next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      logger.info("Shutting down db3");
      graph3.close();
      orientDB3.close();
    }
  }

  @Override
  protected void dbClient2(BareBonesServer[] servers) {}
}
