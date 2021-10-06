package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import junit.framework.Assert;

public class AsyncIndexIT extends BareBoneBase2ServerTest {

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
      graph.command(new OCommandSQL("create class SMS")).execute();
      graph.command(new OCommandSQL("create property SMS.type string")).execute();
      graph.command(new OCommandSQL("create property SMS.lang string")).execute();
      graph.command(new OCommandSQL("create property SMS.source integer")).execute();
      graph.command(new OCommandSQL("create property SMS.content string")).execute();
      graph.command(new OCommandSQL("alter property SMS.lang min 2")).execute();
      graph.command(new OCommandSQL("alter property SMS.lang max 2")).execute();
      graph.command(new OCommandSQL("create index sms_keys ON SMS (type, lang) unique")).execute();

      graph
          .command(
              new OCommandSQL(
                  "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is a test')"))
          .execute();
      try {
        graph
            .command(
                new OCommandSQL(
                    "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is a test')"))
            .execute();
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
      OLogManager.instance().info(this, "Shutting down db1");
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
                new OCommandSQL(
                    "insert into sms (type, lang, source, content) values ( 'notify', 'en', 1, 'This is a test')"))
            .execute();
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
      OLogManager.instance().info(this, "Shutting down db2");
      graph2.close();
    }
  }

  @Override
  protected void dbClient2(BareBonesServer[] servers) {}
}
