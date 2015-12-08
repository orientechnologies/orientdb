package com.orientechnologies.orient.server.distributed.asynch;

import org.junit.Assert;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class AsyncIndexRemoteTest extends BareBoneBase3ServerTest {

  @Override
  protected String getDatabaseName() {
    return "AsyncIndexTestRemote";
  }

  protected void dbClient1() {
    OrientBaseGraph graph = new OrientGraphNoTx(getRemoteURL());
    try {
      graph.command(new OCommandSQL("create class SMS extends V")).execute();
      graph.command(new OCommandSQL("create property SMS.type string")).execute();
      graph.command(new OCommandSQL("create property SMS.lang string")).execute();
      graph.command(new OCommandSQL("create property SMS.source integer")).execute();
      graph.command(new OCommandSQL("create property SMS.content string")).execute();
      graph.command(new OCommandSQL("alter property SMS.lang min 2")).execute();
      graph.command(new OCommandSQL("alter property SMS.lang max 2")).execute();
      graph.command(new OCommandSQL("create index sms_keys ON SMS (type, lang) unique")).execute();

      graph
          .command(new OCommandSQL("create vertex sms content { type: 'notify', lang: 'en', source: 1, content: 'This is a test'}"))
          .execute();

      try {
        graph
            .command(
                new OCommandSQL("create vertex sms content { type: 'notify', lang: 'en', source: 1, content: 'This is a test'}"))
            .execute();
        Assert.fail("violated unique index was not raised");
      } catch (ORecordDuplicatedException e) {
      }

      final Iterable<OrientVertex> result = graph.command(new OSQLSynchQuery<OrientVertex>("select count(*) from SMS")).execute();

      Assert.assertEquals(1, ((Number) result.iterator().next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      OLogManager.instance().info(this, "Shutting down db1");
      graph.shutdown();
    }

    // WAIT FOR ASYNC REPLICATION
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
    }

    // CHECK ON THE 2ND NODE
    OrientBaseGraph graph2 = new OrientGraphNoTx(getRemoteURL2());
    try {
      graph2.command(new OCommandSQL("create vertex sms content { type: 'sms', lang: 'it', source: 1, content: 'This is a test'}"))
          .execute();

      try {
        graph2
            .command(new OCommandSQL("create vertex sms content { type: 'sms', lang: 'it', source: 1, content: 'This is a test'}"))
            .execute();
        Assert.fail("violated unique index was not raised");
      } catch (ORecordDuplicatedException e) {
      }

      final Iterable<OrientVertex> result = graph2.command(new OSQLSynchQuery<OrientVertex>("select count(*) from SMS")).execute();

      Assert.assertEquals(2, ((Number) result.iterator().next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      OLogManager.instance().info(this, "Shutting down db2");
      graph2.shutdown();
    }

    // WAIT FOR ASYNC REPLICATION
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
    }

    // CHECK ON THE 2ND NODE
    OrientBaseGraph graph3 = new OrientGraphNoTx(getRemoteURL3());
    try {
      graph3
          .command(new OCommandSQL("create vertex sms content { type: 'voice', lang: 'fr', source: 1, content: 'This is a test'}"))
          .execute();

      try {
        graph3
            .command(
                new OCommandSQL("create vertex sms content { type: 'voice', lang: 'fr', source: 1, content: 'This is a test'}"))
            .execute();
        Assert.fail("violated unique index was not raised");
      } catch (ORecordDuplicatedException e) {
      }

      final Iterable<OrientVertex> result = graph3.command(new OSQLSynchQuery<OrientVertex>("select count(*) from SMS")).execute();

      Assert.assertEquals(3, ((Number) result.iterator().next().getProperty("count")).intValue());

    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      OLogManager.instance().info(this, "Shutting down db3");
      graph3.shutdown();
    }
  }

  @Override
  protected void dbClient2() {

  }
}
