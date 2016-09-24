package com.orientechnologies.orient.server.distributed;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Tests stop and start of replication.
 * 
 * @author Luca Garulli
 */
public class HAStopStartReplicationTest extends AbstractServerClusterTest {
  private final static int SERVERS = 3;

  @Override
  public String getDatabaseName() {
    return "HAStopStartReplicationTest";
  }

  @Override
  protected void executeTest() throws Exception {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      for (int i = 0; i < 100; ++i) {
        db.command(new OCommandSQL("INSERT into Item (name) values ('foo')")).execute();
      }
    } finally {
      db.close();
    }

    db = new ODatabaseDocumentTx("plocal:target/server1/databases/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      Iterable<ODocument> result = db.command(new OCommandSQL("select count(*) from Item")).execute();
      Assert.assertEquals(100l, result.iterator().next().field("count"));

      db.command(new OCommandSQL("HA STOP REPLICATION usa-0")).execute();

      for (int i = 0; i < 100; ++i) {
        db.command(new OCommandSQL("INSERT into Item (name) values ('foo')")).execute();
      }

      result = db.command(new OCommandSQL("select count(*) from Item")).execute();
      Assert.assertEquals(200l, result.iterator().next().field("count"));

    } finally {
      db.close();
    }

    db = new ODatabaseDocumentTx("plocal:target/server2/databases/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      Iterable<ODocument> result = db.command(new OCommandSQL("select count(*) from Item")).execute();
      Assert.assertEquals(100l, result.iterator().next().field("count"));

    } finally {
      db.close();
    }

    db = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      db.command(new OCommandSQL("HA START REPLICATION usa-0")).execute();

      final ODatabaseDocumentTx staticDb = db;

      waitFor(2000, new OCallable<Boolean, Void>() {
        @Override
        public Boolean call(Void iArgument) {
          final Iterable<ODocument> res = staticDb.command(new OCommandSQL("select count(*) from Item")).execute();
          return res.iterator().next().field("count").equals(200l);
        }
      }, "Realignment error");

      Iterable<ODocument> result = db.command(new OCommandSQL("select count(*) from Item")).execute();
      Assert.assertEquals(200l, result.iterator().next().field("count"));

    } finally {
      db.close();
    }

  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE CLASS Item extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Item.name STRING")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Item.map EMBEDDEDMAP")).execute();
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
