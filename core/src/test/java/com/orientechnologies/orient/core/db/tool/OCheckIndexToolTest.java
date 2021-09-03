package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 14/09/17. */
public class OCheckIndexToolTest {
  @Test
  public void test() {
    ODatabaseDocumentInternal db = new ODatabaseDocumentTx("memory:OCheckIndexToolTest");
    db.create();
    try {
      db.command(new OCommandSQL("create class Foo")).execute();
      db.command(new OCommandSQL("create property Foo.name STRING")).execute();
      db.command(new OCommandSQL("create index Foo.name on Foo (name) NOTUNIQUE")).execute();

      ODocument doc = db.newInstance("Foo");
      doc.field("name", "a");
      doc.save();

      ORID rid = doc.getIdentity();

      int N_RECORDS = 100000;
      for (int i = 0; i < N_RECORDS; i++) {
        doc = db.newInstance("Foo");
        doc.field("name", "x" + i);
        doc.save();
      }

      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, "Foo.name");
      Object key = idx.getDefinition().createValue("a");
      boolean a = idx.remove(key, rid);

      List result = db.query(new OSQLSynchQuery<Object>("SELECT FROM Foo"));
      Assert.assertEquals(N_RECORDS + 1, result.size());

      OCheckIndexTool tool = new OCheckIndexTool();
      tool.setDatabase(db);
      tool.setVerbose(true);
      tool.setOutputListener(
          new OCommandOutputListener() {
            @Override
            public void onMessage(String iText) {
              System.out.println(iText);
            }
          });

      tool.run();
      Assert.assertEquals(1, tool.getTotalErrors());
    } finally {
      db.close();
    }
  }

  @Test
  public void testBugOnCollectionIndex() {
    final OrientDB context =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    try (ODatabaseSession db = context.open("test", "admin", "adminpwd")) {
      db.command("create class testclass");
      db.command("create property testclass.name string");
      db.command("create property testclass.tags linklist");
      db.command("alter property testclass.tags default '[]'");
      db.command("create index testclass_tags_idx on testclass (tags) NOTUNIQUE_HASH_INDEX");

      db.command("insert into testclass set name = 'a',tags = [#5:0] ");
      db.command("insert into testclass set name = 'b'");
      db.command("insert into testclass set name = 'c' ");

      final OCheckIndexTool tool = new OCheckIndexTool();

      tool.setDatabase((ODatabaseDocumentInternal) db);
      tool.setVerbose(true);
      tool.setOutputListener(
          new OCommandOutputListener() {
            @Override
            public void onMessage(String iText) {
              System.out.println(iText);
            }
          });
      tool.run();
      Assert.assertEquals(0, tool.getTotalErrors());
    }
    context.close();
  }
}
