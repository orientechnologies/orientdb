package com.orientechnologies.orient.core.db.document;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ODatabaseDocumentTxTest {

  @Test
  public void testMultipleReads() {
    String url = "memory:" + ODatabaseDocumentTxTest.class.getSimpleName();
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).create();
    try {

      db.getMetadata().getSchema().createClass("TestMultipleRead1");
      db.getMetadata().getSchema().createClass("TestMultipleRead2");

      final HashSet<ORecordId> rids = new HashSet<ORecordId>();

      for (int i = 0; i < 100; ++i) {
        final ODocument rec = new ODocument("TestMultipleRead1").field("id", i).save();
        rids.add((ORecordId) rec.getIdentity());

        final ODocument rec2 = new ODocument("TestMultipleRead2").field("id", i).save();
        rids.add((ORecordId) rec2.getIdentity());
      }

      Set<ORecord> result = db.executeReadRecords(rids, false);
      Assert.assertEquals(result.size(), 200);

      for (ORecord rec : result) {
        Assert.assertTrue(rec instanceof ODocument);
      }

      Set<ORecord> result2 = db.executeReadRecords(rids, true);
      Assert.assertEquals(result2.size(), 200);

      for (ORecord rec : result2) {
        Assert.assertTrue(rec instanceof ODocument);
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testCountClass() throws Exception {
    String url = "memory:" + ODatabaseDocumentTxTest.class.getSimpleName() + "-testCountClass";
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).create();
    try {

      OClass testSuperclass = db.getMetadata().getSchema().createClass("TestSuperclass");
      db.getMetadata().getSchema().createClass("TestSubclass", testSuperclass);

      ODocument toDelete = new ODocument("TestSubclass").field("id", 1).save();

      // 1 SUB, 0 SUPER
      Assert.assertEquals(db.countClass("TestSubclass", false), 1);
      Assert.assertEquals(db.countClass("TestSubclass", true), 1);
      Assert.assertEquals(db.countClass("TestSuperclass", false), 0);
      Assert.assertEquals(db.countClass("TestSuperclass", true), 1);

      db.begin();
      try {
        new ODocument("TestSuperclass").field("id", 1).save();
        new ODocument("TestSubclass").field("id", 1).save();
        // 2 SUB, 1 SUPER

        Assert.assertEquals(db.countClass("TestSuperclass", false), 1);
        Assert.assertEquals(db.countClass("TestSuperclass", true), 3);
        Assert.assertEquals(db.countClass("TestSubclass", false), 2);
        Assert.assertEquals(db.countClass("TestSubclass", true), 2);

        toDelete.delete().save();
        // 1 SUB, 1 SUPER

        Assert.assertEquals(db.countClass("TestSuperclass", false), 1);
        Assert.assertEquals(db.countClass("TestSuperclass", true), 2);
        Assert.assertEquals(db.countClass("TestSubclass", false), 1);
        Assert.assertEquals(db.countClass("TestSubclass", true), 1);
      } finally {
        db.commit();
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testTimezone() {
    String url = "memory:" + ODatabaseDocumentTxTest.class.getSimpleName() + "Timezone";
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).create();
    try {

      db.set(ODatabase.ATTRIBUTES.TIMEZONE, "Europe/Rome");
      Object newTimezone = db.get(ODatabase.ATTRIBUTES.TIMEZONE);
      Assert.assertEquals(newTimezone, "Europe/Rome");

      db.set(ODatabase.ATTRIBUTES.TIMEZONE, "foobar");
      newTimezone = db.get(ODatabase.ATTRIBUTES.TIMEZONE);
      Assert.assertEquals(newTimezone, "GMT");
    } finally {
      db.close();
    }
  }

  @Test(expectedExceptions = ODatabaseException.class)
  public void testSaveInvalidRid() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory: rid_" + ODatabaseDocumentTxTest.class.getSimpleName());
    db.create();
    try {
      ODocument doc = new ODocument();

      doc.field("test", new ORecordId(-2, 10));

      db.save(doc);

    } finally {
      db.drop();
    }
  }

}
