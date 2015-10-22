package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashSet;

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

      Collection<ORecord> result = db.executeReadRecords(rids, false);
      Assert.assertEquals(result.size(), 200);

      for (ORecord rec : result) {
        Assert.assertTrue(rec instanceof ODocument);
      }

      Collection<ORecord> result2 = db.executeReadRecords(rids, true);
      Assert.assertEquals(result2.size(), 200);

      for (ORecord rec : result2) {
        Assert.assertTrue(rec instanceof ODocument);
      }

    } finally {
      db.close();
    }
  }
}
