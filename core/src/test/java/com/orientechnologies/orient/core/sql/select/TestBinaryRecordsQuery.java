package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by tglman on 15/04/16. */
public class TestBinaryRecordsQuery extends BaseMemoryDatabase {

  @Before
  public void beforeTest() {
    super.beforeTest();
    db.addBlobCluster("BlobCluster");
  }

  @Test
  public void testSelectBinary() {
    db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    OResultSet res = db.query("select from cluster:BlobCluster");

    assertEquals(1, res.stream().count());
  }

  @Test
  public void testSelectRidBinary() {
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    OResultSet res = db.query("select @rid from cluster:BlobCluster");
    assertEquals(1, res.stream().count());
  }

  @Test
  public void testDeleteBinary() {
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    OResultSet res = db.command("delete from (select from cluster:BlobCluster)");
    db.getLocalCache().clear();
    assertEquals(1, (long) res.next().getProperty("count"));
    rec = db.load(rec.getIdentity());
    assertNull(rec);
  }

  @Test
  public void testSelectDeleteBinary() {
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    db.getMetadata().getSchema().createClass("RecordPointer");
    ODocument doc = new ODocument("RecordPointer");
    doc.field("ref", rec);
    db.save(doc);

    OResultSet res =
        db.command(
            "delete from cluster:BlobCluster where @rid in (select ref from RecordPointer)");
    db.getLocalCache().clear();
    assertEquals(1, (long) res.next().getProperty("count"));
    rec = db.load(rec.getIdentity());
    assertNull(rec);
  }

  @Test
  public void testDeleteFromSelectBinary() {
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    ORecord rec1 = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    db.getMetadata().getSchema().createClass("RecordPointer");
    ODocument doc = new ODocument("RecordPointer");
    doc.field("ref", rec);
    db.save(doc);

    ODocument doc1 = new ODocument("RecordPointer");
    doc1.field("ref", rec1);
    db.save(doc1);

    OResultSet res = db.command("delete from (select expand(ref) from RecordPointer)");
    db.getLocalCache().clear();
    assertEquals(2, (long) res.next().getProperty("count"));
    rec = db.load(rec.getIdentity());
    assertNull(rec);
    rec = db.load(rec1.getIdentity());
    assertNull(rec);
  }
}
