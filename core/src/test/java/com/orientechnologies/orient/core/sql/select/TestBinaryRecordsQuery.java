package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

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

    List<ORecord> res = db.query(new OSQLSynchQuery<Object>("select from cluster:BlobCluster"));

    assertEquals(1, res.size());
  }

  @Test
  public void testSelectRidBinary() {
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    List<ORecord> res =
        db.query(new OSQLSynchQuery<Object>("select @rid from cluster:BlobCluster"));
    assertEquals(1, res.size());
  }

  @Test
  public void testDeleteBinary() {
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    Integer res =
        db.command(new OCommandSQL("delete from (select from cluster:BlobCluster)")).execute();
    db.getLocalCache().clear();
    assertEquals(1, res.intValue());
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

    Integer res =
        db.command(
                new OCommandSQL(
                    "delete from cluster:BlobCluster where @rid in (select ref from RecordPointer)"))
            .execute();
    db.getLocalCache().clear();
    assertEquals(1, res.intValue());
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

    Integer res =
        db.command(new OCommandSQL("delete from (select expand(ref) from RecordPointer)"))
            .execute();
    db.getLocalCache().clear();
    assertEquals(2, res.intValue());
    rec = db.load(rec.getIdentity());
    assertNull(rec);
    rec = db.load(rec1.getIdentity());
    assertNull(rec);
  }
}
