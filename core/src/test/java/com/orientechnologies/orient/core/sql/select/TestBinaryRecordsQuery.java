package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by tglman on 15/04/16.
 */
public class TestBinaryRecordsQuery {

  private ODatabaseDocument document;

  @Before
  public void before() {
    document = new ODatabaseDocumentTx("memory:test");
    document.create();
    document.addBlobCluster("BlobCluster");
  }

  @After
  public void after() {
    document.drop();
  }

  @Test
  public void testSelectBinary() {
    document.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    List<ORecord> res = document.query(new OSQLSynchQuery<Object>("select from cluster:BlobCluster"));

    assertEquals(1, res.size());
  }

  @Test
  public void testSelectRidBinary() {
    ORecord rec = document.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    List<ORecord> res = document.query(new OSQLSynchQuery<Object>("select @rid from cluster:BlobCluster"));
    assertEquals(1, res.size());
  }

  @Test
  public void testDeleteBinary() {
    ORecord rec = document.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");

    Integer res = document.command(new OCommandSQL("delete from (select from cluster:BlobCluster)")).execute();
    document.getLocalCache().clear();
    assertEquals(1, res.intValue());
    rec = document.load(rec.getIdentity());
    assertNull(rec);
  }


}
