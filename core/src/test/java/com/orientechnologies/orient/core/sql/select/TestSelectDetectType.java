package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSelectDetectType {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + TestBinaryRecordsQuery.class.getSimpleName());
    db.create();
  }

  @Test
  public void testFloatDetection() {

    List<ODocument> res =
        db.query(
            new OSQLSynchQuery<ODocument>("select ty.type() from ( select 1.021484375 as ty)"));
    System.out.println(res.get(0));
    assertEquals(res.get(0).field("ty"), "DOUBLE");
  }

  @After
  public void after() {
    db.drop();
  }
}
