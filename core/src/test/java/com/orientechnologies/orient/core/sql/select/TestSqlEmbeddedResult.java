package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlEmbeddedResult {

  @Test
  public void testEmbeddedRusultTypeNotLink() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + TestSqlEmbeddedResult.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    ODocument doc1 = new ODocument();
    doc1.field("format", 1);
    Set<ODocument> docs = new HashSet<ODocument>();
    docs.add(doc1);
    doc.field("rel", docs);
    // doc
    db.save(doc);

    List<ODocument> res =
        db.query(
            new OSQLSynchQuery<Object>(
                "select $Pics[0] as el FROM Test LET $Pics = (select expand( rel.include('format')) from $current)"));
    Assert.assertEquals(res.size(), 1);
    ODocument ele = res.get(0);
    Assert.assertNotNull(ele.field("el"));

    byte[] bt = ele.toStream();
    ODocument read = new ODocument(bt);
    Assert.assertNotNull(read.field("el"));
    Assert.assertEquals(read.fieldType("el"), OType.EMBEDDED);

    res =
        db.query(
            new OSQLSynchQuery<Object>(
                "select $Pics as el FROM Test LET $Pics = (select expand( rel.include('format')) from $current)"));

    Assert.assertEquals(res.size(), 1);
    ele = res.get(0);
    Assert.assertNotNull(ele.field("el"));
    bt = ele.toStream();
    read = new ODocument(bt);
    Assert.assertNotNull(read.field("el"));
    Assert.assertEquals(read.fieldType("el"), OType.EMBEDDEDLIST);
    db.drop();
  }
}
