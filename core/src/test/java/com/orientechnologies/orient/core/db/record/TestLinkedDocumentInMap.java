package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLinkedDocumentInMap {

  private ODatabaseDocumentTx db;

  @Before
  public void create() {
    db = new ODatabaseDocumentTx("memory:" + TestLinkedDocumentInMap.class.getSimpleName());
    db.create();
  }

  @Test
  public void testLinkedValue() {
    db.getMetadata().getSchema().createClass("PersonTest");
    db.command(new OCommandSQL("delete from PersonTest")).execute();
    ODocument jaimeDoc = new ODocument("PersonTest");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();

    ODocument tyrionDoc = new ODocument("PersonTest");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":[{\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}]}");
    tyrionDoc.save();
    List<Map<String, OIdentifiable>> res = tyrionDoc.field("emergency_contact");
    Map<String, OIdentifiable> doc = res.get(0);
    Assert.assertTrue(doc.get("contact").getIdentity().isValid());

    db.close();
    db.open("admin", "admin");
    List<ODocument> result =
        db.query(new OSQLSynchQuery<ODocument>("select from " + tyrionDoc.getIdentity()));
    res = result.get(0).field("emergency_contact");
    doc = res.get(0);
    Assert.assertTrue(doc.get("contact").getIdentity().isValid());
  }

  @After
  public void after() {
    db.drop();
  }
}
