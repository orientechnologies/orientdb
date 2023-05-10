package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestLinkedDocumentInMap extends BaseMemoryDatabase {

  @Test
  public void testLinkedValue() {
    db.getMetadata().getSchema().createClass("PersonTest");
    db.command("delete from PersonTest").close();
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

    reOpen("admin", "adminpwd");
    try (OResultSet result = db.query("select from " + tyrionDoc.getIdentity())) {
      res = result.next().getProperty("emergency_contact");
      doc = res.get(0);
      Assert.assertTrue(doc.get("contact").getIdentity().isValid());
    }
  }
}
