package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 1/29/2016
 */
public class RecordInsideMapTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public RecordInsideMapTest(@Optional String url) {
    super(url);
  }

  private ODocument setup() {
    OSchema schema = database.getMetadata().getSchema();
    OClass clsUser = schema.getClass("USER");
    if (clsUser != null) {
      schema.dropClass("USER");
    }
    clsUser = schema.createClass("USER");

    ODocument doc = new ODocument(clsUser);
    doc.save();

    return doc;
  }

  private void testResult(ODocument doc) {
    Object obj = doc.field("tag");

    Assert.assertTrue(obj instanceof OTrackedMap);
    OTrackedMap map = (OTrackedMap)obj;

    Assert.assertEquals(map.size(), 1);
    Object value = map.values().iterator().next();

    Assert.assertTrue(value instanceof OTrackedList);
    OTrackedList lst = (OTrackedList)value;

    Assert.assertEquals(lst.size(), 1);

    Object item = lst.get(0);
    Assert.assertTrue(item instanceof ODocument, "Value is not a document");
  }

  @Test
  public void Sql() {
    ODocument doc = setup();

    String cmd = String.format("INSERT INTO User SET tag={\"extra\": (SELECT FROM %s)}",
      doc.getIdentity());
    ODocument document = database.command(new OCommandSQL(cmd)).execute();
    testResult(document);
  }

  @Test
  public void API() {
    ODocument doc = setup();

    Map<String, Object> item = new HashMap<String, Object>();
    List<ORecord> lst = new ArrayList<ORecord>();

    lst.add(doc);
    item.put("extra", lst);
    doc.field("tag", item);
    doc.save();

    testResult(doc);
  }
}
