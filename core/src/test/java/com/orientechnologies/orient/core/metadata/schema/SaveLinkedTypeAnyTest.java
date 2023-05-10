package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 10/06/16. */
public class SaveLinkedTypeAnyTest extends BaseMemoryDatabase {

  @Test
  public void testRemoveLinkedType() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestRemoveLinkedType");
    OProperty prop = classA.createProperty("prop", OType.EMBEDDEDLIST, OType.ANY);

    //    db.command(new OCommandSQL("alter property TestRemoveLinkedType.prop linkedtype
    // null")).execute();
    db.command("insert into TestRemoveLinkedType set prop = [4]").close();

    try (OResultSet result = db.query("select from TestRemoveLinkedType")) {
      Assert.assertTrue(result.hasNext());
      Collection coll = result.next().getProperty("prop");
      Assert.assertFalse(result.hasNext());
      Assert.assertEquals(coll.size(), 1);
      Assert.assertEquals(coll.iterator().next(), 4);
    }
  }

  @Test
  public void testAlterRemoveLinkedType() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("TestRemoveLinkedType");
    OProperty prop = classA.createProperty("prop", OType.EMBEDDEDLIST, OType.ANY);

    db.command("alter property TestRemoveLinkedType.prop linkedtype null").close();
    db.command("insert into TestRemoveLinkedType set prop = [4]").close();

    try (OResultSet result = db.query("select from TestRemoveLinkedType")) {
      Assert.assertTrue(result.hasNext());
      Collection coll = result.next().getProperty("prop");
      Assert.assertFalse(result.hasNext());
      Assert.assertEquals(coll.size(), 1);
      Assert.assertEquals(coll.iterator().next(), 4);
    }
  }
}
