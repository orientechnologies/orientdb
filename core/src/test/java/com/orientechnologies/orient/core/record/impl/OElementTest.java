package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 02/07/16. */
public class OElementTest extends BaseMemoryDatabase {

  @Test
  public void testGetSetProperty() {
    OElement elem = db.newElement();
    elem.setProperty("foo", "foo1");
    elem.setProperty("foo.bar", "foobar");
    elem.setProperty("  ", "spaces");

    Set<String> names = elem.getPropertyNames();
    Assert.assertTrue(names.contains("foo"));
    Assert.assertTrue(names.contains("foo.bar"));
    Assert.assertTrue(names.contains("  "));
  }

  @Test
  public void testLoadAndSave() {
    db.createClassIfNotExist("TestLoadAndSave");
    OElement elem = db.newElement("TestLoadAndSave");
    elem.setProperty("name", "foo");
    db.save(elem);

    OResultSet result = db.query("select from TestLoadAndSave where name = 'foo'");
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("foo", result.next().getProperty("name"));
    result.close();
  }
}
